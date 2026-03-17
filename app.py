from flask import Flask, render_template, request, send_file
import pandas as pd
from sqlalchemy import create_engine, text
from io import BytesIO
import os
from datetime import datetime, timedelta

app = Flask(__name__)

# ==================================================
# CONFIG DB (SUPABASE)
# ==================================================
# Gunakan Port 5432 untuk koneksi langsung yang lebih stabil jika pooler bermasalah
DB_URL = "postgresql+psycopg2://postgres.xjnyjskeauyfpqioanse:matisaja123@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres?sslmode=require"
engine = create_engine(
    DB_URL, 
    pool_pre_ping=True, 
    pool_recycle=300
)

# ==================================================
# LOAD RAWDATA MAPPING (LOKAL)
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))
    # Bersihkan kolom deviceid dan unitno
    df_raw["deviceid"] = df_raw["deviceid"].astype(str).str.strip().str.upper()
    df_raw["unitno"] = df_raw["unitno"].astype(str).str.strip().str.upper()
except Exception as e:
    print(f"Error: Rawdata.xlsx bermasalah: {e}")
    df_raw = pd.DataFrame()

last_rows = []

# ==================================================
# LOGIC UTAMA
# ==================================================
@app.route("/", methods=["GET", "POST"])
def index():
    global last_rows
    hasil = None

    if request.method == "POST":
        raw_text = request.form.get("raw", "")
        site = request.form.get("site", "brcb")

        if not raw_text.strip():
            return render_template("index.html", hasil=None)

        table_name = "tbl_brcb" if site == "brcb" else "tbl_brcg"

        # 2. AMBIL DATA DARI DATABASE
        try:
            with engine.connect() as conn:
                # Kita ambil data 2 hari terakhir saja agar proses matching cepat (tidak berat)
                query = text(f'SELECT * FROM {table_name} WHERE "WAKTU KEJADIAN" > NOW() - INTERVAL \'48 HOURS\'')
                df_event = pd.read_sql(query, conn)
        except Exception as e:
            return render_template("index.html", error=f"❌ DB Error: {e}")

        if df_event.empty:
            return render_template("index.html", error=f"❌ Database {table_name} tidak ada data 24 jam terakhir.")

        # 3. PRE-PROCESSING DATA EVENT
        # Pastikan kolom waktu bersih dari info timezone untuk matching yang mudah
        df_event["WAKTU KEJADIAN"] = pd.to_datetime(df_event["WAKTU KEJADIAN"]).dt.tz_localize(None)
        
        # Ekstrak angka saja dari unit di DB (Misal: DT4724 -> 4724)
        df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")
        
        # Format kolom bantu untuk matching
        df_event["MATCH_JAM"] = df_event["WAKTU KEJADIAN"].dt.strftime("%H%M%S")
        df_event["MATCH_TANGGAL"] = df_event["WAKTU KEJADIAN"].dt.strftime("%Y%m%d")

        rows = []

        # 4. LOOPING RAW TEXT
        for raw in raw_text.splitlines():
            raw = raw.strip().upper()
            if not raw: continue

            try:
                # SLS30I049-CLOSEDEYES_20260317_151611
                parts = raw.split("_")
                if len(parts) < 3:
                    rows.append([raw, "❌ Format Salah", "-", "-", "-", "-", "ERROR"])
                    continue
                
                bagian1, tanggal_raw, jam_raw = parts[0], parts[1], parts[2]
                unit_raw, pelanggaran = bagian1.split("-")

                # --- LOGIKA MATCHING WAKTU ---
                # Karena Downloader sudah +7 jam (menjadi WITA), 
                # sedangkan RAW biasanya masih UTC (selisih 8 jam), 
                # Maka RAW jam 15:16:11 sebenernya adalah Event Jam 14:16:11 di DB yang sudah dikoreksi.
                # Jadi kita tetep KURANGI 1 jam dari RAW untuk mencari data di DB WITA.
                
                jam_dt = datetime.strptime(jam_raw, "%H%M%S") - timedelta(hours=1)
                jam_lookup = jam_dt.strftime("%H%M%S")

                # CEK UNIT KE MAPPING
                cek_unit = df_raw[df_raw["deviceid"] == unit_raw]
                if cek_unit.empty:
                    rows.append([raw, "❌ Device ID Unmapped", pelanggaran, "-", "-", "-", "ERROR"])
                    continue

                nama_unit_full = cek_unit.iloc[0]["unitno"]
                angka_unit_lookup = ''.join(filter(str.isdigit, str(nama_unit_full)))

                # CARI EVENT DI DATA DATABASE
                # Kita cari yang Angka Unit sama, Jam sama (setelah -1), dan Tanggal sama
                match = df_event[
                    (df_event["ANGKA_UNIT"] == angka_unit_lookup) &
                    (df_event["MATCH_JAM"] == jam_lookup) &
                    (df_event["MATCH_TANGGAL"] == tanggal_raw)
                ]

                if match.empty:
                    rows.append([raw, nama_unit_full, pelanggaran, "❌ Tidak ditemukan", "-", "-", "FALSE"])
                else:
                    res = match.iloc[0]
                    status_context = str(res.get("INTERVENSI - STATUS CONTEXT", "-")).upper()
                    
                    rows.append([
                        raw,
                        nama_unit_full,
                        pelanggaran,
                        res["WAKTU KEJADIAN"].strftime('%Y-%m-%d %H:%M:%S'),
                        res["WAKTU KE SERVER GABUNGAN"].strftime('%H:%M:%S') if pd.notnull(res["WAKTU KE SERVER GABUNGAN"]) else "-",
                        "-", 
                        status_context if status_context != "NAN" else "NOT FOUND"
                    ])

            except Exception as e:
                rows.append([raw, "❌ Format salah", str(e), "-", "-", "-", "ERROR"])

        hasil = rows
        last_rows = rows

    return render_template("index.html", hasil=hasil)

# ... (Export Excel tetep sama)

# ==================================================
# EXPORT EXCEL
# ==================================================
@app.route("/export")
def export_excel():
    global last_rows
    if not last_rows:
        return "Tidak ada data untuk diexport"

    df_export = pd.DataFrame(last_rows, columns=[
        "RAW", "Nama Unit", "Pelanggaran", "Waktu Kejadian", 
        "Masuk Gabungan", "Waktu Intervensi", "Status Context"
    ])

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False)
    output.seek(0)

    return send_file(
        output,
        download_name=f"Hasil_Cek_Raw_{datetime.now().strftime('%Y%m%d')}.xlsx",
        as_attachment=True
    )

if __name__ == "__main__":
    app.run(debug=True)
