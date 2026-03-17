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
DB_URL = "postgresql+psycopg2://postgres.xjnyjskeauyfpqioanse:mat1saja123@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres?sslmode=require"
engine = create_engine(
    DB_URL, 
    pool_pre_ping=True, 
    pool_recycle=300
)

# ==================================================
# LOAD RAWDATA MAPPING (LOKAL)
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Load mapping deviceid ke unitno sekali saja saat startup
try:
    df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))
    # Bersihkan kolom agar matching lebih akurat
    df_raw["deviceid"] = df_raw["deviceid"].astype(str).str.strip().str.upper()
except Exception as e:
    print(f"Error: Rawdata.xlsx tidak ditemukan atau rusak. {e}")
    df_raw = pd.DataFrame()

# Global variable untuk simpan hasil terakhir (buat export)
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
            return render_template("index.html", hasil="❌ Tidak ada input RAW")

        # 1. Pilih Tabel Database berdasarkan input site
        table_name = "tbl_brcb" if site == "brcb" else "tbl_brcg"

        # 2. AMBIL DATA DARI DATABASE (Nggantiin Upload Manual)
        try:
            with engine.connect() as conn:
                df_event = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)
        except Exception as e:
            return render_template("index.html", hasil=f"❌ DB Error: {e}")

        if df_event.empty:
            return render_template("index.html", hasil=f"❌ Database {table_name} kosong.")

        # 3. PRE-PROCESSING DATA EVENT (Sesuai Logic Lama)
        df_event["WAKTU KEJADIAN"] = pd.to_datetime(df_event["WAKTU KEJADIAN"])
        df_event["WAKTU KE SERVER GABUNGAN"] = pd.to_datetime(df_event["WAKTU KE SERVER GABUNGAN"])
        
        # Ekstrak angka saja dari KODE KENDARAAN (Misal: DT123 -> 123)
        df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")
        df_event["JAM"] = df_event["WAKTU KEJADIAN"].dt.strftime("%H%M%S")
        df_event["TANGGAL"] = df_event["WAKTU KEJADIAN"].dt.strftime("%Y%m%d")

        rows = []

        # 4. LOOPING RAW TEXT (Logic Lama Kamu)
        for raw in raw_text.splitlines():
            raw = raw.strip().upper()
            if not raw: continue

            try:
                # Format: SLS30I614-YAWNING_20260309_235021
                bagian1, tanggal, jam = raw.split("_")
                unit_raw, pelanggaran = bagian1.split("-")

                # Logic Kurangi 1 Jam (Sesuai kode lama kamu)
                jam_dt = pd.to_datetime(jam, format="%H%M%S") - pd.Timedelta(hours=1)
                jam_final = jam_dt.strftime("%H%M%S")

                # CEK UNIT KE MAPPING RAWDATA.XLSX
                cek_unit = df_raw[df_raw["deviceid"] == unit_raw]

                if cek_unit.empty:
                    # [RAW, Unit, Alert, Kejadian, Gabungan, Status/Intervensi, Label]
                    rows.append([raw, "❌ Unit Tidak Terdaftar", pelanggaran, "-", "-", "-", "ERROR"])
                    continue

                nama_unit = cek_unit.iloc[0]["unitno"]
                angka_unit = ''.join(filter(str.isdigit, str(nama_unit)))

                # CARI EVENT DI DATA DATABASE
                cari = df_event[
                    (df_event["ANGKA_UNIT"] == angka_unit) &
                    (df_event["JAM"] == jam_final) &
                    (df_event["TANGGAL"] == tanggal)
                ]

                if cari.empty:
                    rows.append([raw, nama_unit, pelanggaran, "❌ Tidak ditemukan", "-", "-", "FALSE"])
                else:
                    row_db = cari.iloc[0]
                    # Format intervensi (ambil status context)
                    status_context = row_db.get("INTERVENSI - STATUS CONTEXT", "-")
                    
                    rows.append([
                        raw,
                        nama_unit,
                        pelanggaran,
                        row_db["WAKTU KEJADIAN"].strftime('%Y-%m-%d %H:%M:%S'),
                        row_db["WAKTU KE SERVER GABUNGAN"].strftime('%H:%M:%S'),
                        "-", # Waktu intervensi jika ada kolomnya
                        str(status_context).upper() # Menghasilkan TRUE/FALSE untuk Badge
                    ])

            except Exception:
                rows.append([raw, "❌ Format salah", "-", "-", "-", "-", "ERROR"])

        hasil = rows
        last_rows = rows  # Simpan untuk export

    return render_template("index.html", hasil=hasil)

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
