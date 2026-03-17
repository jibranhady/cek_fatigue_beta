from flask import Flask, render_template, request, send_file
import pandas as pd
from sqlalchemy import create_engine, text
from io import BytesIO
import os
from datetime import datetime, timedelta

app = Flask(__name__)

# ==================================================
# 1. DB CONNECTION (SUPABASE - REVISED)
# ==================================================
# Menggunakan pool_pre_ping agar tidak "server closed connection"
DB_URL = "postgresql+psycopg2://postgres:mat1saja123@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres?sslmode=require"
engine = create_engine(
    DB_URL, 
    pool_pre_ping=True, 
    pool_recycle=300
)

# ==================================================
# 2. LOAD RAWDATA (LOKAL)
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Pastikan Rawdata.xlsx ada di folder yang sama dengan app.py
try:
    df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))
    df_raw["deviceid_clean"] = df_raw["deviceid"].astype(str).str.strip().str.upper()
except Exception as e:
    print(f"Error loading Rawdata.xlsx: {e}")
    df_raw = pd.DataFrame()

# Penyimpanan sementara hasil query untuk fitur export
storage = {"last_rows": []}

# ==================================================
# ✅ CORE LOGIC: BULK MATCHING
# ==================================================
@app.route("/", methods=["GET", "POST"])
def index():
    hasil = None

    if request.method == "POST":
        raw_text = request.form.get("raw", "")
        site = request.form.get("site", "brcb")

        if not raw_text.strip():
            return render_template("index.html", hasil=None, error="❌ Masukkan teks RAW!")

        # Pilih Tabel berdasarkan radio button/select
        table = "tbl_brcb" if site == "brcb" else "tbl_brcg"

        # Ambil Data dari Database (Hanya ambil kolom yang perlu agar hemat RAM)
        try:
            with engine.connect() as conn:
                query = f'SELECT * FROM {table}'
                df_event = pd.read_sql(query, conn)
        except Exception as e:
            return render_template("index.html", error=f"❌ Database Error: {e}")

        if df_event.empty:
            return render_template("index.html", error="❌ Database Kosong (Gunakan Downloader dulu)")

        # --- NORMALISASI DATA DATABASE ---
        waktu_cols = ["WAKTU KEJADIAN", "WAKTU KE SERVER GABUNGAN", "WAKTU INTERVENSI"]
        for col in waktu_cols:
            if col in df_event.columns:
                df_event[col] = pd.to_datetime(df_event[col], errors="coerce")

        # Ekstrak angka dari KODE KENDARAAN (misal: "DT123" -> "123")
        df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)").fillna("")
        df_event["JAM_STR"] = df_event["WAKTU KEJADIAN"].dt.strftime("%H%M%S")
        df_event["TANGGAL_STR"] = df_event["WAKTU KEJADIAN"].dt.strftime("%Y%m%d")

        rows = []
        # --- LOOP PROSES RAW TEXT ---
        for raw_line in raw_text.splitlines():
            raw_line = raw_line.strip().upper()
            if not raw_line: continue

            try:
                # Format: UNIT-PELANGGARAN_TANGGAL_JAM (Contoh: DSM01-FATIGUE_20240318_100000)
                bagian_depan, tanggal_raw, jam_raw = raw_line.split("_")
                unit_raw, pelanggaran = bagian_depan.split("-")

                # FIX -1 JAM (Logika bisnis: waktu di RAW biasanya 1 jam lebih cepat)
                jam_dt = datetime.strptime(jam_raw, "%H%M%S") - timedelta(hours=1)
                jam_final = jam_dt.strftime("%H%M%S")

                # Cari di file Rawdata.xlsx (Mapping DeviceID ke UnitNo)
                cek_unit = df_raw[df_raw["deviceid_clean"] == unit_raw]

                if cek_unit.empty:
                    rows.append({
                        "raw": raw_line, "unit": "❌ Tak Ditemukan", "pelanggaran": pelanggaran,
                        "status": "DeviceID Salah", "waktu": "", "server": "", "intervensi": ""
                    })
                    continue

                nama_unit = str(cek_unit.iloc[0]["unitno"])
                # Ambil angka saja dari nama unit (misal: "PAMA 123" -> "123")
                angka_unit = ''.join(filter(str.isdigit, nama_unit))

                # --- MATCHING KE DATA DATABASE ---
                mask = (
                    (df_event["ANGKA_UNIT"] == angka_unit) &
                    (df_event["JAM_STR"] == jam_final) &
                    (df_event["TANGGAL_STR"] == tanggal_raw)
                )
                cari = df_event[mask]

                if cari.empty:
                    rows.append({
                        "raw": raw_line, "unit": nama_unit, "pelanggaran": pelanggaran,
                        "status": "❌ Tak Ada di DB", "waktu": "-", "server": "-", "intervensi": "-"
                    })
                else:
                    match = cari.iloc[0]
                    rows.append({
                        "raw": raw_line,
                        "unit": nama_unit,
                        "pelanggaran": pelanggaran,
                        "status": "✅ MATCH",
                        "waktu": match["WAKTU KEJADIAN"],
                        "server": match["WAKTU KE SERVER GABUNGAN"],
                        "intervensi": match["WAKTU INTERVENSI"],
                        "context": match.get("INTERVENSI - STATUS CONTEXT", "-")
                    })

            except Exception as e:
                rows.append({
                    "raw": raw_line, "unit": "Format Error", "pelanggaran": "-", 
                    "status": f"❌ {str(e)}", "waktu": "", "server": "", "intervensi": ""
                })

        hasil = rows
        storage["last_rows"] = rows

    return render_template("index.html", hasil=hasil)

# ==================================================
# EXPORT TO EXCEL
# ==================================================
@app.route("/export")
def export_excel():
    if not storage["last_rows"]:
        return "Tidak ada data untuk di-export", 400

    df_export = pd.DataFrame(storage["last_rows"])
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False, sheet_name='Report')
    
    output.seek(0)
    return send_file(
        output, 
        download_name=f"Report_Matching_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx", 
        as_attachment=True
    )

if __name__ == "__main__":
    app.run(debug=True, port=5000)
