from flask import Flask, render_template, request, send_file
import pandas as pd
from sqlalchemy import create_engine, text
from io import BytesIO
import os
from datetime import datetime, timedelta

app = Flask(__name__)

# ==================================================
# CONFIG DB
# ==================================================
DB_URL = "postgresql+psycopg2://postgres.xjnyjskeauyfpqioanse:matisaja123@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres?sslmode=require"

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_recycle=300
)

# ==================================================
# LOAD RAWDATA MAPPING
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))
    df_raw["deviceid"] = df_raw["deviceid"].astype(str).str.strip().str.upper()
    df_raw["unitno"] = df_raw["unitno"].astype(str).str.strip().str.upper()
except Exception as e:
    print(f"Error Rawdata.xlsx: {e}")
    df_raw = pd.DataFrame()

last_rows = []

# ==================================================
# ROUTE UTAMA
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

        # ==================================================
        # AMBIL DATA DB (24 JAM TERAKHIR)
        # ==================================================
        try:
            with engine.connect() as conn:
                query = text(f"""
                    SELECT * FROM {table_name}
                    WHERE "WAKTU KEJADIAN" > NOW() - INTERVAL '24 HOURS'
                """)
                df_event = pd.read_sql(query, conn)
        except Exception as e:
            return render_template("index.html", error=f"❌ DB Error: {e}")

        if df_event.empty:
            return render_template("index.html", error="❌ Database kosong (24 jam terakhir).")

        # ==================================================
        # PREPROCESS DATA DB
        # ==================================================
        df_event["WAKTU KEJADIAN"] = pd.to_datetime(df_event["WAKTU KEJADIAN"]).dt.tz_localize(None)
        df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")

        rows = []

        # ==================================================
        # LOOP RAW INPUT
        # ==================================================
        for raw in raw_text.splitlines():
            raw = raw.strip().upper()
            if not raw:
                continue

            try:
                # FORMAT: SLS30I049-CLOSEDEYES_20260317_151611
                parts = raw.split("_")

                if len(parts) < 3:
                    rows.append([raw, "❌ Format Salah", "-", "-", "-", "-", "ERROR"])
                    continue

                bagian1, tanggal_raw, jam_raw = parts[0], parts[1], parts[2]
                unit_raw, pelanggaran = bagian1.split("-")

                # ==================================================
                # TANPA OFFSET (DB SUDAH BENAR)
                # ==================================================
                waktu_lookup = datetime.strptime(tanggal_raw + jam_raw, "%Y%m%d%H%M%S")

                # ==================================================
                # CEK MAPPING DEVICE → UNIT
                # ==================================================
                cek_unit = df_raw[df_raw["deviceid"] == unit_raw]

                if cek_unit.empty:
                    rows.append([raw, "❌ Device ID Unmapped", pelanggaran, "-", "-", "-", "ERROR"])
                    continue

                nama_unit_full = cek_unit.iloc[0]["unitno"]
                angka_unit_lookup = ''.join(filter(str.isdigit, str(nama_unit_full)))

                # ==================================================
                # MATCHING (PAKAI RANGE ±2 MENIT)
                # ==================================================
                match = df_event[
                    (df_event["ANGKA_UNIT"] == angka_unit_lookup) &
                    (df_event["WAKTU KEJADIAN"] >= waktu_lookup - timedelta(minutes=2)) &
                    (df_event["WAKTU KEJADIAN"] <= waktu_lookup + timedelta(minutes=2))
                ]

                if match.empty:
                    rows.append([
                        raw,
                        nama_unit_full,
                        pelanggaran,
                        "❌ Tidak ditemukan",
                        "-",
                        "-",
                        "FALSE"
                    ])
                else:
                    res = match.iloc[0]

                    # ==========================
                    # STATUS (BOOLEAN SAFE)
                    # ==========================
                    status_raw = res.get("INTERVENSI - STATUS CONTEXT")

                    status_context = (
                        "TRUE" if status_raw is True else
                        "FALSE" if status_raw is False else
                        "NOT FOUND"
                    )

                    # ==========================
                    # WAKTU INTERVENSI (TEXT SAFE)
                    # ==========================
                    waktu_intervensi = res.get("WAKTU INTERVENSI", "-")

                    if pd.notnull(waktu_intervensi):
                        try:
                            waktu_intervensi = pd.to_datetime(waktu_intervensi).strftime("%H:%M:%S")
                        except:
                            waktu_intervensi = str(waktu_intervensi)
                    else:
                        waktu_intervensi = "-"

                    rows.append([
                        raw,
                        nama_unit_full,
                        pelanggaran,
                        res["WAKTU KEJADIAN"].strftime('%Y-%m-%d %H:%M:%S'),
                        res["WAKTU KE SERVER GABUNGAN"].strftime('%H:%M:%S') if pd.notnull(res["WAKTU KE SERVER GABUNGAN"]) else "-",
                        waktu_intervensi,
                        status_context
                    ])

            except Exception as e:
                rows.append([raw, "❌ Format salah", str(e), "-", "-", "-", "ERROR"])

        hasil = rows
        last_rows = rows

    return render_template("index.html", hasil=hasil)

# ==================================================
# EXPORT EXCEL
# ==================================================
@app.route("/export")
def export_excel():
    global last_rows

    if not last_rows:
        return "Tidak ada data"

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
        download_name=f"Hasil_{datetime.now().strftime('%Y%m%d')}.xlsx",
        as_attachment=True
    )

# ==================================================
# RUN APP
# ==================================================
if __name__ == "__main__":
    app.run(debug=True)
