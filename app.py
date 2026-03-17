from flask import Flask, render_template, request, send_file
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
import os

app = Flask(__name__)

# =========================
# DB CONNECTION (SUPABASE)
# =========================
DB_URL = "postgresql+psycopg2://postgres.xjnyjskeauyfpqioanse:matisaja123@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"
engine = create_engine(DB_URL)

# =========================
# LOAD RAWDATA (LOKAL)
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))

df_raw["deviceid_clean"] = df_raw["deviceid"].astype(str).str.strip().str.upper()

last_rows = []

# ==================================================
# ✅ BULK RAW (PAKAI DATABASE)
# ==================================================
@app.route("/", methods=["GET", "POST"])
def index():

    global last_rows
    hasil = None

    if request.method == "POST":

        raw_text = request.form["raw"]
        site = request.form["site"]

        if not raw_text.strip():
            return render_template("index.html", hasil="❌ Tidak ada RAW")

        # =========================
        # PILIH TABLE
        # =========================
        if site == "brcb":
            table = "tbl_brcb"
        else:
            table = "tbl_brcg"

        # =========================
        # AMBIL DATA DARI DATABASE
        # =========================
        df_event = pd.read_sql(f"SELECT * FROM {table}", engine)

        # =========================
        # NORMALISASI DATA
        # =========================
        waktu_cols = [
            "WAKTU KEJADIAN",
            "WAKTU KE SERVER GABUNGAN",
            "WAKTU INTERVENSI"
        ]

        for col in waktu_cols:
            if col in df_event.columns:
                df_event[col] = pd.to_datetime(df_event[col], errors="coerce")

        df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")
        df_event["JAM"] = df_event["WAKTU KEJADIAN"].dt.strftime("%H%M%S")
        df_event["TANGGAL"] = df_event["WAKTU KEJADIAN"].dt.strftime("%Y%m%d")

        rows = []

        # =========================
        # LOOP RAW
        # =========================
        for raw in raw_text.splitlines():

            raw = raw.strip().upper()

            if not raw:
                continue

            try:
                bagian1, tanggal, jam = raw.split("_")
                unit_raw, pelanggaran = bagian1.split("-")

                # 🔥 FIX -1 JAM
                jam_dt = pd.to_datetime(jam, format="%H%M%S") - pd.Timedelta(hours=1)
                jam_final = jam_dt.strftime("%H%M%S")

                cek_unit = df_raw[df_raw["deviceid_clean"] == unit_raw]

                if cek_unit.empty:
                    rows.append([raw, "❌ Unit tidak ditemukan", "", "", "", "", ""])
                    continue

                nama_unit = cek_unit.iloc[0]["unitno"]
                angka_unit = ''.join(filter(str.isdigit, nama_unit))

                # =========================
                # MATCH DATA
                # =========================
                cari = df_event[
                    (df_event["ANGKA_UNIT"] == angka_unit) &
                    (df_event["JAM"] == jam_final) &
                    (df_event["TANGGAL"] == tanggal)
                ]

                if cari.empty:
                    rows.append([raw, nama_unit, pelanggaran, "❌ Tidak ditemukan", "", "", ""])
                else:
                    row = cari.iloc[0]

                    rows.append([
                        raw,
                        nama_unit,
                        pelanggaran,
                        row["WAKTU KEJADIAN"],
                        row["WAKTU KE SERVER GABUNGAN"],
                        row["WAKTU INTERVENSI"],
                        row["INTERVENSI - STATUS CONTEXT"]
                    ])

            except:
                rows.append([raw, "❌ Format salah", "", "", "", "", ""])

        hasil = rows
        last_rows = rows

    return render_template("index.html", hasil=hasil)


# ==================================================
# EXPORT
# ==================================================
@app.route("/export")
def export_excel():

    if not last_rows:
        return "Tidak ada data"

    df = pd.DataFrame(last_rows)

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, download_name="report.xlsx", as_attachment=True)


# ==================================================
# RUN
# ==================================================
if __name__ == "__main__":
    app.run()
