from flask import Flask, render_template, request, send_file
import pandas as pd
from sqlalchemy import create_engine, text
from io import BytesIO
import os
from datetime import datetime, timedelta

app = Flask(__name__)

# ==========================
# CONFIG DB
# ==========================
DB_URL = "postgresql+psycopg2://postgres.xjnyjskeauyfpqioanse:matisaja123@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres?sslmode=require"

engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=300)

# ==========================
# LOAD RAWDATA
# ==========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))
df_raw["deviceid"] = df_raw["deviceid"].astype(str).str.strip().str.upper()
df_raw["unitno"] = df_raw["unitno"].astype(str).str.strip().str.upper()

last_rows = []

# ==========================
# HITUNG LTIME
# ==========================
def hitung_ltime(alert, bedms):
    try:
        t1 = pd.to_datetime(alert)
        t2 = pd.to_datetime(bedms)

        diff = t2 - t1
        total_sec = int(diff.total_seconds())

        if total_sec < 0:
            return "-"

        m = total_sec // 60
        s = total_sec % 60

        return f"{m}m {s}s"
    except:
        return "-"

# ==========================
# ROUTE
# ==========================
@app.route("/", methods=["GET", "POST"])
def index():
    global last_rows
    hasil = None

    if request.method == "POST":
        raw_text = request.form.get("raw", "")
        site = request.form.get("site", "brcb")

        table_name = "tbl_brcb" if site == "brcb" else "tbl_brcg"

        # ambil data 24 jam
        with engine.connect() as conn:
            df_event = pd.read_sql(text(f"""
                SELECT * FROM {table_name}
                WHERE "WAKTU KEJADIAN" > NOW() - INTERVAL '24 HOURS'
            """), conn)

        df_event["WAKTU KEJADIAN"] = pd.to_datetime(df_event["WAKTU KEJADIAN"])
        df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")

        rows = []

        for raw in raw_text.splitlines():
            raw = raw.strip().upper()
            if not raw:
                continue

            try:
                parts = raw.split("_")

                if len(parts) < 3:
                    rows.append([raw, "FORMAT SALAH", "-", "-", "-", "-", "ERROR"])
                    continue

                bagian1, tanggal_raw, jam_raw = parts[0], parts[1], parts[2]
                unit_raw, pelanggaran = bagian1.split("-")

                waktu_lookup = datetime.strptime(tanggal_raw + jam_raw, "%Y%m%d%H%M%S")

                cek_unit = df_raw[df_raw["deviceid"] == unit_raw]

                if cek_unit.empty:
                    rows.append([raw, "UNMAPPED", pelanggaran, "-", "-", "-", "ERROR"])
                    continue

                nama_unit = cek_unit.iloc[0]["unitno"]
                angka_unit = ''.join(filter(str.isdigit, str(nama_unit)))

                match = df_event[
                    (df_event["ANGKA_UNIT"] == angka_unit) &
                    (df_event["WAKTU KEJADIAN"] >= waktu_lookup - timedelta(minutes=2)) &
                    (df_event["WAKTU KEJADIAN"] <= waktu_lookup + timedelta(minutes=2))
                ]

                if match.empty:
                    rows.append([
                        raw,
                        nama_unit,
                        pelanggaran,
                        "TIDAK DITEMUKAN",
                        "-",
                        "-",
                        "FALSE"
                    ])
                else:
                    res = match.iloc[0]

                    time_alert = res["WAKTU KEJADIAN"]
                    time_bedms = res["WAKTU KE SERVER GABUNGAN"]

                    ltime = hitung_ltime(time_alert, time_bedms)

                    status_val = res.get("INTERVENSI - STATUS CONTEXT")
                    status = str(status_val).strip().upper() if pd.notnull(status_val) else "NOT FOUND"

                    rows.append([
                        raw,
                        nama_unit,
                        pelanggaran,
                        time_alert.strftime('%Y-%m-%d %H:%M:%S'),
                        time_bedms.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(time_bedms) else "-",
                        ltime,
                        status
                    ])

            except Exception as e:
                rows.append([raw, "ERROR", str(e), "-", "-", "-", "ERROR"])

        hasil = rows
        last_rows = rows

    return render_template("index.html", hasil=hasil)

# ==========================
# EXPORT
# ==========================
@app.route("/export")
def export_excel():
    global last_rows

    df_export = pd.DataFrame(last_rows, columns=[
        "PID", "Unit", "Alert", "Time Alert",
        "Time BeDMS", "LTime", "Status"
    ])

    output = BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False)

    output.seek(0)

    return send_file(output, download_name="hasil.xlsx", as_attachment=True)

# ==========================
# RUN
# ==========================
if __name__ == "__main__":
    app.run(debug=True)
