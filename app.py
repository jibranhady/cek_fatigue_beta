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

# NORMALISASI
df_raw["unit_clean"] = df_raw["unitno"].apply(lambda x: ''.join(filter(str.isdigit, str(x))))
df_raw = df_raw[df_raw["unit_clean"] != ""]

last_rows = []

# ==========================
# ALERT MAP
# ==========================
def map_alert(alert):
    mapping = {
        "MENUTUP MATA": "CLOSED_EYES",
        "MENGANTUK": "DROWSY",
        "MENGUAP": "YAWNING",
        "HEAD NOD": "HEAD_NOD"
    }
    return mapping.get(str(alert).strip().upper(), str(alert).upper().replace(" ", "_"))

# ==========================
# LTIME
# ==========================
def hitung_ltime(alert, bedms):
    try:
        t1 = pd.to_datetime(alert)
        t2 = pd.to_datetime(bedms)

        diff = t2 - t1
        total_sec = int(diff.total_seconds())

        if total_sec < 0:
            return "-"

        return f"{total_sec//60}m {total_sec%60}s"
    except:
        return "-"

# ==========================
# STATUS NORMALIZER 🔥
# ==========================
def normalize_status(val):
    if pd.isnull(val):
        return "NOT FOUND"

    val = str(val).strip().lower()

    if val in ["true", "t", "1"]:
        return "TRUE"
    else:
        return "FALSE"

# ==========================
# INDEX (FIX MATCHING)
# ==========================
@app.route("/", methods=["GET", "POST"])
def index():
    global last_rows
    hasil = None

    if request.method == "POST":
        raw_text = request.form.get("raw", "")
        site = request.form.get("site", "brcb")
        status_filter = request.form.get("status_filter", "ALL")

        table_name = "tbl_brcb" if site == "brcb" else "tbl_brcg"

        with engine.connect() as conn:
            df_event = pd.read_sql(text(f"""
                SELECT *
                FROM {table_name}
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

                # 🔥 MATCH PALING DEKAT
                df_unit_match = df_event[df_event["ANGKA_UNIT"] == angka_unit].copy()

                if df_unit_match.empty:
                    match = pd.DataFrame()
                else:
                    df_unit_match["selisih"] = (df_unit_match["WAKTU KEJADIAN"] - waktu_lookup).abs()
                    match = df_unit_match.sort_values("selisih").head(1)

                    if match.iloc[0]["selisih"] > timedelta(minutes=2):
                        match = pd.DataFrame()

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

                    # 🔥 STATUS FIX
                    status = normalize_status(res.get("INTERVENSI - STATUS CONTEXT"))

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

        if status_filter == "TRUE":
            rows = [r for r in rows if r[6] == "TRUE"]
        elif status_filter == "FALSE":
            rows = [r for r in rows if r[6] == "FALSE"]

        hasil = rows
        last_rows = rows

    return render_template("index.html", hasil=hasil)

# ==========================
# ROUTE TRUE (AMAN)
# ==========================
@app.route("/true")
def halaman_true():

    site = request.args.get("site", "brcg")
    unit_filter = request.args.get("unit", "")
    jam_filter = request.args.get("jam", "")

    table_name = "tbl_brcb" if site == "brcb" else "tbl_brcg"

    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT *
            FROM {table_name}
            WHERE LOWER("INTERVENSI - STATUS CONTEXT") = 'true'
        """), conn)

    df["WAKTU KEJADIAN"] = pd.to_datetime(df["WAKTU KEJADIAN"])

    rows = []

    for _, r in df.iterrows():
        try:
            unit = str(r["KODE KENDARAAN"])
            unit_number = ''.join(filter(str.isdigit, unit))

            device_match = df_raw[df_raw["unit_clean"] == unit_number]
            if device_match.empty:
                continue

            device_id = device_match.iloc[0]["deviceid"]
            waktu = r["WAKTU KEJADIAN"]

            if jam_filter and waktu.hour != int(jam_filter):
                continue

            if unit_filter and unit_filter.upper() not in unit.upper():
                continue

            alert_en = map_alert(r["PERINGATAN"])

            pid = f"{device_id}-{alert_en}_{waktu.strftime('%Y%m%d_%H%M%S')}"

            rows.append([
                pid,
                unit,
                alert_en,
                waktu.strftime('%Y-%m-%d %H:%M:%S'),
                r.get("URL VIDEO") or "#"
            ])

        except:
            continue

    return render_template("true.html", data=rows)

# ==========================
# RUN
# ==========================
if __name__ == "__main__":
    app.run(debug=True)
