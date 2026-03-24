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

# 🔥 NORMALISASI ANGKA
df_raw["unit_clean"] = df_raw["unitno"].apply(lambda x: ''.join(filter(str.isdigit, str(x))))

# 🔥 BUANG NULL
df_raw = df_raw[df_raw["unit_clean"] != ""]

last_rows = []

# ==========================
# ALERT MAPPING
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
# ROUTE UTAMA (INDEX)
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

        if status_filter == "TRUE":
            rows = [r for r in rows if r[6] == "TRUE"]
        elif status_filter == "FALSE":
            rows = [r for r in rows if r[6] == "FALSE"]

        hasil = rows
        last_rows = rows

    return render_template("index.html", hasil=hasil)

# ==========================
# ROUTE TRUE
# ==========================
@app.route("/true")
def halaman_true():

    site = request.args.get("site", "brcg")
    unit_filter = request.args.get("unit", "")
    jam_filter = request.args.get("jam", "")

    table_name = "tbl_brcb" if site == "brcb" else "tbl_brcg"

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE LOWER("INTERVENSI - STATUS CONTEXT") = 'true'
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    df["WAKTU KEJADIAN"] = pd.to_datetime(df["WAKTU KEJADIAN"], errors='coerce')

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

            # 🔥 FILTER JAM
            if jam_filter:
                if waktu.hour != int(jam_filter):
                    continue

            # 🔥 FILTER UNIT
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
# ROUTE DEVICE STATUS
# ==========================
@app.route("/devicestatus")
def device_status():

    query = """
        SELECT "KODE KENDARAAN", "WAKTU KEJADIAN"
        FROM tbl_brcg
        WHERE "WAKTU KEJADIAN" > NOW() - INTERVAL '7 DAYS'
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    df["WAKTU KEJADIAN"] = pd.to_datetime(df["WAKTU KEJADIAN"], errors='coerce')

    df["unit_clean"] = df["KODE KENDARAAN"].astype(str).apply(
        lambda x: ''.join(filter(str.isdigit, x))
    )

    df["tanggal"] = df["WAKTU KEJADIAN"].dt.date

    today = datetime.now().date()

    results = []

    for _, r in df_raw.iterrows():

        unit_clean = r["unit_clean"]
        unit_name = r["unitno"]
        device_id = r["deviceid"]

        df_unit = df[df["unit_clean"] == unit_clean]

        daily = df_unit.groupby("tanggal").size()

        mean = daily.mean() if len(daily) > 0 else 0
        max_val = daily.max() if len(daily) > 0 else 0
        today_count = daily.get(today, 0)

        # 🔥 LAST ALERT
        if not df_unit.empty:
            last_time = df_unit["WAKTU KEJADIAN"].max()
            last_str = last_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            last_str = "-"

        if today_count == 0:
            status = "NO DATA"
        elif max_val > 0 and today_count >= max_val * 1.5:
            status = "SPIKE"
        elif today_count > mean + 3:
            status = "HIGH"
        else:
            status = "NORMAL"

        results.append({
            "device": device_id,
            "unit": unit_name,
            "avg": round(mean, 2),
            "max": int(max_val),
            "today": int(today_count),
            "last": last_str,
            "status": status
        })

    results = sorted(results, key=lambda x: x["today"], reverse=True)

    return render_template("devicestatus.html", data=results)

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
