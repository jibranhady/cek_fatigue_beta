from flask import Flask, render_template, request, send_file
import pandas as pd
import os
from io import BytesIO

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_PATH = os.path.join(BASE_DIR, "upload.xlsx")

# =========================
# LOAD RAWDATA
# =========================
df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))

# normalize deviceid SEKALI SAJA
df_raw["deviceid_clean"] = df_raw["deviceid"].astype(str).str.strip().str.upper()

last_rows = []


# ==================================================
# ✅ MENU 1 — BULK (FINAL)
# ==================================================
@app.route("/", methods=["GET", "POST"])
def index():

    global last_rows
    hasil = None

    if request.method == "POST":

        # =========================
        # UPLOAD FILE EVENT
        # =========================
        if "file" in request.files:

            file = request.files["file"]

            if file.filename != "":
                file.save(UPLOAD_PATH)
                hasil = "✅ File laporan berhasil diupload"

        # =========================
        # BULK CEK RAW
        # =========================
        if "raw" in request.form:

            if not os.path.exists(UPLOAD_PATH):
                return render_template("index.html", hasil="❌ Upload laporan dulu")

            df_event = pd.read_excel(
                UPLOAD_PATH,
                usecols=[
                    "KODE KENDARAAN",
                    "WAKTU KEJADIAN",
                    "WAKTU KE SERVER GABUNGAN",
                    "WAKTU INTERVENSI",
                    "INTERVENSI - STATUS CONTEXT"
                ]
            )

            df_event["WAKTU KEJADIAN"] = pd.to_datetime(df_event["WAKTU KEJADIAN"])
            df_event["WAKTU KE SERVER GABUNGAN"] = pd.to_datetime(df_event["WAKTU KE SERVER GABUNGAN"])
            df_event["WAKTU INTERVENSI"] = pd.to_datetime(df_event["WAKTU INTERVENSI"])

            df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")
            df_event["JAM"] = df_event["WAKTU KEJADIAN"].dt.strftime("%H%M%S")
            df_event["TANGGAL"] = df_event["WAKTU KEJADIAN"].dt.strftime("%Y%m%d")

            rows = []

            for raw in request.form["raw"].splitlines():

                raw = raw.strip().upper()
                if not raw:
                    continue

                try:
                    bagian1, tanggal, jam = raw.split("_")
                    unit_raw, pelanggaran = bagian1.split("-")

                    # 🔥 RAW dikurangi 1 jam
                    jam_dt = pd.to_datetime(jam, format="%H%M%S") - pd.Timedelta(hours=1)
                    jam_final = jam_dt.strftime("%H%M%S")

                    cek_unit = df_raw[df_raw["deviceid_clean"] == unit_raw]

                    if cek_unit.empty:
                        rows.append([raw, "❌ Unit tidak ditemukan", "", "", "", "", ""])
                        continue

                    nama_unit = cek_unit.iloc[0]["unitno"]
                    angka_unit = ''.join(filter(str.isdigit, nama_unit))

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
# ✅ REPORT — INPUT URL
# ==================================================
@app.route("/report", methods=["GET", "POST"])
def report():

    global last_rows
    hasil = None

    if request.method == "POST":

        tanggal_cek = request.form["tanggal"]
        shift = request.form["shift"]
        validated = request.form["validated"]

        url_text = request.form["urls"]

        if not url_text.strip():
            return render_template("report.html", hasil="❌ Tidak ada URL")

        rows = []

        for url in url_text.splitlines():

            url = url.strip()

            if not url.startswith("http"):
                continue

            try:

                parts = url.split("/")
                if len(parts) < 3:
                    continue

                sls = parts[-3].strip().upper()
                folder = parts[-2]

                folder_clean = folder.replace(".mp4", "")
                alert, tanggal, jam = folder_clean.split("_")

                # 🔥 FIX WAKTU (-1 JAM)
                dt_full = pd.to_datetime(tanggal + jam, format="%Y%m%d%H%M%S")
                dt_final = dt_full - pd.Timedelta(hours=1)

                tanggal_final = dt_final.strftime("%Y%m%d")
                jam_final = dt_final.strftime("%H%M%S")

                match = df_raw[df_raw["deviceid_clean"] == sls]

                if match.empty:
                    continue

                distrik = match.iloc[0]["distrik"]
                ip = match.iloc[0]["device_ip"]

                pid = f"{sls}-{alert}_{tanggal_final}_{jam_final}"

                rows.append([
                    tanggal_cek,
                    pid,
                    distrik,
                    sls,
                    ip,
                    alert,
                    "",  # waktu intervensi kosong (ga ada di URL)
                    "",
                    f"SHIFT {shift}",
                    validated,
                    url
                ])

            except:
                continue

        if not rows:
            hasil = "⚠️ Tidak ada data terbaca"

        else:
            rows.sort(key=lambda x: x[1])
            hasil = rows
            last_rows = rows

    return render_template("report.html", hasil=hasil)


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


if __name__ == "__main__":
    app.run()
