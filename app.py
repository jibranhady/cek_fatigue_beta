from flask import Flask, render_template, request, send_file
import pandas as pd
import os
from io import BytesIO

app = Flask(__name__)

# =========================
# PATH
# =========================
UPLOAD_PATH = "upload.xlsx"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))

# simpan hasil terakhir buat export
last_rows = []


# =========================
# MENU 1 — BULK CHECK
# =========================
@app.route("/", methods=["GET", "POST"])
def index():

    global last_rows
    hasil = None

    if request.method == "POST":

        # UPLOAD FILE
        if "file" in request.files:

            file = request.files["file"]

            if file.filename != "":
                file.save(UPLOAD_PATH)
                hasil = "✅ File laporan berhasil diupload"

        # BULK CEK
        if "raw" in request.form:

            if not os.path.exists(UPLOAD_PATH):
                hasil = "❌ Upload laporan dulu"

            else:

                raw_text = request.form["raw"]

                if not raw_text.strip():
                    hasil = "❌ Tidak ada input"

                else:

                    df_event = pd.read_excel(
                        UPLOAD_PATH,
                        usecols=[
                            "KODE KENDARAAN",
                            "WAKTU KEJADIAN",
                            "WAKTU KE SERVER GABUNGAN",
                            "INTERVENSI - STATUS CONTEXT"
                        ]
                    )

                    df_event["WAKTU KEJADIAN"] = pd.to_datetime(df_event["WAKTU KEJADIAN"])
                    df_event["WAKTU KE SERVER GABUNGAN"] = pd.to_datetime(df_event["WAKTU KE SERVER GABUNGAN"])

                    df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")
                    df_event["JAM"] = df_event["WAKTU KEJADIAN"].dt.strftime("%H%M%S")
                    df_event["TANGGAL"] = df_event["WAKTU KEJADIAN"].dt.strftime("%Y%m%d")

                    rows = []

                    for raw in raw_text.splitlines():

                        raw = raw.strip().upper()
                        if not raw:
                            continue

                        try:
                            bagian1, tanggal, jam = raw.split("_")
                            unit_raw, pelanggaran = bagian1.split("-")

                            jam_dt = pd.to_datetime(jam, format="%H%M%S") - pd.Timedelta(hours=1)
                            jam_final = jam_dt.strftime("%H%M%S")

                            cek_unit = df_raw[df_raw["deviceid"] == unit_raw]

                            if cek_unit.empty:
                                rows.append([raw, "❌ Unit tidak ditemukan", "", "", "", ""])
                                continue

                            nama_unit = cek_unit.iloc[0]["unitno"]
                            angka_unit = ''.join(filter(str.isdigit, nama_unit))

                            cari = df_event[
                                (df_event["ANGKA_UNIT"] == angka_unit) &
                                (df_event["JAM"] == jam_final) &
                                (df_event["TANGGAL"] == tanggal)
                            ]

                            if cari.empty:
                                rows.append([raw, nama_unit, pelanggaran, "❌ Tidak ditemukan", "", ""])
                            else:
                                row = cari.iloc[0]
                                rows.append([
                                    raw,
                                    nama_unit,
                                    pelanggaran,
                                    row["WAKTU KEJADIAN"],
                                    row["WAKTU KE SERVER GABUNGAN"],
                                    row["INTERVENSI - STATUS CONTEXT"]
                                ])

                        except:
                            rows.append([raw, "❌ Format salah", "", "", "", ""])

                    hasil = rows
                    last_rows = rows

    return render_template("index.html", hasil=hasil)


# =========================
# MENU 2 — REPORTING HARIAN
# =========================
@app.route("/report", methods=["GET", "POST"])
def report():

    global last_rows
    hasil = None

    if request.method == "POST":

        tanggal_cek = request.form["tanggal"]
        shift = request.form["shift"]
        validated_by = request.form["validated"]
        jam_awal = request.form["jam_awal"]
        jam_akhir = request.form["jam_akhir"]

        file = request.files["file"]

        if file.filename != "":

            df = pd.read_excel(file)
            rows = []

            for _, r in df.iterrows():

                try:
                    url = str(r["URL VIDEO"])

                    parts = url.split("/")
                    sls = parts[-3]
                    alert_time = parts[-2]

                    alert, tanggal, jam = alert_time.split("_")

                    # kurangi 1 jam
                    jam_dt = pd.to_datetime(jam, format="%H%M%S") - pd.Timedelta(hours=1)
                    jam_final = jam_dt.strftime("%H%M%S")

                    # filter jam
                    if not (jam_awal.replace(":", "") <= jam_final <= jam_akhir.replace(":", "")):
                        continue

                    pid = f"{sls}-{alert}_{tanggal}_{jam_final}"

                    angka_unit = ''.join(filter(str.isdigit, str(r["KODE KENDARAAN"])))
                    match = df_raw[df_raw["unitno"].astype(str).str.contains(angka_unit)]

                    if match.empty:
                        continue

                    distrik = match.iloc[0]["distrik"]
                    device_ip = match.iloc[0]["device_ip"]

                    rows.append([
                        tanggal_cek,
                        pid,
                        angka_unit,
                        distrik,
                        sls,
                        device_ip,
                        alert,
                        r["INTERVENSI - STATUS CONTEXT"],
                        r["WAKTU KE SERVER GABUNGAN"],
                        "",
                        "",
                        f"SHIFT {shift}",
                        validated_by,
                        url
                    ])

                except:
                    continue

            rows.sort(key=lambda x: x[1])
            hasil = rows
            last_rows = rows

    return render_template("report.html", hasil=hasil)


# =========================
# EXPORT
# =========================
@app.route("/export")
def export_excel():

    global last_rows

    if not last_rows:
        return "Tidak ada data"

    df_export = pd.DataFrame(last_rows, columns=[
        "Tanggal Pengecekan",
        "PID",
        "ID Unit",
        "Distrik",
        "Device Number",
        "Device IP",
        "Alert",
        "Validasi OCR",
        "Feedback Site",
        "Validasi IOT Ops",
        "Keterangan",
        "Shift HO",
        "Validated By",
        "Link Video"
    ])

    output = BytesIO()
    df_export.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, download_name="reporting_harian.xlsx", as_attachment=True)


if __name__ == "__main__":
    app.run()
