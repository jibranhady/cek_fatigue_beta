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

# ambil angka unit sekali saja
df_raw["ANGKA"] = df_raw["unitno"].astype(str).str.extract(r"(\d+)")

last_rows = []


# ==================================================
# ✅ MENU 1 — BULK (TIDAK DIUBAH)
# ==================================================
@app.route("/", methods=["GET", "POST"])
def index():

    global last_rows
    hasil = None

    if request.method == "POST":

        # upload file event
        if "file" in request.files:
            file = request.files["file"]
            if file.filename != "":
                file.save(UPLOAD_PATH)
                hasil = "✅ File laporan berhasil diupload"

        # bulk cek raw
        if "raw" in request.form:

            if not os.path.exists(UPLOAD_PATH):
                return render_template("index.html", hasil="❌ Upload laporan dulu")

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

            for raw in request.form["raw"].splitlines():

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


# ==================================================
# ✅ MENU 2 — REPORTING FINAL
# ==================================================
@app.route("/report", methods=["GET", "POST"])
def report():

    global last_rows
    hasil = None

    if request.method == "POST":

        tanggal_cek = request.form["tanggal"]
        shift = request.form["shift"]
        validated = request.form["validated"]

        file = request.files["file"]

        if file.filename == "":
            return render_template("report.html", hasil="❌ Upload file dulu")

        df = pd.read_excel(file)

        # cari kolom url video fleksibel
        url_cols = [c for c in df.columns if "video" in c.lower()]
        if not url_cols:
            return render_template("report.html", hasil="❌ Kolom URL VIDEO tidak ditemukan")

        url_col = url_cols[0]

        rows = []

        for _, r in df.iterrows():

            try:
                url = str(r[url_col]).strip()
                if not url or "http" not in url:
                    continue

                parts = url.split("/")
                sls = parts[-3]
                folder = parts[-2]

                raw_format = f"{sls}-{folder}"
                bagian1, tanggal, jam = raw_format.split("_")
                sls_fix, alert = bagian1.split("-")

                # =========================
                # KURANGI 1 JAM
                # =========================
                jam_dt = pd.to_datetime(jam, format="%H%M%S") - pd.Timedelta(hours=1)
                jam_final = jam_dt.strftime("%H%M%S")
                jam_int = int(jam_final[:2])

                # =========================
                # FILTER SHIFT (DIGABUNG)
                # =========================
                if shift == "1":
                    valid_jam = list(range(2,6)) + list(range(10,13))

                elif shift == "2":
                    valid_jam = list(range(10,13)) + list(range(14,17))

                elif shift == "3":
                    valid_jam = list(range(14,17)) + [22,23,0]

                if jam_int not in valid_jam:
                    continue

                # PID FINAL
                pid = f"{sls_fix}-{alert}_{tanggal}_{jam_final}"

                angka = ''.join(filter(str.isdigit, str(r["KODE KENDARAAN"])))
                match = df_raw[df_raw["ANGKA"] == angka]

                if match.empty:
                    continue

                distrik = match.iloc[0]["distrik"]
                ip = match.iloc[0]["device_ip"]

                rows.append([
                    tanggal_cek,
                    pid,
                    angka,
                    distrik,
                    sls_fix,
                    ip,
                    alert,
                    r["INTERVENSI - STATUS CONTEXT"],
                    r["WAKTU KE SERVER GABUNGAN"],
                    f"SHIFT {shift}",
                    validated,
                    url
                ])

            except:
                continue

        # kalau kosong
        if not rows:
            return render_template("report.html", hasil="⚠️ Data belum masuk")

        # urut berdasarkan waktu
        rows.sort(key=lambda x: x[1].split("_")[-1])

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
