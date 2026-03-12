from flask import Flask, render_template, request, send_file
import pandas as pd
import os
from io import BytesIO

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# =========================
# LOAD RAWDATA
# =========================
df_raw = pd.read_excel(os.path.join(BASE_DIR, "Rawdata.xlsx"))
df_raw["ANGKA"] = df_raw["unitno"].astype(str).str.extract(r"(\d+)")

last_rows = []

# ==================================================
# ✅ REPORT FINAL (ALL FIXED)
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

        # =========================
        # NORMALISASI KOLOM
        # =========================
        df.columns = df.columns.str.strip().str.upper()

        if "URL VIDEO" not in df.columns:
            return render_template("report.html", hasil=f"❌ Kolom tidak ada: {list(df.columns)}")

        if "KODE KENDARAAN" not in df.columns:
            return render_template("report.html", hasil="❌ Kolom KODE KENDARAAN tidak ada")

        rows = []

        # ==================================================
        # LOOP DATA
        # ==================================================
        for _, r in df.iterrows():

            try:

                # =========================
                # AMBIL URL
                # =========================
                url = str(r["URL VIDEO"]).strip()

                if not url or "http" not in url:
                    continue

                parts = url.split("/")
                if len(parts) < 3:
                    continue

                folder = parts[-2]  # CLOSEDEYES_20260310_001338

                if "_" not in folder:
                    continue

                alert, tanggal, jam = folder.split("_")

                # =========================
                # FIX WAKTU (PENTING)
                # =========================
                dt_full = pd.to_datetime(tanggal + jam, format="%Y%m%d%H%M%S")
                dt_final = dt_full - pd.Timedelta(hours=1)

                tanggal_final = dt_final.strftime("%Y%m%d")
                jam_final = dt_final.strftime("%H%M%S")
                jam_int = int(dt_final.strftime("%H"))

                # =========================
                # FILTER SHIFT
                # =========================
                if shift == "1":
                    valid_jam = list(range(2,6)) + list(range(10,13))

                elif shift == "2":
                    valid_jam = list(range(10,13)) + list(range(14,17))

                else:
                    valid_jam = list(range(14,17)) + [22,23,0]

                if jam_int not in valid_jam:
                    continue

                # =========================
                # AMBIL ANGKA DARI KENDARAAN
                # =========================
                kode = str(r["KODE KENDARAAN"]).strip()

                if "-" not in kode:
                    continue

                angka = kode.split("-")[-1]

                match = df_raw[df_raw["ANGKA"] == angka]
                if match.empty:
                    continue

                distrik = match.iloc[0]["distrik"]
                ip = match.iloc[0]["device_ip"]
                sls = match.iloc[0]["deviceid"]

                # =========================
                # PID FINAL
                # =========================
                pid = f"{sls}-{alert}_{tanggal_final}_{jam_final}"

                rows.append([
                    tanggal_cek,
                    pid,
                    angka,
                    distrik,
                    sls,
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

        # =========================
        # JIKA KOSONG
        # =========================
        if not rows:
            return render_template("report.html", hasil="⚠️ Data belum masuk")

        # =========================
        # SORT BERDASARKAN WAKTU
        # =========================
        rows.sort(key=lambda x: x[1].split("_")[-1])

        hasil = rows
        last_rows = rows

    return render_template("report.html", hasil=hasil)


# ==================================================
# EXPORT EXCEL
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
