from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)

# =========================
# LOAD RAW DATABASE (tetap)
# =========================
df_raw = pd.read_excel("Rawdata.xlsx")

# event nanti diisi setelah upload
df_event = None


@app.route("/", methods=["GET", "POST"])
def index():
    global df_event
    hasil = None

    # =========================
    # UPLOAD FILE LAPORAN
    # =========================
    if request.method == "POST" and "file" in request.files:
        file = request.files["file"]

        if file.filename != "":
            df_event = pd.read_excel(file)

            # format waktu
            df_event["WAKTU KEJADIAN"] = pd.to_datetime(df_event["WAKTU KEJADIAN"])
            df_event["WAKTU KE SERVER GABUNGAN"] = pd.to_datetime(df_event["WAKTU KE SERVER GABUNGAN"])

            # ambil angka unit
            df_event["ANGKA_UNIT"] = df_event["KODE KENDARAAN"].astype(str).str.extract(r"(\d+)")

            # ambil jam + tanggal
            df_event["JAM"] = df_event["WAKTU KEJADIAN"].dt.strftime("%H%M%S")
            df_event["TANGGAL"] = df_event["WAKTU KEJADIAN"].dt.strftime("%Y%m%d")

            hasil = "✅ File laporan berhasil diupload"

    # =========================
    # CEK RAW DATA
    # =========================
    if request.method == "POST" and "raw" in request.form:

        if df_event is None:
            hasil = "❌ Upload laporan dulu"
        else:
            raw = request.form["raw"]

            try:
                # parsing raw
                bagian1, tanggal, jam = raw.split("_")
                unit_raw, pelanggaran = bagian1.split("-")

                # kurangi 1 jam
                jam_dt = pd.to_datetime(jam, format="%H%M%S") - pd.Timedelta(hours=1)
                jam_final = jam_dt.strftime("%H%M%S")

                # =========================
                # CARI DI RAWDATA
                # =========================
                cek_unit = df_raw[df_raw["deviceid"] == unit_raw]

                if cek_unit.empty:
                    hasil = "❌ Unit tidak ditemukan"
                else:
                    nama_unit = cek_unit.iloc[0]["unitno"]

                    angka_unit = ''.join(filter(str.isdigit, nama_unit))

                    # =========================
                    # CARI EVENT
                    # =========================
                    cari = df_event[
                        (df_event["ANGKA_UNIT"] == angka_unit) &
                        (df_event["JAM"] == jam_final) &
                        (df_event["TANGGAL"] == tanggal)
                    ]

                    if cari.empty:
                        hasil = "❌ Data tidak ditemukan"
                    else:
                        row = cari.iloc[0]
                        hasil = {
                            "unit": nama_unit,
                            "pelanggaran": pelanggaran,
                            "kejadian": row["WAKTU KEJADIAN"],
                            "gabungan": row["WAKTU KE SERVER GABUNGAN"],
                        }

            except:
                hasil = "❌ Format raw salah"

    return render_template("index.html", hasil=hasil)


if __name__ == "__main__":
    app.run(debug=True)