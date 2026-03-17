from flask import Flask, render_template, request, send_file
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO

app = Flask(__name__)

# =========================
# DATABASE
# =========================
conn_str = "postgresql://postgres:matisaja123@db.xjnyjskeauyfpqioanse.supabase.co:5432/postgres"
engine = create_engine(conn_str)

# =========================
# RAWDATA
# =========================
df_raw = pd.read_excel("Rawdata.xlsx")
df_raw["deviceid_clean"] = df_raw["deviceid"].astype(str).str.strip().str.upper()

last_rows = []

# ==================================================
# BULK RAW
# ==================================================
@app.route("/", methods=["GET", "POST"])
def index():

    global last_rows
    hasil = None

    if request.method == "POST":

        raw_text = request.form["raw"]
        site = request.form["site"]

        # 🔥 PILIH TABLE BERDASARKAN SITE
        if site == "brcb":
            table_name = "tbl_event_brcb"
        else:
            table_name = "tbl_event_brcg"

        df_event = pd.read_sql(f"SELECT * FROM {table_name}", engine)

        df_event["ANGKA_UNIT"] = df_event["kode_kendaraan"].astype(str).str.extract(r"(\d+)")
        df_event["JAM"] = df_event["waktu_kejadian"].dt.strftime("%H%M%S")
        df_event["TANGGAL"] = df_event["waktu_kejadian"].dt.strftime("%Y%m%d")

        rows = []

        for raw in raw_text.splitlines():

            raw = raw.strip().upper()
            if not raw:
                continue

            try:
                bagian1, tanggal, jam = raw.split("_")
                unit_raw, pelanggaran = bagian1.split("-")

                dt = pd.to_datetime(tanggal + jam, format="%Y%m%d%H%M%S")
                dt_fix = dt - pd.Timedelta(hours=1)

                tanggal_fix = dt_fix.strftime("%Y%m%d")
                jam_fix = dt_fix.strftime("%H%M%S")

                cek = df_raw[df_raw["deviceid_clean"] == unit_raw]

                if cek.empty:
                    rows.append([raw, "❌ Unit tidak ditemukan", "", "", "", "", ""])
                    continue

                angka = ''.join(filter(str.isdigit, cek.iloc[0]["unitno"]))

                cari = df_event[
                    (df_event["ANGKA_UNIT"] == angka) &
                    (df_event["JAM"] == jam_fix) &
                    (df_event["TANGGAL"] == tanggal_fix)
                ]

                if cari.empty:
                    rows.append([raw, angka, pelanggaran, "❌ Tidak ditemukan", "", "", ""])
                else:
                    row = cari.iloc[0]

                    rows.append([
                        raw,
                        angka,
                        pelanggaran,
                        row["waktu_kejadian"],
                        row["waktu_server_gabungan"],
                        row["waktu_intervensi"],
                        row["status_context"]
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


if __name__ == "__main__":
    app.run()
