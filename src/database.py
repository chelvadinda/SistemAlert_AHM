from mysql.connector import Error
from .scraping import format_tanggal
from .scraping import format_waktu
from .parsing import parse_dirasakan

def save_gempa(koneksi, data, magnitudo_min=0):
    magnitudo_bmkg = float(data.get("Magnitude"))
    if magnitudo_bmkg < magnitudo_min:
        print(f"Gempa {magnitudo_bmkg} < {magnitudo_min}, tidak disimpan ke DB.")
        return None

    kursor = koneksi.cursor()
    tanggal = data.get("Tanggal")
    waktu = data.get("Waktu")
    koordinat = data.get("Koordinat")

    tanggal = format_tanggal(tanggal)
    waktu = format_waktu(waktu)

    sql_check = "SELECT 1 FROM gempa_terkini WHERE tanggal=%s AND waktu=%s AND koordinat=%s LIMIT 1"
    kursor.execute(sql_check, (tanggal, waktu, koordinat))
    row = kursor.fetchone()

    if row:
        print("Data gempa sudah ada.")
        kursor.close()
        return None

    sql_insert = """
        INSERT INTO gempa_terkini
        (tanggal, waktu, wilayah, koordinat, lintang, bujur, magnitudo, kedalaman, potensi, dirasakan)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        tanggal,
        waktu,
        data.get("Wilayah"),
        data.get("Koordinat"),
        data.get("Lintang"),
        data.get("Bujur"),
        float(data.get("Magnitude")),
        data.get("Kedalaman"),
        data.get("Potensi"),
        data.get("Dirasakan")
    )

    try:
        kursor.execute(sql_insert, params)
        new_id = kursor.lastrowid
        print(f"Data berhasil tersimpan di database. id_gempa_terkini={new_id}")
        kursor.close()
        return new_id
    except Error as e:
        print("Data gagal disimpan ke database:", e)
        kursor.close()
        return None
    
def save_kejadian_perlokasi(koneksi, id_gempa_terkini: int, dirasakan: str, tahun: int):
    rows = parse_dirasakan(dirasakan)
    if not rows:
        return 0

    inserted = 0
    kursor = koneksi.cursor()

    for name, m1, m2 in rows:
        sql_check = "SELECT 1 FROM kejadian_perlokasi WHERE id_gempa_terkini=%s AND nama_kabupaten_kota=%s LIMIT 1"
        kursor.execute(sql_check, (id_gempa_terkini, name))
        if kursor.fetchone():
            continue

        # Cari id_kabupaten_kota & provinsi
        id_kabupaten_kota, id_provinsi = cari_id_kabupaten_dan_provinsi(koneksi, name)

        # debugging
        if id_kabupaten_kota:
            print(f"{name} → id_kabupaten_kota={id_kabupaten_kota}")
        else:
            print(f"{name} tidak ditemukan di tabel kabupaten_kota")

        sql_insert = """
            INSERT INTO kejadian_perlokasi
            (id_gempa_terkini, id_kabupaten_kota, nama_kabupaten_kota, tahun, mmi_min, mmi_max)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (id_gempa_terkini, id_kabupaten_kota, name, tahun, m1, m2)
        kursor.execute(sql_insert, params)
        inserted += kursor.rowcount

    kursor.close()

    # 🔹 Tambahkan pemanggilan fungsi baru di sini
    update_gempa_berdampak(koneksi, id_gempa_terkini, tahun)

    return inserted

def cari_id_kabupaten_dan_provinsi(koneksi, nama_bmkg: str):
    """
    Mencocokkan nama daerah (dari BMKG) ke tabel kabupaten_kota dan provinsi.
    Matching dilakukan di sisi Python agar lebih fleksibel.
    """
    kursor = koneksi.cursor(dictionary=True)
    kursor.execute("""
        SELECT k.id_kabupaten_kota, k.id_provinsi, k.nama_kabupaten_kota, p.nama_provinsi
        FROM kabupaten_kota k
        JOIN provinsi p ON k.id_provinsi = p.id_provinsi
    """)
    semua_data = kursor.fetchall()
    kursor.close()

    # Normalisasi nama BMKG (hapus kata Kab., Kota, tanda baca, dan spasi)
    def normalisasi(nama):
        return (
            nama.lower()
            .replace("kabupaten", "")
            .replace("kab.", "")
            .replace("kab ", "")
            .replace("kab", "")
            .replace("kota", "")
            .replace("-", "")
            .replace(".", "")
            .replace(",", "")
            .replace(" ", "")
            .strip()
        )

    nama_target = normalisasi(nama_bmkg)
    kandidat_cocok = []

    for row in semua_data:
        nama_db = normalisasi(row["nama_kabupaten_kota"])
        if nama_target in nama_db or nama_db in nama_target:
            kandidat_cocok.append(row)

    if not kandidat_cocok:
        print(f"{nama_bmkg} tidak ditemukan di tabel kabupaten_kota")
        return None, None

    # Jika ada beberapa hasil, prioritaskan yang mengandung 'kota' di nama aslinya
    for row in kandidat_cocok:
        if "kota" in row["nama_kabupaten_kota"].lower():
            print(f"{nama_bmkg} cocok dengan {row['nama_kabupaten_kota']}")
            return row["id_kabupaten_kota"], row["id_provinsi"]

    # Kalau tidak ada yang mengandung 'kota', ambil hasil pertama
    row = kandidat_cocok[0]
    print(f"{nama_bmkg} cocok dengan {row['nama_kabupaten_kota']}")
    return row["id_kabupaten_kota"], row["id_provinsi"]

def update_gempa_berdampak(koneksi, id_gempa_terkini: int, tahun: int):
    """
    Memperbarui tabel gempa_berdampak per provinsi per tahun.
    - total_kejadian  : semua gempa yang terjadi di provinsi tsb (tanpa filter MMI)
    - jumlah_berdampak: gempa dengan MMI >= 4 di provinsi tsb
    """
    kursor = koneksi.cursor(dictionary=True)

    # 🔹 Cari semua provinsi yang terlibat pada id_gempa_terkini ini
    sql_all_prov = """
        SELECT DISTINCT p.id_provinsi, p.nama_provinsi
        FROM kejadian_perlokasi kpl
        JOIN kabupaten_kota kk ON kpl.id_kabupaten_kota = kk.id_kabupaten_kota
        JOIN provinsi p ON kk.id_provinsi = p.id_provinsi
        WHERE kpl.id_gempa_terkini = %s
    """
    kursor.execute(sql_all_prov, (id_gempa_terkini,))
    prov_list = kursor.fetchall()

    if not prov_list:
        print("[ℹ️] Tidak ada provinsi terkait untuk gempa ini.")
        kursor.close()
        return 0

    for prov in prov_list:
        id_provinsi = prov["id_provinsi"]
        nama_provinsi = prov["nama_provinsi"]

        # 🔹 Hitung total kejadian gempa di provinsi ini pada tahun tsb (berdasarkan semua gempa_terkini)
        sql_total = """
            SELECT COUNT(DISTINCT gt.id_gempa_terkini) AS total
            FROM kejadian_perlokasi kpl
            JOIN kabupaten_kota kk ON kpl.id_kabupaten_kota = kk.id_kabupaten_kota
            JOIN provinsi p ON kk.id_provinsi = p.id_provinsi
            JOIN gempa_terkini gt ON kpl.id_gempa_terkini = gt.id_gempa_terkini
            WHERE p.id_provinsi = %s AND YEAR(gt.tanggal) = %s
        """
        kursor.execute(sql_total, (id_provinsi, tahun))
        total_kejadian = kursor.fetchone()["total"]

        # 🔹 Hitung jumlah gempa berdampak (MMI >= 4)
        sql_dampak = """
            SELECT COUNT(DISTINCT kpl.id_gempa_terkini) AS berdampak
            FROM kejadian_perlokasi kpl
            JOIN kabupaten_kota kk ON kpl.id_kabupaten_kota = kk.id_kabupaten_kota
            JOIN provinsi p ON kk.id_provinsi = p.id_provinsi
            WHERE p.id_provinsi = %s AND kpl.tahun = %s AND kpl.mmi_max >= 4
        """
        kursor.execute(sql_dampak, (id_provinsi, tahun))
        jumlah_berdampak = kursor.fetchone()["berdampak"]

        # 🔹 Cek apakah sudah ada data di gempa_berdampak
        sql_check = """
            SELECT id_gempa_berdampak
            FROM gempa_berdampak
            WHERE id_provinsi = %s AND tahun = %s
        """
        kursor.execute(sql_check, (id_provinsi, tahun))
        row = kursor.fetchone()

        if row:
            # Update total dan berdampak (sinkron dengan data aktual)
            sql_update = """
                UPDATE gempa_berdampak
                SET total_kejadian = %s,
                    jumlah_berdampak = %s
                WHERE id_gempa_berdampak = %s
            """
            kursor.execute(sql_update, (total_kejadian, jumlah_berdampak, row["id_gempa_berdampak"]))
            print(f"Update {nama_provinsi} {tahun}: total={total_kejadian}, berdampak={jumlah_berdampak}")
        else:
            # Tambah data baru
            sql_insert = """
                INSERT INTO gempa_berdampak (id_provinsi, provinsi, tahun, total_kejadian, jumlah_berdampak)
                VALUES (%s, %s, %s, %s, %s)
            """
            kursor.execute(sql_insert, (id_provinsi, nama_provinsi, tahun, total_kejadian, jumlah_berdampak))
            print(f"Tambah {nama_provinsi} {tahun}: total={total_kejadian}, berdampak={jumlah_berdampak}")

    kursor.close()
    return 1