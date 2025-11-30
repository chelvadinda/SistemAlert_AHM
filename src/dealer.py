from .distance import haversine

def cari_dealer_terdampak(koneksi, koordinat_gempa: str, radius_km: float = 100.0):
    """
    Menentukan dealer yang berada dalam radius tertentu dari pusat gempa.
    """
    try:
        lat_str, lon_str = koordinat_gempa.split(",")
        lat_gempa = float(lat_str.strip())
        lon_gempa = float(lon_str.strip())

        kursor = koneksi.cursor(dictionary=True)
        kursor.execute("""
            SELECT d.id_dealer, d.nama_dealer, d.alamat, d.koordinat,
                   md.nama_main_dealer, md.area
            FROM dealer d
            JOIN main_dealer md ON d.id_main_dealer = md.id_main_dealer
        """)
        dealer_list = kursor.fetchall()
        kursor.close()

        terdampak = []
        for d in dealer_list:
            try:
                lat_d, lon_d = map(float, d["koordinat"].replace(" ", "").split(","))
                jarak = haversine(lat_gempa, lon_gempa, lat_d, lon_d)
                if jarak <= radius_km:
                    terdampak.append({
                        "id_dealer": d["id_dealer"],
                        "nama_dealer": d["nama_dealer"],
                        "alamat": d["alamat"],
                        "nama_main_dealer": d["nama_main_dealer"],
                        "area": d["area"],
                        "jarak_km": round(jarak, 2)
                    })
            except Exception:
                continue

        if terdampak:
            print(f"\nDealer terdampak dalam radius {radius_km} km:")
            for t in terdampak:
                print(f"- {t['nama_dealer']} ({t['area']}) — {t['jarak_km']} km")
        else:
            print(f"\n Tidak ada dealer dalam radius {radius_km} km dari pusat gempa.")

        return terdampak
    except Exception as e:
        print("Terjadi kesalahan pada cari_dealer_terdampak:", e)
        return []
    
def save_dealer_terdampak(koneksi, id_gempa_terkini, daftar):
    try:
        kursor = koneksi.cursor()

        # Ambil tanggal & waktu dari tabel gempa_terkini
        kursor.execute("SELECT tanggal, waktu FROM gempa_terkini WHERE id_gempa_terkini = %s", (id_gempa_terkini,))
        result = kursor.fetchone()
        tanggal, waktu = result if result else (None, None)

        for d in daftar:
            sql = """
                INSERT INTO alert (id_gempa_terkini, id_dealer, tanggal, waktu,
                                   nama_dealer_terdampak, alamat_dealer_terdampak)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            params = (id_gempa_terkini, d["id_dealer"], tanggal, waktu, d["nama_dealer"], d["alamat"])
            kursor.execute(sql, params)

        kursor.close()
        print(f"{len(daftar)} dealer terdampak tersimpan di tabel alert.")
    except Exception as e:
        print("Gagal menyimpan dealer terdampak:", e)
