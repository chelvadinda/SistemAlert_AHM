import time
from datetime import datetime
from src.config import load_src, get_db
from src.scraping import scrape_gempa, format_tanggal
from src.database import save_gempa, save_kejadian_perlokasi
from src.dealer import cari_dealer_terdampak, save_dealer_terdampak

def main():
    src = load_src()
    print("Mulai scraping tiap 1 menit... (Ctrl+C untuk berhenti)")

    koneksi = get_db()
    if not koneksi:
        print("Tidak bisa terhubung ke database.")
        return None
    else:
        print("Koneksi DB : OK")

    magnitudo = float(input("Masukkan magnitudo minimal: "))
    last_id = None

    try:
        while True:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Mengambil data dari BMKG...")
            data = scrape_gempa(src)

            if data is None:
                print("Data gagal diambil.")
            else:
                magnitudo_bmkg = float(data.get("Magnitude"))
                curr_id = (data.get("Tanggal"), data.get("Waktu"), data.get("Koordinat"))

                if curr_id == last_id:
                    print("Tidak ada data gempa terbaru")
                else:
                    if magnitudo_bmkg >= magnitudo:
                        for k, v in data.items():
                            print(f"{k:12}: {v}")
                        insert = save_gempa(koneksi, data, magnitudo)
                        if insert:
                            tahun = format_tanggal(data.get("Tanggal")).year
                            n = save_kejadian_perlokasi(koneksi, insert, data.get("Dirasakan"), tahun)
                            print(f"kejadian_perlokasi: +{n} baris")
                            dealer_terdampak = cari_dealer_terdampak(koneksi, data.get("Koordinat"), radius_km=100)
                            if dealer_terdampak:
                                save_dealer_terdampak(koneksi, insert, dealer_terdampak)
                                print(f"Total dealer terdampak: {len(dealer_terdampak)}")
                        last_id = curr_id
                    else:
                        print(
                            f"Data gempa diabaikan (magnitudo {data.get('Magnitude')} < {magnitudo})")
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nProgram dihentikan.")
    finally:
        if koneksi and koneksi.is_connected():
            koneksi.close()
            print("Koneksi DB ditutup.")


if __name__ == "__main__":
    main()