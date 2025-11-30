import requests
from datetime import datetime, time as dtime
from .config import BULAN

def scrape_gempa(src: str) -> dict | None:
    try:
        respon = requests.get(src, timeout=30)
        respon.raise_for_status()
        data = respon.json()

        d = data["Infogempa"]["gempa"]

        return {
            "Tanggal": d.get("Tanggal"),
            "Waktu": d.get("Jam"),
            "Wilayah": d.get("Wilayah"),
            "Koordinat": d.get("Coordinates"),
            "Lintang": d.get("Lintang"),
            "Bujur": d.get("Bujur"),
            "Magnitude": d.get("Magnitude"),
            "Kedalaman": d.get("Kedalaman"),
            "Potensi": d.get("Potensi"),
            "Dirasakan": d.get("Dirasakan")
        }

    except requests.exceptions.RequestException as e:
        print("Kesalahan jaringan/HTTP:", e)
        return None
    except (KeyError, ValueError, TypeError) as e:
        print("Format data tidak sesuai:", e)
        return None
    except Exception as e:
        print("Terjadi kesalahan:", e)
        return None
    
def format_tanggal(s: str):
    parts = s.strip().split()
    day = int(parts[0])
    mon = parts[1]
    month = BULAN[mon]
    year = int(parts[2])
    return datetime(year, month, day).date()


def format_waktu(s: str):
    parts = s.strip().split()
    time_part = parts[0]
    hh, mm, ss = map(int, time_part.split(":"))
    return dtime(hh, mm, ss)