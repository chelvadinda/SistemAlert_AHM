import os
import mysql.connector
from dotenv import load_dotenv
from mysql.connector import Error

#Kamus
ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
         "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}
BULAN = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "Mei": 5,
         "Jun": 6, "Jul": 7, "Agu": 8, "Sep": 9, "Okt": 10, "Nov": 11, "Des": 12}

def load_src() -> str:
    load_dotenv()
    src = os.getenv("SOURCE_URL", "").strip()
    if not src:
        raise RuntimeError("SOURCE_URL tidak tersedia.")
    return src

def get_db():
    load_dotenv()
    try:
        koneksi = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            autocommit=True
        )
        return koneksi
    except Error as e:
        print("Gagal terhubung ke MySQL:", e)
        return None