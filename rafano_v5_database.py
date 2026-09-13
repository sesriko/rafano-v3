import sqlite3, os, time, yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

DB_NAME = "rafano.db"

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS saham_master (
        kode TEXT PRIMARY KEY,
        nama TEXT,
        sektor TEXT,
        market_cap INTEGER,
        last_update TEXT
    )""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS price_daily (
        tanggal TEXT,
        kode TEXT,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (tanggal, kode)
    )""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS broksum_daily (
        tanggal TEXT,
        kode TEXT,
        broker TEXT,
        sisi TEXT,
        net_val INTEGER,
        lot INTEGER,
        avg_price INTEGER,
        PRIMARY KEY (tanggal, kode, broker)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_broksum_kode_tgl ON broksum_daily(kode, tanggal)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_price_kode_tgl ON price_daily(kode, tanggal)")
    conn.commit()
    conn.close()
    print(f"DB {DB_NAME} siap")

def update_master():
    print("Update saham_master...")
    # fallback list kalau fungsi get_idx_stock_list belum ada
    try:
        from rafano_v3 import get_idx_stock_list
        stock_list = get_idx_stock_list()
    except:
        stock_list = ["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","ADRO","ANTM","BRPT","GOTO","AMMN","AMRT","UNVR","ICBP","KLBF","MDKA","PGAS","PTBA","UNTR","INDF"]
    conn = get_db()
    for kode in stock_list[:20]: # test 20 dulu biar cepet
        try:
            ticker = yf.Ticker(f"{kode}.JK")
            info = ticker.fast_info
            nama = kode
            sektor = "-"
            mcap = 0
            try:
                info2 = ticker.info
                nama = info2.get('shortName', kode)
                sektor = info2.get('sector','-')
                mcap = info2.get('marketCap',0)
            except:
                pass
            conn.execute("INSERT OR REPLACE INTO saham_master VALUES (?,?,?,?,?)", (kode, nama, sektor, mcap, datetime.now().isoformat()))
            print(f"  + {kode}")
        except Exception as e:
            print(f"  skip {kode}: {e}")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    update_master()
    print("Cek DB:")
