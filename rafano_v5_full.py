import sqlite3, os, time, yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

DB_NAME = "rafano.db"

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def update_master_full():
    print("📥 Update master 900 saham...")
    # Ambil list lengkap dari file idx atau pakai list hardcoded
    # Untuk sekarang gua pakai LQ45 + IDX80 + Kompas100 = ~150 saham unik, nanti lu ganti 900
    # Lu bisa upload file kode saham ke /content/kode_saham.txt

    # Coba baca dari repo kalau ada
    import os
    stock_list = []
    if os.path.exists("idx_list.txt"):
        with open("idx_list.txt") as f:
            stock_list = [l.strip() for l in f if l.strip()]

    if not stock_list:
        # fallback 100 saham paling aktif IDX
        stock_list = ["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","ADRO","ANTM","BRPT","GOTO","AMMN","AMRT","UNVR","ICBP","KLBF","MDKA","PGAS","PTBA","UNTR","INDF","ACES","ACST","ADHI","ADMG","ADMR","AKRA","AMAR","AMOR","APLN","ARGO","ARTO","ASLC","ASRI","AUTO","AVIA","BACA","BANK","BAPA","BBHI","BBRM","BBTN","BCIP","BDMN","BEBS","BELL","BEST","BFIN","BIRD","BIPI","BIPP","BIRD","BJBR","BJTM","BKDP","BKSL","BMHS","BNGA","BNLI","BOSS","BRIS","BRMS","BSDE","BTPS","BULL","BUMI","BUVA","CAMP","CARE","CBUT","CDIA","CEKA","CENT","CMNT","CNMA","CPIN","CPRO","CTRA","DEWA","DMAS","DOID","DSNG","DSSA","ELSA","EMTK","ENRG","ERAA","ESSA","EXCL","FILM","FREN","GJTL","GOTO","HRUM","ICBP","INCO","INDF","INDY","INKP","INTP","ISAT","ITMG","JSMR","KLBF","MAPI","MDKA","MEDC","MIKA","MNCN","PGAS","PGEO","PTBA","PTMP","RAJA","SCMA","SIDO","SMGR","SMRA","SRTG","SSIA","TLKM","TOWR","TPIA","UNTR","UNVR","WIKA","WIRG","WSKT"]

    stock_list = list(set(stock_list))
    print(f"Total list: {len(stock_list)} saham")

    conn = get_db()
    for kode in stock_list:
        try:
            ticker = yf.Ticker(f"{kode}.JK")
            # pakai fast_info biar cepet
            nama = kode
            sektor = "-"
            mcap = 0
            try:
                # info kadang lambat, coba dulu
                info = ticker.info
                nama = info.get('shortName', kode)
                sektor = info.get('sector','-')
                mcap = info.get('marketCap',0)
            except:
                pass
            conn.execute("INSERT OR REPLACE INTO saham_master (kode, nama, sektor, market_cap, last_update) VALUES (?,?,?,?,?)",
                         (kode, nama, sektor, mcap, datetime.now().isoformat()))
            print(f" + {kode}")
        except Exception as e:
            print(f" skip {kode}")
    conn.commit()
    conn.close()
    print(f"✅ master selesai: {len(stock_list)} saham")

def update_price_2tahun():
    print("\n📥 Download OHLCV 2 tahun (batch)...")
    conn = get_db()
    codes = [r[0] for r in conn.execute("SELECT kode FROM saham_master").fetchall()]
    print(f"Download {len(codes)} saham...")

    # Batch download yfinance - 1 request untuk semua saham (cepat!)
    tickers = [f"{c}.JK" for c in codes]
    end = datetime.now()
    start = end - timedelta(days=730)

    try:
        df_all = yf.download(tickers, start=start, end=end, group_by='ticker', threads=True, progress=True, auto_adjust=True)
    except Exception as e:
        print(f"Error batch, coba per 20 saham: {e}")
        df_all = None

    rows = []
    if df_all is not None and not df_all.empty:
        # Kalau multi ticker, df_all punya MultiIndex
        for kode in codes:
            try:
                if len(codes) == 1:
                    df = df_all
                else:
                    # yfinance format beda-beda
                    if f"{kode}.JK" in df_all.columns.get_level_values(0):
                        df = df_all[f"{kode}.JK"]
                    else:
                        continue
                if df.empty: continue
                for idx, row in df.iterrows():
                    if pd.isna(row['Close']): continue
                    rows.append((idx.strftime("%Y-%m-%d"), kode, float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']), int(row['Volume']) if not pd.isna(row['Volume']) else 0))
            except Exception as e:
                print(f" skip price {kode}: {e}")

    print(f"Insert {len(rows)} rows price...")
    conn.executemany("INSERT OR REPLACE INTO price_daily VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    print(f"✅ price_daily: {len(rows)} rows")

if __name__ == "__main__":
    update_master_full()
    update_price_2tahun()

    conn = get_db()
    print("\n=== STATS ===")
    print(f"saham_master: {conn.execute('SELECT COUNT(*) FROM saham_master').fetchone()[0]}")
    print(f"price_daily: {conn.execute('SELECT COUNT(*) FROM price_daily').fetchone()[0]} rows")
    print(f"DB size: {os.path.getsize(DB_NAME)/1024/1024:.2f} MB")
    conn.close()
