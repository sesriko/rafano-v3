
"""
RAFANO V5 + BANDAR DB - Opsi B
- OHLCV dari yfinance (116 saham)
- broker_summary dari Arjum -> save ke SQLite
- Rate limiter 1.3s anti-429
"""
import os, time, sqlite3, datetime
import yfinance as yf
import pandas as pd
import requests

DB_PATH = "rafano.db"
ARJUM_BASE = "https://stock.arjum.com/api"
ARJUM_KEY = os.getenv("ARJUM_API_KEY") or ""

# List 116 saham dari V5 lu (LQ45 core)
SYMBOLS = ["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","BMTR","BIPI","GOTO","BUKA","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","BREN","CUAN","WIFI","DEWA","BULL","NCKL","AMRT","TOWR","TBIG","ELSA","BEKS","BNGA","AALI","ACES","ADMR","AKRA","AMMN","BRMS","BRPT","BSDE","CPIN","CTRA","EMTK","ICBP","INDF","INKP","INTP","ITMG","JPFA","KLBF","MEDC","SMGR","SMRA","TPIA","UNTR","UNVR","ARTO","BBYB","BFIN","BIRD","BELL","BEST","BIPI","BKSL","BMTR","BNLI","BRMS","BTPS","BUKA","BYAN","CTRA","DEWA","DMAS","DOID","ELSA","ENRG","ERAA","ESSA","EXCL","FILM","GJTL","GOTO","HRUM","INCO","INDY","INKP","INPC","INTP","IPCM","ISAT","ITMG","JSMR","KLBF","LPPF","MAPI","MBMA","MDKA","MEDC","MIKA","MNCN","MTEL","MYOR","NCKL","PGAS","PGEO","PTBA","PTRO","RAJA","RALS","SCMA","SIDO","SRTG","SSIA","TLKM","TOWR","TPIA","UNTR","UNVR","WIKA","WIFI","WTON","ADRO","AKRA","AMMN","AMRT","ANTM","ARTO","BBCA","BBKP","BBNI","BBRI","BFIN","BMRI","BRIS","BRPT","BSDE","BUKA","CPIN","EMTK","GOTO","ICBP","INDF","INDY","INKP","INTP","ITMG","KLBF","MDKA","MEDC","PGAS","PTBA","SMGR","TLKM","TOWR","TPIA","UNTR","UNVR"]

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # ohlcv udah ada dari V5
    cur.execute("""CREATE TABLE IF NOT EXISTS ohlcv (
        symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY(symbol, date))""")
    # BARU: broker summary
    cur.execute("""CREATE TABLE IF NOT EXISTS broker_summary (
        symbol TEXT, date TEXT, broker_code TEXT,
        bval REAL, sval REAL, nval REAL,
        bvol INTEGER, svol INTEGER,
        PRIMARY KEY(symbol, date, broker_code))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS bandar_daily (
        symbol TEXT, date TEXT PRIMARY KEY,
        foreign_net_1w REAL, foreign_str_1w TEXT, accum_type TEXT,
        total_bval REAL, total_sval REAL)""")
    conn.commit(); conn.close()

def update_ohlcv():
    print("📈 Update OHLCV yfinance 116 saham...")
    conn = sqlite3.connect(DB_PATH)
    for sym in SYMBOLS:
        try:
            ticker = yf.Ticker(f"{sym}.JK")
            hist = ticker.history(period="2y", auto_adjust=False)
            if hist.empty: continue
            hist = hist.reset_index()
            for _, row in hist.iterrows():
                conn.execute("INSERT OR REPLACE INTO ohlcv VALUES (?,?,?,?,?,?,?)",
                    (sym, row['Date'].strftime("%Y-%m-%d"), float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']), int(row['Volume'])))
            print(f" {sym} {len(hist)} rows")
        except Exception as e:
            print(f" {sym} err {e}")
        time.sleep(0.3)
    conn.commit(); conn.close()

def update_bandar():
    if not ARJUM_KEY:
        print("⚠ ARJUM_API_KEY kosong, skip bandar")
        return
    print("🏦 Update Bandar dari Arjum...")
    conn = sqlite3.connect(DB_PATH)
    # 5 hari bursa terakhir
    end = datetime.date.today()
    start = end - datetime.timedelta(days=12)

    for sym in SYMBOLS:
        try:
            url = f"{ARJUM_BASE}/broker-summary/{sym}"
            params = {"start_date": start.strftime("%Y-%m-%d"), "end_date": end.strftime("%Y-%m-%d"), "flow": "all", "all_data": "true", "broker_limit": 20}
            headers = {"X-API-Key": ARJUM_KEY, "Accept": "application/json"}
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code == 429:
                print(f" 429 {sym} tunggu 6s")
                time.sleep(6)
                continue
            if r.status_code!= 200:
                print(f" {sym} {r.status_code}")
                time.sleep(1.3); continue
            data = r.json()
            brokers = data.get('brokers', [])
            if not brokers:
                time.sleep(1.3); continue

            total_b = sum([float(b.get('bval') or 0) for b in brokers])
            total_s = sum([float(b.get('sval') or 0) for b in brokers])
            net_1w = sum([float(b.get('nval') or 0) for b in brokers])

            # format
            def fmt(v):
                if abs(v)>=1e12: return f"{'+' if v>0 else ''}{v/1e12:.2f}T"
                elif abs(v)>=1e9: return f"{'+' if v>0 else ''}{v/1e9:.2f}B"
                elif abs(v)>=1e6: return f"{'+' if v>0 else ''}{v/1e6:.0f}M"
                else: return f"{int(v)}"

            if net_1w>5e9: atype="BIG ACCUM"
            elif net_1w>1e9: atype="ACCUM"
            elif net_1w<-5e9: atype="BIG DIST"
            elif net_1w<-1e9: atype="DIST"
            else: atype="NEUTRAL"

            today = datetime.date.today().strftime("%Y-%m-%d")
            for b in brokers:
                conn.execute("INSERT OR REPLACE INTO broker_summary VALUES (?,?,?,?,?,?,?)",
                    (sym, today, b.get('broker_code'), float(b.get('bval') or 0), float(b.get('sval') or 0), float(b.get('nval') or 0), int(b.get('bvol') or 0), int(b.get('svol') or 0)))

            conn.execute("INSERT OR REPLACE INTO bandar_daily VALUES (?,?,?,?,?,?)",
                (sym, today, net_1w, fmt(net_1w), atype, total_b, total_s))
            conn.commit()
            print(f" {sym} {fmt(net_1w)} {atype} {len(brokers)} brokers")
            time.sleep(1.3) # anti-429

        except Exception as e:
            print(f" {sym} bandar err {e}")
            time.sleep(1.3)
    conn.close()

if __name__ == "__main__":
    init_db()
    update_ohlcv()
    update_bandar()
    print("✅ DONE V5 + BANDAR")
