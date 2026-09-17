"""
RAFANO V5 - GITHUB NATIVE - 200 CANDLE + BROKSUM 5 HARI
"""
import os, time, sqlite3, datetime, requests, json
import yfinance as yf

DB_PATH = "rafano.db"
ARJUM_BASE = "https://stock.arjum.com/api"
ARJUM_KEY = os.getenv("ARJUM_API_KEY") or ""

def get_all_idx_symbols():
    symbols=[]
    try:
        r=requests.get("https://raw.githubusercontent.com/irfnmdl/IDX-Stock/master/data/stock_codes.txt", timeout=10)
        if r.status_code==200:
            symbols=[x.strip().upper().replace(".JK","").replace("$","") for x in r.text.split("\n") if len(x.strip())>=3]
            print(f"Got {len(symbols)} from IDX txt")
    except Exception as e:
        print(f"fail fetch {e}")
    if len(symbols)<200:
        # fallback liquid 400
        symbols="BBCA,BBRI,BMRI,BBNI,TLKM,ASII,GOTO,BUKA,BREN,CUAN,ADRO,ANTM,INCO,MDKA,PTBA,PGAS,BBKP,BRIS,NCKL,AMRT,TOWR,ELSA,DEWA,BULL,WIFI,AMMN,BRMS,BIPI,BMTR,BBHI,AALI,ACES,ADMR,AKRA,BRPT,BSDE,CPIN,CTRA,EMTK,ICBP,INDF,INKP,INTP,ITMG,JPFA,KLBF,MEDC,SMGR,SMRA,TPIA,UNTR,UNVR,ADHI,ARCI,ARTO,ASRI,AUTO,BIRD,BKSL,BUMI,DOID,ERAA,ESSA,FILM,HRUM,INDY,JSMR,LPPF,LSIP,MAPI,MBMA,MIKA,MNCN,MTEL,SCMA,SIDO,SRTG,SSIA,WIKA".split(",")
    clean=[]
    for s in symbols:
        s=s.strip().upper().replace("$","").replace(".JK","")
        if len(s)<3 or len(s)>6: continue
        if "-W" in s or s.startswith("$"): continue
        clean.append(s)
    clean=sorted(list(set(clean)))
    print(f"TOTAL CLEAN: {len(clean)}")
    return clean

def init_db():
    conn=sqlite3.connect(DB_PATH)
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS ohlcv (symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY(symbol, date))")
    cur.execute("CREATE TABLE IF NOT EXISTS bandar_daily (symbol TEXT, date TEXT, foreign_net REAL, foreign_str TEXT, accum_type TEXT, PRIMARY KEY(symbol, date))")
    cur.execute("CREATE TABLE IF NOT EXISTS broker_summary (symbol TEXT, date TEXT, broker_code TEXT, bval REAL, sval REAL, nval REAL, PRIMARY KEY(symbol, date, broker_code))")
    conn.commit(); conn.close()

def update_ohlcv_200(symbols):
    conn=sqlite3.connect(DB_PATH)
    total=len(symbols)
    print(f"=== OHLCV 200 CANDLE {total} SAHAM ===")
    for i, sym in enumerate(symbols,1):
        try:
            ticker=yf.Ticker(f"{sym}.JK")
            hist=ticker.history(period="1y", auto_adjust=False, timeout=10)
            if hist is None or hist.empty or len(hist)<20:
                if i%100==0: print(f"[{i}/{total}] {sym} empty")
                continue
            hist=hist.tail(200)
            hist=hist.reset_index()
            for _, row in hist.iterrows():
                try:
                    d=row['Date'].strftime("%Y-%m-%d") if hasattr(row['Date'],'strftime') else str(row['Date'])[:10]
                    conn.execute("INSERT OR REPLACE INTO ohlcv VALUES (?,?,?,?,?,?,?)",(sym,d,float(row['Open']),float(row['High']),float(row['Low']),float(row['Close']),int(row['Volume'])))
                except: pass
            if i%50==0:
                conn.commit()
                print(f"OHLCV {i}/{total} {sym} OK")
        except Exception as e:
            print(f"{sym} err {e}")
        time.sleep(0.12)
    conn.commit(); conn.close()
    print("OHLCV DONE")

def update_broksum_5d(symbols):
    if not ARJUM_KEY:
        print("No ARJUM_API_KEY skip broksum")
        return
    conn=sqlite3.connect(DB_PATH)
    end=datetime.date.today()
    start=end-datetime.timedelta(days=10)
    total=len(symbols)
    for i, sym in enumerate(symbols,1):
        try:
            url=f"{ARJUM_BASE}/broker-summary/{sym}"
            params={"start_date":start.strftime("%Y-%m-%d"),"end_date":end.strftime("%Y-%m-%d"),"flow":"all","all_data":"true","broker_limit":15}
            r=requests.get(url, headers={"X-API-Key":ARJUM_KEY.strip()}, params=params, timeout=15)
            if r.status_code==429:
                print(f"429 {sym} sleep 40s"); time.sleep(40); continue
            if r.status_code!=200:
                time.sleep(0.8); continue
            data=r.json()
            daily=data.get('data',[]) or [data]
            if isinstance(daily, dict): daily=[daily]
            for day in daily[-5:]:
                d=day.get('date') or end.strftime("%Y-%m-%d")
                brokers=day.get('brokers',[]) or data.get('brokers',[])
                if not brokers: continue
                net=sum([float(b.get('nval',0) or 0) for b in brokers])
                fmt=f"{net/1e9:+.2f}B" if abs(net)>=1e9 else f"{net/1e6:+.0f}M"
                atype="BIG ACCUM" if net>5e9 else "ACCUM" if net>1e9 else "BIG DIST" if net<-5e9 else "DIST" if net<-1e9 else "NEUTRAL"
                conn.execute("INSERT OR REPLACE INTO bandar_daily VALUES (?,?,?,?)",(sym,d,net,fmt,atype))
            if i%50==0:
                conn.commit()
                print(f"BROKSUM {i}/{total} {sym}")
            time.sleep(1.0)
        except Exception as e:
            print(f"{sym} err {e}"); time.sleep(1.0)
    conn.commit(); conn.close()
    print("BROKSUM DONE")

if __name__=="__main__":
    print(f"RAFANO GITHUB MODE - {datetime.datetime.now()}")
    symbols=get_all_idx_symbols()
    init_db()
    update_ohlcv_200(symbols)
    update_broksum_5d(symbols)
    print("ALL DONE")
