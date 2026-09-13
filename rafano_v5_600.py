"""
RAFANO V5 600 FIX - ANTI STUCK
"""
import os, time, sqlite3, datetime, requests
import yfinance as yf
import pandas as pd

DB_PATH = "rafano.db"
ARJUM_BASE = "https://stock.arjum.com/api"
ARJUM_KEY = os.getenv("ARJUM_API_KEY") or ""

def get_symbols():
    raw = "BBCA,BBRI,BMRI,BBNI,TLKM,ASII,BMTR,BIPI,GOTO,BUKA,BBKP,BRIS,ANTM,INCO,MDKA,ADRO,PTBA,PGAS,EXCL,ISAT,BREN,CUAN,WIFI,DEWA,BULL,NCKL,AMRT,TOWR,TBIG,ELSA,BEKS,BNGA,AALI,ACES,ADMR,AKRA,AMMN,BRMS,BRPT,BSDE,CPIN,CTRA,EMTK,ICBP,INDF,INKP,INTP,ITMG,JPFA,KLBF,MEDC,SMGR,SMRA,TPIA,UNTR,UNVR,ADHI,ARCI,ARTO,ASRI,ASSA,AUTO,BBHI,BIRD,BKSL,BUMI,DOID,ENRG,ERAA,ESSA,FILM,GPRA,HRUM,INDY,JSMR,LPPF,LSIP,MAPI,MBMA,MIKA,MNCN,MTEL,PTMP,RAJA,SCMA,SIDO,SRTG,SSIA,WIKA,AALI,ABMM,BBTN,BFIN,HRTA,MBAP,PTRO,TINS".split(",")
    clean=[]
    for s in raw:
        s=s.strip().upper().replace("$","").replace(".JK","")
        if len(s)<3 or len(s)>5: continue
        if "-W" in s or s.startswith("$"): continue
        clean.append(s)
    return sorted(list(set(clean)))[:350]

SYMBOLS = get_symbols()
print(f"TOTAL CLEAN: {len(SYMBOLS)}")

def init_db():
    conn=sqlite3.connect(DB_PATH)
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS ohlcv (symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY(symbol, date))")
    cur.execute("CREATE TABLE IF NOT EXISTS bandar_daily (symbol TEXT, date TEXT, foreign_net_1w REAL, foreign_str_1w TEXT, accum_type TEXT, PRIMARY KEY(symbol, date))")
    cur.execute("CREATE TABLE IF NOT EXISTS broker_summary (symbol TEXT, date TEXT, broker_code TEXT, nval REAL, PRIMARY KEY(symbol, date, broker_code))")
    conn.commit(); conn.close()

def update_ohlcv():
    conn=sqlite3.connect(DB_PATH)
    total=len(SYMBOLS)
    for i, sym in enumerate(SYMBOLS,1):
        try:
            cnt=conn.execute("SELECT COUNT(*) FROM ohlcv WHERE symbol=?",(sym,)).fetchone()[0]
            if cnt>300:
                if i%50==0: print(f"[{i}/{total}] SKIP {sym}")
                continue
            try:
                ticker=yf.Ticker(f"{sym}.JK")
                hist=ticker.history(period="1y", auto_adjust=False, timeout=8)
            except Exception as e:
                print(f"[{i}/{total}] {sym} timeout {e}")
                continue
            if hist is None or hist.empty or len(hist)<10:
                print(f"[{i}/{total}] {sym} empty skip")
                continue
            hist=hist.reset_index()
            for _, row in hist.iterrows():
                try:
                    d=row['Date'].strftime("%Y-%m-%d") if hasattr(row['Date'],'strftime') else str(row['Date'])[:10]
                    conn.execute("INSERT OR REPLACE INTO ohlcv VALUES (?,?,?,?,?,?,?)",(sym,d,float(row['Open']),float(row['High']),float(row['Low']),float(row['Close']),int(row['Volume'])))
                except: pass
            if i%20==0:
                conn.commit()
                print(f"OHLCV {i}/{total} {sym} OK")
        except Exception as e:
            print(f"{sym} err {e}")
        time.sleep(0.2)
    conn.commit(); conn.close()
    print("OHLCV DONE")

def update_bandar():
    if not ARJUM_KEY:
        print("No ARJUM_KEY skip bandar")
        return
    conn=sqlite3.connect(DB_PATH)
    end=datetime.date.today()
    start=end-datetime.timedelta(days=10)
    total=len(SYMBOLS)
    for i, sym in enumerate(SYMBOLS,1):
        try:
            exists=conn.execute("SELECT 1 FROM bandar_daily WHERE symbol=? AND date=?",(sym,end.strftime("%Y-%m-%d"))).fetchone()
            if exists: continue
            url=f"{ARJUM_BASE}/broker-summary/{sym}"
            params={"start_date":start.strftime("%Y-%m-%d"),"end_date":end.strftime("%Y-%m-%d"),"flow":"all","all_data":"true","broker_limit":10}
            r=requests.get(url, headers={"X-API-Key":ARJUM_KEY.strip()}, params=params, timeout=12)
            if r.status_code==429:
                print(f"429 {sym} sleep 35s")
                time.sleep(35)
                continue
            if r.status_code!=200:
                time.sleep(1.2); continue
            data=r.json()
            brokers=data.get('brokers',[])
            if not brokers: time.sleep(1.2); continue
            net=sum([float(b.get('nval') or 0) for b in brokers])
            def fmt(v): return f"{v/1e9:+.2f}B" if abs(v)>=1e9 else f"{v/1e6:+.0f}M"
            atype="BIG ACCUM" if net>5e9 else "ACCUM" if net>1e9 else "BIG DIST" if net<-5e9 else "DIST" if net<-1e9 else "NEUTRAL"
            conn.execute("INSERT OR REPLACE INTO bandar_daily VALUES (?,?,?,?)",(sym,end.strftime("%Y-%m-%d"),net,fmt(net),atype))
            if i%15==0:
                conn.commit()
                print(f"BANDAR {i}/{total} {sym} {fmt(net)}")
            time.sleep(1.3)
        except Exception as e:
            print(f"{sym} err {e}")
            time.sleep(1.3)
    conn.commit(); conn.close()
    print("BANDAR DONE")

if __name__=="__main__":
    print(f"RAFANO V5 FIX {len(SYMBOLS)} CLEAN - {datetime.datetime.now()}")
    init_db()
    update_ohlcv()
    update_bandar()
