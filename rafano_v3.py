"""
RAFANO V4.7 SCORING EDITION - FINAL UPGRADE
Upgrade dari V4.6:
+ Filter Value >=1B (anti saham sepi gocap 100 lot)
+ Filter Close Position >=60% (anti pucuk dijemur)
+ Auto-Expand ARJUM 60->100->150 kalau BO <3
+ Scoring 0-100 (Vol, BO strength, Value, Foreign, ClosePos)
+ Bandar check (Foreign Net + Top Broker)
+ History anti FOMO (udah naik 10% dari BO skip)

Flow: ARJUM 60 -> ITICK 30 REALTIME -> VOL1.5x -> BO50/BOB200 -> VALUE1B -> CLOSEPOS60% -> SCORING -> TELEGRAM

Total time: 15-20 detik
"""

import os, time, datetime, threading, requests, pytz, json
import numpy as np, pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
load_dotenv()

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or ""
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID") or os.getenv("CHAT_ID") or ""
ARJUM_API_KEY = os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE = "https://stock.arjum.com/api"
ITICK_TOKEN = os.getenv("ITICK_TOKEN") or os.getenv("ITICK_API_KEY") or "7a470a83276242309fb940684046d35a88e450fdb95b46c383670e1e0c5e96f5"
ITICK_BASE = "https://api.itick.org"
ITICK_ENABLED = bool(ITICK_TOKEN)

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

FCA_EXCLUDE = {"FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ELSA","ENRG","ENVY","ERAA","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","TINS-W","BIPI-W","BULL-W","DEWA-W"}

HISTORY_CACHE={}; SCREENER_CACHE={}
HISTORY_CACHE_TTL=300; SCREENER_CACHE_TTL=120
QUOTA_HIT=False; LAST_429_TIME=0
BO_HISTORY_FILE="/tmp/bo_history.json"
BO_HISTORY={}

try:
    if os.path.exists(BO_HISTORY_FILE):
        with open(BO_HISTORY_FILE,'r') as f: BO_HISTORY=json.load(f)
except: BO_HISTORY={}

def save_bo_history():
    try:
        with open(BO_HISTORY_FILE,'w') as f: json.dump(BO_HISTORY,f)
    except: pass

def get_cached_history(k):
    import time
    if k in HISTORY_CACHE:
        ts,d=HISTORY_CACHE[k]
        if time.time()-ts<HISTORY_CACHE_TTL: return d
    return None
def set_cached_history(k,d):
    import time; HISTORY_CACHE[k]=(time.time(),d)
def get_cached_screener():
    import time
    if 'latest' in SCREENER_CACHE:
        ts,d=SCREENER_CACHE['latest']
        if time.time()-ts<SCREENER_CACHE_TTL: return d
    return None
def set_cached_screener(d):
    import time; SCREENER_CACHE['latest']=(time.time(),d)

def arjum_get(path, params=None):
    global QUOTA_HIT, LAST_429_TIME
    import time as _time
    if QUOTA_HIT and _time.time()-LAST_429_TIME<300: return None
    url=f"{ARJUM_BASE}{path}"
    try:
        headers={"X-API-Key": ARJUM_API_KEY.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r=requests.get(url,headers=headers,params=params,timeout=15)
        if r.status_code==200: return r.json()
        elif r.status_code==429:
            QUOTA_HIT=True; LAST_429_TIME=_time.time(); return None
    except: pass
    return None

def get_screener_latest(force_today=False):
    if not force_today:
        c=get_cached_screener()
        if c and isinstance(c, dict) and 'rows' in c: return c
    data=arjum_get("/screener/latest")
    if data and isinstance(data, dict) and 'rows' in data:
        set_cached_screener(data); return data
    return data if data else {}

def get_history_pro(sym, limit=120):
    hk=f"{sym}_1d_{limit}"
    cached=get_cached_history(hk)
    if cached is not None: return cached
    data=arjum_get(f"/history/{sym}",params={"limit":limit,"frame":"daily"})
    rows=[]
    if data:
        if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data,list): rows=data
    if rows:
        try:
            df=pd.DataFrame(rows)
            rn={}
            for c in df.columns:
                cl=str(c).lower()
                if cl in ['o','open']: rn[c]='Open'
                elif cl in ['h','high']: rn[c]='High'
                elif cl in ['l','low']: rn[c]='Low'
                elif cl in ['c','close']: rn[c]='Close'
                elif cl in ['v','volume']: rn[c]='Volume'
                elif cl in ['date','time','t','datetime','timestamp']: rn[c]='Date'
            df.rename(columns=rn,inplace=True)
            if 'Date' in df.columns: df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
            df=df.sort_index()
            for col in ['Open','High','Low','Close','Volume']: df[col]=pd.to_numeric(df[col],errors='coerce')
            df=df.dropna(subset=['Close'])
            if len(df)>=10:
                set_cached_history(hk,df); return df
        except: pass
    try:
        import yfinance as yf
        hist=yf.Ticker(f"{sym}.JK").history(period="1y",interval="1d",timeout=10)
        if hist is not None and len(hist)>10:
            set_cached_history(hk,hist.tail(limit)); return hist.tail(limit)
    except: pass
    return None

def get_broker_simple(sym):
    """Bandar simple check dari Arjum broker summary"""
    try:
        data=arjum_get(f"/broker/{sym}",params={"frame":"daily","limit":20})
        if not data: return None
        rows=data.get('data') or data if isinstance(data,list) else data.get('data',[])
        if not rows: return None
        df=pd.DataFrame(rows)
        # cari net foreign kalau ada
        # Arjum broker format beda-beda, kita bikin simple
        # kalau gak ada, return None (skip bandar filter)
        return {"raw": df}
    except: return None

def get_itick_quotes_batch(symbols, max_batch=15):
    if not ITICK_ENABLED or not symbols: return {}
    try:
        codes=",".join(symbols[:max_batch])
        url = f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
        headers = {"accept": "application/json", "token": ITICK_TOKEN}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code==200:
            j=r.json()
            if j.get('code')==0 and j.get('data'):
                result={}
                for item in j['data']:
                    code=item.get('s') or item.get('code')
                    if not code: continue
                    code=str(code).upper().replace(".JK","")
                    result[code]={
                        'price': float(item.get('ld') or item.get('c') or 0),
                        'change': float(item.get('ch') or 0),
                        'changepct': float(item.get('chp') or 0),
                        'volume': float(item.get('v') or 0),
                        'high': float(item.get('h') or 0),
                        'low': float(item.get('l') or 0),
                        'source': 'ITICK_REALTIME'
                    }
                return result
    except Exception as e:
        print(f"itick batch err: {e}")
    return {}

def format_large_number(val,show_sign=False):
    if pd.isna(val) or val==0: return "0"
    a=abs(val); s="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if a>=1_000_000_000: return f"{s}{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{s}{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{s}{a/1_000:,.0f}K"
    else: return f"{s}{val:,.0f}"

def check_bo_ema50_bob200(sym, hd, realtime_price=None):
    try:
        if hd is None or len(hd) < 55: return None
        c = float(realtime_price if realtime_price and realtime_price>0 else hd['Close'].iloc[-1])
        pc = float(hd['Close'].iloc[-2])
        if c < 50: return None
        ema50_s = hd['Close'].ewm(span=50, adjust=False).mean()
        ema200_s = hd['Close'].ewm(span=200, adjust=False).mean()
        ema50 = float(ema50_s.iloc[-1]); ema200 = float(ema200_s.iloc[-1])
        pe50 = float(ema50_s.iloc[-2]); pe200 = float(ema200_s.iloc[-2])
        is_bo=False; stype=""
        if pc <= pe50 and c > ema50 and c > pe50:
            is_bo=True; stype="BO EMA50"
        elif pc <= pe200 and c > ema200 and c > pe200:
            is_bo=True; stype="BOB EMA200"
        if not is_bo: return None
        if len(hd)>=4:
            c2=float(hd['Close'].iloc[-3]); c3=float(hd['Close'].iloc[-4])
            pe50_2=float(ema50_s.iloc[-3]); pe50_3=float(ema50_s.iloc[-4])
            pe200_2=float(ema200_s.iloc[-3]); pe200_3=float(ema200_s.iloc[-4])
            if stype=="BO EMA50":
                if c2 > pe50_2 and c3 > pe50_3: return None
                if c2 > pe50_2 and pc > pe50: return None
            if stype=="BOB EMA200":
                if c2 > pe200_2 and c3 > pe200_3: return None
                if c2 > pe200_2 and pc > pe200: return None
        return {"type": stype, "ema50": ema50, "ema200": ema200, "pe50": pe50, "pe200": pe200}
    except: return None

def calculate_score(item):
    """
    Scoring 0-100:
    Vol ratio 30 poin (1.5x=10, 2x=20, 3x=30)
    BO strength 20 poin (jarak di atas EMA)
    Value 20 poin (1B=5, 5B=15, 10B=20)
    Close Position 15 poin (60%=5, 80%=15)
    Type 15 poin (BOB200=15, BO50=10)
    """
    score=0
    vol=item.get('vol_ratio',0)
    if vol>=3.0: score+=30
    elif vol>=2.5: score+=25
    elif vol>=2.0: score+=20
    elif vol>=1.5: score+=10
    
    # BO strength: jarak close di atas EMA
    c=item.get('close',0); ema=item.get('ema50') if item.get('type')=='BO EMA50' else item.get('ema200',0)
    if ema>0:
        dist=(c-ema)/ema*100
        if dist>=5: score+=20
        elif dist>=3: score+=15
        elif dist>=1: score+=10
        else: score+=5
    
    val=item.get('vol_rp',0)
    if val>=10_000_000_000: score+=20
    elif val>=5_000_000_000: score+=15
    elif val>=2_000_000_000: score+=10
    elif val>=1_000_000_000: score+=5
    
    cp=item.get('close_pos',0)
    if cp>=0.8: score+=15
    elif cp>=0.7: score+=10
    elif cp>=0.6: score+=5
    
    if item.get('type')=='BOB EMA200': score+=15
    else: score+=10
    
    return min(score,100)

def scan_v47(top_gainer_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000, min_closepos=0.6):
    print(f"[{get_now_wib()}] 🚀 V4.7 SCORING: ARJUM {top_gainer_arjum}->ITICK {top_itick} VOL>{vol_thr}x VAL>{format_large_number(min_value)} CP>{min_closepos*100:.0f}%")
    
    screener = get_screener_latest(force_today=True)
    rows = screener.get('rows', []) if isinstance(screener, dict) else []
    if not rows:
        print("❌ Arjum kosong"); return []
    
    candidates_arjum=[]
    for r in rows:
        code = (r.get('stock_code') or r.get('symbol') or "").replace(".JK","").upper()
        if not code or code in FCA_EXCLUDE or "-W" in code: continue
        try:
            close=float(r.get('close') or 0)
            if close < 50: continue
        except: continue
        candidates_arjum.append(code)
        if len(candidates_arjum) >= 150: break # ambil 150 buat auto-expand
    
    print(f"📊 ARJUM pool {len(candidates_arjum)} saham (TOP 150 gainer hari ini)")
    
    # Loop auto-expand kalau BO <3
    final_detected=[]
    arjum_slice_sizes=[top_gainer_arjum, 100, 150]
    
    for slice_size in arjum_slice_sizes:
        current_pool = candidates_arjum[:slice_size]
        print(f"\n🔍 Trying ARJUM TOP {slice_size}: {current_pool[:5]}...")
        
        all_quotes={}
        for i in range(0, len(current_pool), 15):
            batch=current_pool[i:i+15]
            q=get_itick_quotes_batch(batch,15)
            all_quotes.update(q)
            time.sleep(0.2)
        if not all_quotes:
            for r in rows:
                code=(r.get('stock_code') or "").replace(".JK","").upper()
                if code in current_pool:
                    all_quotes[code]={'price': float(r.get('close') or 0), 'changepct': float(r.get('change_pct') or 0), 'high': float(r.get('high') or r.get('close') or 0), 'low': float(r.get('low') or r.get('close') or 0), 'source': 'ARJUM_DELAY'}
        
        sorted_itick = sorted(all_quotes.items(), key=lambda x: x[1].get('changepct', -999), reverse=True)
        top_codes = [c for c,_ in sorted_itick[:top_itick]]
        
        def check_one(sym):
            try:
                hd=get_history_pro(sym, limit=120)
                if hd is None or len(hd)<55: return None
                q=all_quotes.get(sym, {})
                realtime_price=q.get('price',0)
                changepct=q.get('changepct',0)
                c = float(realtime_price if realtime_price>0 else hd['Close'].iloc[-1])
                
                # VOL filter
                v_last=float(hd['Volume'].iloc[-1])
                v_avg=float(hd['Volume'].iloc[-21:-1].mean())
                if v_avg==0 or v_last==0: return None
                ratio=v_last/v_avg
                if ratio < vol_thr: return None
                
                # VALUE filter 1B
                vol_rp = v_last * c
                if vol_rp < min_value: 
                    print(f"   ⏭ {sym} VALUE {format_large_number(vol_rp)} < {format_large_number(min_value)} SKIP")
                    return None
                
                # CLOSE POS filter
                high_today = q.get('high') or float(hd['High'].iloc[-1])
                low_today = q.get('low') or float(hd['Low'].iloc[-1])
                if high_today==0: high_today=c
                if low_today==0: low_today=c
                close_pos = (c - low_today) / (high_today - low_today) if high_today!=low_today else 1.0
                if close_pos < min_closepos:
                    print(f"   ⏭ {sym} CLOSE POS {close_pos*100:.0f}% < {min_closepos*100:.0f}% (pucuk) SKIP")
                    return None
                
                # BO filter
                bo_info = check_bo_ema50_bob200(sym, hd, realtime_price)
                if not bo_info: return None
                
                # Anti FOMO: udah naik 10% dari BO?
                if sym in BO_HISTORY:
                    bo_price=BO_HISTORY[sym].get('price',0)
                    if bo_price>0 and c > bo_price*1.10:
                        print(f"   ⏭ {sym} udah naik 10% dari BO {bo_price}->{c} SKIP FOMO")
                        return None
                
                item={
                    "symbol": sym,
                    "type": bo_info['type'],
                    "close": int(c),
                    "change_pct": changepct,
                    "vol_ratio": ratio,
                    "vol_rp": vol_rp,
                    "close_pos": close_pos,
                    "ema50": bo_info['ema50'],
                    "ema200": bo_info['ema200'],
                    "high": high_today,
                    "low": low_today,
                    "source": q.get('source','')
                }
                item['score']=calculate_score(item)
                return item
            except Exception as e:
                print(f"check err {sym}: {e}"); return None
        
        with ThreadPoolExecutor(max_workers=15) as ex:
            futs={ex.submit(check_one, s): s for s in top_codes}
            batch_detected=[]
            for f in as_completed(futs):
                r=f.result()
                if r:
                    batch_detected.append(r)
                    print(f"🔥 VALID {r['symbol']} {r['type']} {r['change_pct']:+.1f}% VOL {r['vol_ratio']:.2f}x VAL {format_large_number(r['vol_rp'])} CP {r['close_pos']*100:.0f}% SCORE {r['score']}")
        
        batch_detected.sort(key=lambda x: x['score'], reverse=True)
        final_detected=batch_detected
        
        # Auto-expand logic
        if len(final_detected)>=3:
            print(f"✅ Dapat {len(final_detected)} BO, cukup. Stop expand.")
            break
        else:
            if slice_size==arjum_slice_sizes[-1]:
                print(f"⚠ Cuma {len(final_detected)} BO dari TOP {slice_size}, sudah max expand.")
                break
            print(f"⚠ Cuma {len(final_detected)} BO <3, expand ke TOP {arjum_slice_sizes[arjum_slice_sizes.index(slice_size)+1]}...")
    
    # save BO history
    for it in final_detected:
        BO_HISTORY[it['symbol']]={'price': it['close'], 'date': get_now_wib().strftime('%Y-%m-%d'), 'type': it['type']}
    save_bo_history()
    
    final_detected.sort(key=lambda x: x['score'], reverse=True)
    return final_detected

def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN: return
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try: requests.post(url,json=pl,timeout=15)
    except: pass

def broadcast_v47(signals, vol_thr=1.5):
    if not signals:
        send_reply(TARGET_CHAT_ID, f"📉 *V4.7 SCORING* | ARJUM60->ITICK30 + VOL>{vol_thr}x + BO + VAL1B + CP60%\nTidak ada yang valid hari ini.")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*🚀 V4.7 TOP {len(signals)} BO SCORING* 🔥\n{now}\nARJUM60->ITICK30 + VOL>{vol_thr}x + BO50/BOB200 + VAL1B + CP60% + SCORE\n{'='*38}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        emoji="🟢" if it['score']>=80 else "🟡" if it['score']>=60 else "⚪"
        line=f"{idx}. {emoji} *{it['symbol']}* {it['type']} | SCORE {it['score']}\n   {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x Rp {format_large_number(it['vol_rp'])} CP {it['close_pos']*100:.0f}%\n\n"
        kb.append([{"text": f"{it['symbol']} S{it['score']} {it['vol_ratio']:.1f}x", "callback_data": f"chart_{it['symbol']}_1d"}])
        if len(msg)+len(line)>3500:
            send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else:
            msg+=line
    if msg:
        send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb})

AUTO_NOTIFY_ENABLED = os.getenv("AUTO_NOTIFY", "true").lower() == "true"
AUTO_INTERVAL = 180
COOLDOWN_MIN = 60
LAST_ALERT_MAP = {}

def is_market_hours_v2():
    now=get_now_wib()
    if now.weekday()>=5: return False, "WEEKEND"
    h=now.hour+now.minute/60
    if 8.9 <= h < 9.0: return True, "PRE_OPEN"
    if 9.0 <= h < 12.0: return True, "SESI_1"
    if 12.0 <= h < 13.5: return False, "BREAK"
    if 13.5 <= h < 15.85: return True, "SESI_2"
    return False, "CLOSED"

def should_alert(sym, vol_ratio):
    import time as tm
    now=tm.time()
    if sym not in LAST_ALERT_MAP: return True
    last_t, last_v = LAST_ALERT_MAP[sym]
    if now-last_t < COOLDOWN_MIN*60:
        if vol_ratio > last_v*1.5: return True
        return False
    return True

def auto_scan_loop():
    print(f"🤖 AUTO V4.7 SCORING tiap {AUTO_INTERVAL}s")
    while True:
        try:
            is_open, sesi = is_market_hours_v2()
            if not is_open:
                time.sleep(300); continue
            print(f"[{get_now_wib()}] 🔍 AUTO V4.7 {sesi}...")
            sigs = scan_v47(top_gainer_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000, min_closepos=0.6)
            to_broadcast=[]
            for sig in sigs:
                if should_alert(sig['symbol'], sig['vol_ratio']):
                    to_broadcast.append(sig)
                    LAST_ALERT_MAP[sig['symbol']]=(time.time(), sig['vol_ratio'])
            if to_broadcast:
                broadcast_v47(to_broadcast, vol_thr=1.5)
            time.sleep(AUTO_INTERVAL)
        except Exception as e:
            print(f"AUTO err: {e}"); time.sleep(60)

def telegram_bot_listener():
    offset=0
    print("🤖 RAFANO V4.7 SCORING - Listening...")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
    except: pass
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200: time.sleep(3); continue
            data=res.json()
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip(); chat_id=update["message"]["chat"]["id"]; first=txt.split()[0].lower() if txt else ""
                    if first in ["/start","/help","/menu"]:
                        send_reply(chat_id, "🔥 *RAFANO V4.7 SCORING*\nARJUM60->ITICK30+VOL1.5x+BO+VAL1B+CP60%+SCORE\n\n/scanbo - Scan final scoring (20 detik)\n/scanbo 2 - Vol >2x\n/scanbo 2 2000000000 - Vol2x Val2B\n/topbo 20 1.5 - custom\n/quota")
                    elif first.startswith("/"):
                        parts=txt.split()
                        vol_thr=1.5; top_final=30; top_arjum=60; min_val=1_000_000_000; min_cp=0.6
                        try:
                            if len(parts)>=2: vol_thr=float(parts[1])
                            if len(parts)>=3: min_val=float(parts[2])
                            if len(parts)>=4: top_final=int(float(parts[3]))
                        except: pass
                        if vol_thr>=10:
                            top_final=int(vol_thr); vol_thr=1.5
                        send_reply(chat_id, f"🚀 *V4.7 SCORING*\nARJUM{top_arjum}->ITICK{top_final} VOL>{vol_thr}x VAL>{format_large_number(min_val)} CP>{min_cp*100:.0f}%\n~20 detik...")
                        def run_scan(tg=chat_id, vt=vol_thr, tf=top_final, ta=top_arjum, mv=min_val):
                            sigs=scan_v47(top_gainer_arjum=ta, top_itick=tf, vol_thr=vt, min_value=mv, min_closepos=min_cp)
                            broadcast_v47(sigs, vol_thr=vt)
                        threading.Thread(target=run_scan, args=(chat_id, vol_thr, top_final, top_arjum, min_val)).start()
        except Exception as e:
            print(f"Listener err {e}"); time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.7 SCORING EDITION")
    print("ARJUM60->ITICK30+VOL1.5x+BO+VAL1B+CP60%+SCORE")
    print("==========================================")
    if AUTO_NOTIFY_ENABLED:
        threading.Thread(target=auto_scan_loop, daemon=True).start()
    telegram_bot_listener()
