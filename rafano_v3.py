"""
RAFANO V4.8 BANDAR INFO EDITION
Upgrade V4.7 -> V4.8:
- Bandar (Foreign Buy + Broker Akum) jadi PENAMBAH SCORE, bukan filter wajib
- Default: score +15 FB, +15 Akum, total max 130 (tapi cap 100)
- Info tambahan di alert: [FB +2.5B AKUM 62%] / [FS -1B DIST]
- Gak pernah 0 hasil gara-gara bandar

Flow FINAL:
ARJUM 60 -> ITICK 30 REALTIME -> VOL1.5x -> BO50/BOB200 -> VAL1B -> CP60% -> BANDAR INFO + SCORE -> TELEGRAM
"""

import os, time, datetime, threading, requests, pytz, json
import numpy as np, pandas as pd
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

HISTORY_CACHE={}; SCREENER_CACHE={}; BROKER_CACHE={}
HISTORY_CACHE_TTL=300; SCREENER_CACHE_TTL=120; BROKER_CACHE_TTL=300
QUOTA_HIT=False; LAST_429_TIME=0

def get_cached(k, cache, ttl):
    import time
    if k in cache:
        ts,d=cache[k]
        if time.time()-ts<ttl: return d
    return None
def set_cached(k,d,cache):
    import time; cache[k]=(time.time(),d)

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
    except Exception as e:
        print(f"Arjum err {path}: {e}")
    return None

def get_screener_latest(force_today=False):
    if not force_today:
        c=get_cached('latest', SCREENER_CACHE, SCREENER_CACHE_TTL)
        if c and isinstance(c, dict) and 'rows' in c: return c
    data=arjum_get("/screener/latest")
    if data and isinstance(data, dict) and 'rows' in data:
        set_cached('latest', data, SCREENER_CACHE); return data
    return data if data else {}

def get_history_pro(sym, limit=120):
    hk=f"{sym}_1d_{limit}"
    cached=get_cached(hk, HISTORY_CACHE, HISTORY_CACHE_TTL)
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
                set_cached(hk,df,HISTORY_CACHE); return df
        except: pass
    try:
        import yfinance as yf
        hist=yf.Ticker(f"{sym}.JK").history(period="1y",interval="1d",timeout=10)
        if hist is not None and len(hist)>10:
            set_cached(hk,hist.tail(limit),HISTORY_CACHE); return hist.tail(limit)
    except: pass
    return None

def get_bandar_info(sym):
    """
    Bandar Info - BEST EFFORT dari Arjum
    Coba beberapa endpoint Arjum yang ada:
    1. /foreign/{sym} -> foreign flow
    2. /broker/{sym} -> broker summary
    Kalau gagal, return None (gak bikin fail)
    """
    cache_key=f"bandar_{sym}"
    cached=get_cached(cache_key, BROKER_CACHE, BROKER_CACHE_TTL)
    if cached is not None: return cached

    foreign_net=0
    akum_ratio=0
    foreign_str="N/A"
    bandar_str="N/A"
    is_fb=False
    is_akum=False

    # Coba endpoint foreign
    try:
        # Endpoint ini ada di beberapa versi Arjum
        fdata=arjum_get(f"/foreign/{sym}", params={"limit": 5, "frame": "daily"})
        if fdata:
            # format bisa dict atau list
            rows=fdata.get('data') if isinstance(fdata, dict) else fdata
            if isinstance(rows, list) and len(rows)>0:
                last=rows[0] if isinstance(rows[0], dict) else {}
                # cari field foreign net
                for k in ['foreign_net','net_foreign','foreign_flow','net_buy','net']:
                    if k in last:
                        foreign_net=float(last[k] or 0)
                        break
                # kadang ada buy/sell terpisah
                if foreign_net==0:
                    buy=float(last.get('foreign_buy') or last.get('buy_foreign') or 0)
                    sell=float(last.get('foreign_sell') or last.get('sell_foreign') or 0)
                    if buy or sell:
                        foreign_net=buy-sell
            elif isinstance(rows, dict):
                foreign_net=float(rows.get('foreign_net') or rows.get('net_foreign') or 0)

        if foreign_net!=0:
            is_fb = foreign_net > 0
            if abs(foreign_net)>=1_000_000_000:
                foreign_str=f"{'FB' if is_fb else 'FS'} {foreign_net/1_000_000_000:+.1f}B"
            elif abs(foreign_net)>=1_000_000:
                foreign_str=f"{'FB' if is_fb else 'FS'} {foreign_net/1_000_000:+.0f}M"
            else:
                foreign_str=f"{'FB' if is_fb else 'FS'} {foreign_net:+.0f}"
        else:
            # fallback dari screener kalau ada
            screener=get_screener_latest()
            rows=screener.get('rows',[]) if isinstance(screener,dict) else []
            for r in rows:
                code=(r.get('stock_code') or "").replace(".JK","").upper()
                if code==sym:
                    # kadang screener ada foreign flow
                    fn=float(r.get('foreign_net') or r.get('net_foreign') or 0)
                    if fn!=0:
                        foreign_net=fn
                        is_fb=fn>0
                        foreign_str=f"{'FB' if is_fb else 'FS'} {fn/1_000_000_000:+.1f}B"
                    break
    except Exception as e:
        print(f"bandar foreign err {sym}: {e}")

    # Coba endpoint broker akum (simplified)
    try:
        bdata=arjum_get(f"/broker/{sym}", params={"frame":"daily","limit":10})
        if bdata:
            # Arjum broker summary format agak random, kita coba parse
            # yang penting dapat akum ratio top 3 broker
            rows=bdata.get('data') if isinstance(bdata, dict) else bdata
            if isinstance(rows, list) and len(rows)>0:
                # kalau ada field akum/dist
                # kita coba hitung dari buy vs sell broker top
                total_buy=0; total_sell=0
                for br in rows[:5]:
                    if isinstance(br, dict):
                        total_buy+=float(br.get('buy') or br.get('buy_volume') or br.get('b') or 0)
                        total_sell+=float(br.get('sell') or br.get('sell_volume') or br.get('s') or 0)
                if total_buy+total_sell>0:
                    akum_ratio=total_buy/(total_buy+total_sell)*100 if (total_buy+total_sell)>0 else 50
                    is_akum=akum_ratio>55
                    bandar_str=f"{'AKUM' if is_akum else 'DIST'} {akum_ratio:.0f}%"
    except Exception as e:
        print(f"bandar broker err {sym}: {e}")

    result={
        "foreign_net": foreign_net,
        "is_foreign_buy": is_fb,
        "akum_ratio": akum_ratio,
        "is_akum": is_akum,
        "foreign_str": foreign_str,
        "bandar_str": bandar_str,
        "score_bonus": (15 if is_fb else 0) + (15 if is_akum else 0)
    }
    set_cached(cache_key, result, BROKER_CACHE)
    return result

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
                        'changepct': float(item.get('chp') or 0),
                        'high': float(item.get('h') or 0),
                        'low': float(item.get('l') or 0),
                        'source': 'ITICK_REALTIME'
                    }
                return result
    except: pass
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

def calculate_score_v48(item):
    """
    Scoring V4.8:
    Base (70 poin):
    - Vol 30, BO strength 20, Value 20 (total 70)
    ClosePos 15, Type 15 = 30, jadi base max 100
    Bandar bonus +30 (FB 15 + AKUM 15) = max 130 tapi cap 100
    Tapi kita tampilkan base + bonus biar tau mana yang ada bandar
    """
    score=0
    vol=item.get('vol_ratio',0)
    if vol>=3.0: score+=30
    elif vol>=2.5: score+=25
    elif vol>=2.0: score+=20
    elif vol>=1.5: score+=10
    
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
    
    base_score=min(score,100)
    
    # Bandar bonus
    bandar_bonus=item.get('bandar_bonus',0)
    total_score=min(base_score+bandar_bonus,100) # cap 100 biar rapi
    
    return base_score, bandar_bonus, total_score

def scan_v48(top_gainer_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000, min_closepos=0.6):
    print(f"[{get_now_wib()}] 🚀 V4.8 BANDAR INFO: ARJUM {top_gainer_arjum}->ITICK {top_itick} VOL>{vol_thr}x + BO + BANDAR SCORE")
    
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
        if len(candidates_arjum) >= 150: break
    
    final_detected=[]
    for slice_size in [top_gainer_arjum, 100, 150]:
        current_pool = candidates_arjum[:slice_size]
        print(f"\n🔍 ARJUM TOP {slice_size}...")
        
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
                c = float(realtime_price if realtime_price>0 else hd['Close'].iloc[-1])
                
                v_last=float(hd['Volume'].iloc[-1]); v_avg=float(hd['Volume'].iloc[-21:-1].mean())
                if v_avg==0 or v_last==0: return None
                ratio=v_last/v_avg
                if ratio < vol_thr: return None
                vol_rp = v_last * c
                if vol_rp < min_value: return None
                
                high_today = q.get('high') or float(hd['High'].iloc[-1])
                low_today = q.get('low') or float(hd['Low'].iloc[-1])
                close_pos = (c - low_today) / (high_today - low_today) if high_today!=low_today else 1.0
                if close_pos < min_closepos: return None
                
                bo_info = check_bo_ema50_bob200(sym, hd, realtime_price)
                if not bo_info: return None
                
                # BANDAR INFO - PENAMBAH SCORE
                bandar = get_bandar_info(sym)
                
                item={
                    "symbol": sym,
                    "type": bo_info['type'],
                    "close": int(c),
                    "change_pct": q.get('changepct',0),
                    "vol_ratio": ratio,
                    "vol_rp": vol_rp,
                    "close_pos": close_pos,
                    "ema50": bo_info['ema50'],
                    "ema200": bo_info['ema200'],
                    "bandar": bandar,
                    "bandar_bonus": bandar.get('score_bonus',0),
                    "foreign_str": bandar.get('foreign_str',''),
                    "bandar_str": bandar.get('bandar_str',''),
                    "is_fb": bandar.get('is_foreign_buy',False),
                    "is_akum": bandar.get('is_akum',False),
                    "source": q.get('source','')
                }
                base, bonus, total = calculate_score_v48(item)
                item['base_score']=base
                item['bandar_bonus']=bonus
                item['score']=total
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
                    fb_icon="🟩" if r['is_fb'] else "🟥" if r['foreign_str']!="N/A" else "⬜"
                    ak_icon="📈" if r['is_akum'] else "📉" if r['bandar_str']!="N/A" else "⬜"
                    print(f"🔥 {r['symbol']} {r['type']} SCORE {r['score']} (Base {r['base_score']}+Bandar {r['bandar_bonus']}) {r['foreign_str']} {r['bandar_str']} {fb_icon}{ak_icon}")
        
        batch_detected.sort(key=lambda x: x['score'], reverse=True)
        final_detected=batch_detected
        if len(final_detected)>=3: break
    
    final_detected.sort(key=lambda x: x['score'], reverse=True)
    return final_detected

def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN: return
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try: requests.post(url,json=pl,timeout=15)
    except: pass

def broadcast_v48(signals, vol_thr=1.5):
    if not signals:
        send_reply(TARGET_CHAT_ID, f"📉 *V4.8 BANDAR INFO* | VOL>{vol_thr}x + BO + VAL1B + CP60%\nTidak ada BO valid hari ini.")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*🚀 V4.8 TOP {len(signals)} BO + BANDAR INFO* 🔥\n{now}\nARJUM60->ITICK30 VOL>{vol_thr}x BO+VAL1B+CP60%+BANDAR\n{'='*40}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        base=it.get('base_score',0); bonus=it.get('bandar_bonus',0); tot=it.get('score',0)
        if tot>=85: emoji="🟢"
        elif tot>=70: emoji="🟡"
        elif tot>=55: emoji="⚪"
        else: emoji="🔵"
        
        fb_str=it.get('foreign_str','')
        bd_str=it.get('bandar_str','')
        bandar_line=""
        if fb_str!="N/A" or bd_str!="N/A":
            bandar_line=f"   Bandar: {fb_str} {bd_str} (+{bonus})\n"
        
        line=f"{idx}. {emoji} *{it['symbol']}* {it['type']} | SCORE {tot} (Base {base}+Bandar {bonus})\n   {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x Rp {format_large_number(it['vol_rp'])} CP {it['close_pos']*100:.0f}%\n{bandar_line}\n"
        kb.append([{"text": f"{it['symbol']} S{tot} {it['foreign_str']}", "callback_data": f"chart_{it['symbol']}_1d"}])
        if len(msg)+len(line)>3500:
            send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else:
            msg+=line
    if msg:
        send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb})

def telegram_bot_listener():
    offset=0
    print("🤖 RAFANO V4.8 BANDAR INFO - Listening...")
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
                        send_reply(chat_id, "🔥 *RAFANO V4.8 BANDAR INFO*\nBandar jadi score + info, bukan filter wajib\n\n/scanbo - Scan final + bandar info\n/scanbo 2 - Vol>2x\n/scanbo 2 2000000000 - Vol2x Val2B\n/quota")
                    elif first.startswith("/"):
                        parts=txt.split(); vol_thr=1.5; top_final=30; top_arjum=60; min_val=1_000_000_000
                        try:
                            if len(parts)>=2: vol_thr=float(parts[1])
                            if len(parts)>=3: min_val=float(parts[2])
                        except: pass
                        if vol_thr>=10: top_final=int(vol_thr); vol_thr=1.5
                        send_reply(chat_id, f"🚀 *V4.8 SCAN BO + BANDAR INFO*\nARJUM{top_arjum}->ITICK{top_final} VOL>{vol_thr}x VAL>{format_large_number(min_val)}\n~20 detik (include bandar check)...")
                        def run_scan(tg=chat_id, vt=vol_thr, tf=top_final, ta=top_arjum, mv=min_val):
                            sigs=scan_v48(top_gainer_arjum=ta, top_itick=tf, vol_thr=vt, min_value=mv, min_closepos=0.6)
                            broadcast_v48(sigs, vol_thr=vt)
                        threading.Thread(target=run_scan, args=(chat_id, vol_thr, top_final, top_arjum, min_val)).start()
        except Exception as e:
            print(f"Listener err {e}"); time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.8 BANDAR INFO EDITION")
    print("BANDAR = SCORE + INFO, BUKAN FILTER WAJIB")
    print("==========================================")
    telegram_bot_listener()
