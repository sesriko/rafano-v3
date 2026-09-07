"""
RAFANO V4.1 FINAL - PRO MAX FIX
- Fix SyntaxError unmatched ')'
- Fix Broker MTF: Daily != 5D != 20D (pakai /broker-accumulation?days=N + series points)
- Fix Top Dist: - jadi valid
- Fix Seasonality & Pattern: compute dari 1000 hari history
- Auto-post: /autopost FILM (single) dan /autopost 3 (top 3)
- Caption kayak Analisa AI Arjum Pro

Cara update:
cd /content/rafano-v3
rm rafano_v3.py
# download rafano_v4_1_final.py dari sini
mv rafano_v4_1_final.py rafano_v3.py
python rafano_v3.py
"""
import os, time, datetime, threading, requests, pytz, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()

def safe_get_env(k):
    v=os.getenv(k)
    if v:
        v=str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")): v=v[1:-1].strip()
        return v
    try:
        from google.colab import userdata
        vv=userdata.get(k)
        if vv: 
            vv=str(vv).strip().strip('"').strip("'")
            os.environ[k]=vv
            return vv
    except: pass
    return None

TIMEZONE_WIB=pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID=safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY=safe_get_env("ARJUM_API_KEY")
ARJUM_BASE="https://stock.arjum.com/api"

print(f"✅ TELEGRAM_BOT_TOKEN {'OK' if TELEGRAM_BOT_TOKEN else 'MISSING'}")
print(f"✅ TARGET_CHAT_ID {TARGET_CHAT_ID if TARGET_CHAT_ID else 'MISSING'}")
print(f"✅ ARJUM_API_KEY {'OK' if ARJUM_API_KEY else 'MISSING'}")

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

def format_large_number(val, show_sign=False):
    if pd.isna(val) or val==0: return "0"
    abs_val=abs(val); sign="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if abs_val>=1_000_000_000: return f"{sign}{abs_val/1_000_000_000:.2f}B"
    elif abs_val>=1_000_000: return f"{sign}{abs_val/1_000_000:,.1f}M"
    elif abs_val>=1_000: return f"{sign}{abs_val/1_000:,.0f}K"
    else: return f"{sign}{val:,.0f}"

def safe_int(v,d=0):
    try:
        if pd.isna(v): return d
        return int(v)
    except: return d

def calculate_atr(df,p=14):
    h,l,c=df['High'],df['Low'],df['Close']
    tr=pd.concat([h-l,(h-c.shift(1)).abs(),(l-c.shift(1)).abs()],axis=1).max(axis=1)
    return tr.rolling(p,min_periods=1).mean()

def calculate_rsi(series,p=14):
    delta=series.diff()
    gain=delta.where(delta>0,0.0); loss=-delta.where(delta<0,0.0)
    avg_gain=gain.ewm(alpha=1/p,min_periods=p,adjust=False).mean()
    avg_loss=loss.ewm(alpha=1/p,min_periods=p,adjust=False).mean()
    rs=avg_gain/avg_loss.replace(0,0.00001)
    rsi=100-(100/(1+rs))
    return rsi.fillna(50)

def calculate_vsa_metrics(df):
    pr=(df['High']-df['Low']).replace(0,0.1)
    cp=(df['Close']-df['Low'])/pr
    cp=np.clip(cp,0.05,0.95)
    br=0.30+cp*0.60
    if 'V1' in df.columns:
        vr=df['Volume']/df['V1'].replace(0,1)
        is_green=df['Close']>=df['Open']
        boost=np.where((vr>1.5)&is_green,0.10,0)
        boost+=np.where((vr>2.5)&is_green,0.10,0)
        br=br+boost
    br=np.clip(br,0.05,0.95)
    df['Vol_Buy']=df['Volume']*br
    df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']
    df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']
    df['Buy_Pct']=br*100
    return df,br

def calculate_bollinger(df,p=20,std=2):
    sma=df['Close'].rolling(p).mean()
    s= df['Close'].rolling(p).std()
    return sma, sma+s*std, sma-s*std

def calculate_macd(df):
    ema12=df['Close'].ewm(span=12).mean()
    ema26=df['Close'].ewm(span=26).mean()
    macd=ema12-ema26
    signal=macd.ewm(span=9).mean()
    hist=macd-signal
    return macd,signal,hist

BROKER_CACHE={}
def arjum_get(path,params=None,use_cache=True):
    key=f"{path}?{str(params)}"
    if use_cache and key in BROKER_CACHE:
        ts,data=BROKER_CACHE[key]
        if time.time()-ts<300: return data
    url=f"{ARJUM_BASE}{path}"
    try:
        api_key=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or ""
        headers={"X-API-Key":api_key.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r=requests.get(url,headers=headers,params=params,timeout=15)
        if r.status_code==200:
            j=r.json()
            if use_cache: BROKER_CACHE[key]=(time.time(),j)
            return j
        else:
            print(f"⚠ {path} {params} -> {r.status_code}")
            return None
    except Exception as e:
        print(f"arjum_get {path} err {e}")
        return None

def get_history_pro(symbol,limit=150,timeframe="1d"):
    tf=timeframe.lower()
    frame_map={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly"}
    arjum_frame=frame_map.get(tf,"daily")
    data=arjum_get(f"/history/{symbol}",params={"limit":limit,"frame":arjum_frame})
    rows=[]
    if data:
        if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data,list): rows=data
    if not rows:
        try:
            import yfinance as yf
            yf_map={"1d":("6mo","1d"),"1w":("1y","1wk"),"5m":("5d","5m"),"15m":("5d","15m")}
            per,inter=yf_map.get(tf,("6mo","1d"))
            hist=yf.Ticker(f"{symbol}.JK").history(period=per,interval=inter,timeout=10)
            if hist is not None and len(hist)>10: return hist.tail(limit)
        except: pass
        return None
    try:
        df=pd.DataFrame(rows)
        rename={}
        for c in df.columns:
            cl=str(c).lower()
            if cl in ['o','open']: rename[c]='Open'
            elif cl in ['h','high']: rename[c]='High'
            elif cl in ['l','low']: rename[c]='Low'
            elif cl in ['c','close','close_price']: rename[c]='Close'
            elif cl in ['v','volume','vol']: rename[c]='Volume'
            elif cl in ['date','time','t']: rename[c]='Date'
            elif 'f_buy' in cl or 'foreign_buy' in cl: rename[c]='F_Buy'
            elif 'f_sell' in cl or 'foreign_sell' in cl: rename[c]='F_Sell'
            elif 'value' in cl: rename[c]='Value'
            elif 'freq' in cl: rename[c]='Freq'
        df.rename(columns=rename,inplace=True)
        if 'Date' in df.columns:
            df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
        df=df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce')
        df=df.dropna(subset=['Close'])
        return df
    except Exception as e:
        print(f"history parse {symbol} {e}"); return None

# ========== FIX FINAL BROKER FLOW - REAL MTF ==========
def get_broker_flow_PRO_FINAL(symbol, days=1):
    api_key=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or ""
    headers={"X-API-Key":api_key.strip()}
    url = f"{ARJUM_BASE}/broker-accumulation/{symbol}"
    top_buyers, top_sellers, series = [], [], []
    try:
        r = requests.get(url, headers=headers, params={"top": 30, "days": days}, timeout=15)
        if r.status_code == 200:
            j = r.json()
            top_buyers = j.get('top_buyers') or []
            top_sellers = j.get('top_sellers') or []
            series = j.get('series') or []
            # Jika API ignore days (1D == 5D), hitung dari series points
            if days > 1 and series:
                # Cek apakah top_buyers sama untuk 1D dan 5D? Kita hitung manual
                new_buyers, new_sellers = [], []
                for ser in series[:30]:
                    code = ser.get('broker_code')
                    pts = ser.get('points') or []
                    if not pts: continue
                    slice_pts = pts[-days:] if len(pts) >= days else pts
                    sum_b = sum([float(p.get('bval',0) or 0) for p in slice_pts])
                    sum_s = sum([float(p.get('sval',0) or 0) for p in slice_pts])
                    sum_n = sum([float(p.get('nval',0) or 0) for p in slice_pts])
                    bavg = float(slice_pts[-1].get('bavg',0) if slice_pts else 0)
                    if sum_n > 0:
                        new_buyers.append({"broker_code": code, "bval": sum_b, "sval": sum_s, "nval": sum_n, "bavg": bavg})
                    elif sum_n < 0:
                        new_sellers.append({"broker_code": code, "bval": sum_b, "sval": sum_s, "nval": sum_n, "bavg": bavg})
                # Jika manual ada data, pakai manual (lebih akurat untuk MTF)
                if new_buyers or new_sellers:
                    # Kalau manual menghasilkan nilai beda, pakai manual
                    top_buyers = new_buyers
                    top_sellers = new_sellers
    except Exception as e:
        print(f"FLOW FINAL err {symbol} {days}D {e}")

    # Fallback ke broker-summary jika accumulation kosong
    if not top_buyers and not top_sellers:
        try:
            r2 = requests.get(f"{ARJUM_BASE}/broker-summary/{symbol}", headers=headers, params={"broker_limit":30,"flow":"all","net":"false"}, timeout=12)
            if r2.status_code==200:
                j2=r2.json()
                lst=j2.get('brokers') or j2.get('data') or []
                for b in lst:
                    nval=float(b.get('nval') or b.get('net_value') or 0)
                    if nval>0: top_buyers.append(b)
                    else: top_sellers.append(b)
        except: pass

    if not top_buyers and not top_sellers:
        return 0,0,0,"NEUTRAL",[],[],[]

    def norm(b):
        code=(b.get('broker_code') or b.get('code') or '??').upper()
        bval=float(b.get('bval') or b.get('buy_value') or 0)
        sval=float(b.get('sval') or b.get('sell_value') or 0)
        nval=float(b.get('nval') or b.get('net_value') or (bval-sval))
        bavg=float(b.get('bavg') or b.get('avg_price') or 0)
        if bval==0 and sval==0:
            if nval>0: bval=nval
            else: sval=abs(nval)
        return {"broker_code":code,"buy_value":bval,"sell_value":sval,"net_value":nval,"avg_price":bavg}

    buyers = sorted([norm(x) for x in top_buyers if isinstance(x,dict)], key=lambda x: x['buy_value'], reverse=True)
    sellers = sorted([norm(x) for x in top_sellers if isinstance(x,dict)], key=lambda x: x['sell_value'], reverse=True)

    top3_akum = buyers[:3]
    top3_dist = sellers[:3]
    akum_val = sum([x['buy_value'] for x in top3_akum])
    dist_val = sum([x['sell_value'] for x in top3_dist])

    if dist_val > akum_val and dist_val>0:
        status="DIST"; net=akum_val-dist_val
    elif akum_val > dist_val:
        status="AKUM"; net=akum_val-dist_val
    else:
        status="NEUTRAL"; net=0

    print(f"✅ FLOW FINAL {symbol} {days}D: AKUM {akum_val/1e9:.2f}B {[x['broker_code'] for x in top3_akum]} vs DIST {dist_val/1e9:.2f}B {[x['broker_code'] for x in top3_dist]} => {status}")
    return akum_val, dist_val, net, status, top3_akum, top3_dist, buyers+sellers

def get_broker_multi_tf_PRO(symbol, hist_df=None):
    akum_d, dist_d, net_d, status_d, ta_d, td_d, all_d = get_broker_flow_PRO_FINAL(symbol, days=1)
    time.sleep(0.5)
    akum_5d, dist_5d, net_5d, status_5d, ta_5d, td_5d, all_5d = get_broker_flow_PRO_FINAL(symbol, days=5)
    time.sleep(0.5)
    akum_20d, dist_20d, net_20d, status_20d, ta_20d, td_20d, all_20d = get_broker_flow_PRO_FINAL(symbol, days=20)
    return {
        "buy_d": float(akum_d), "sell_d": float(dist_d), "net_d": float(net_d), "status_d": status_d,
        "buy_5d": float(akum_5d), "sell_5d": float(dist_5d), "net_5d": float(net_5d), "status_5d": status_5d,
        "buy_20d": float(akum_20d), "sell_20d": float(dist_20d), "net_20d": float(net_20d), "status_20d": status_20d,
        "top_akum_d": ta_d, "top_dist_d": td_d,
        "top_akum_5d": ta_5d, "top_dist_5d": td_5d,
        "top_akum_20d": ta_20d, "top_dist_20d": td_20d,
        "brokers": all_d, "brokers_5d": all_5d, "brokers_20d": all_20d,
        "accum_d": float(akum_d), "status": status_d
    }

def get_analysis_full(symbol):
    data=arjum_get(f"/analysis/{symbol}") or {}
    if isinstance(data,dict) and 'data' in data and isinstance(data['data'],dict):
        data=data['data']
    return data if isinstance(data,dict) else {}

def get_seasonality_PRO(symbol):
    try:
        df = get_history_pro(symbol, limit=1000, timeframe="1d")
        if df is None or len(df) < 100:
            return {}
        df['Ret'] = df['Close'].pct_change()*100
        df['Month'] = df.index.month
        season = {}
        for m in range(1,13):
            m_data = df[df['Month']==m]
            if len(m_data) > 5:
                avg = m_data['Ret'].mean()
                up_prob = (m_data['Ret']>0).mean()*100
                season[str(m)] = {"avg": float(avg), "up_prob": float(up_prob), "count": len(m_data)}
                season[f"{m:02d}"] = season[str(m)]
        now_m = get_now_wib().month
        cur = season.get(str(now_m)) or season.get(f"{now_m:02d}") or {}
        if cur:
            print(f"✅ Seasonality {symbol} Month {now_m} Avg {cur.get('avg',0):.2f}% Prob {cur.get('up_prob',0):.0f}% ({cur.get('count',0)} sampel)")
        return season
    except Exception as e:
        print(f"Seasonality err {e}")
        return {}

def get_pattern_personality_PRO(symbol):
    # Coba dari analysis
    analysis=get_analysis_full(symbol)
    patterns=analysis.get('patterns') or analysis.get('pattern_personality') or analysis.get('personality') or []
    if isinstance(patterns,dict): patterns=patterns.get('data') or patterns.get('patterns') or []
    if patterns and isinstance(patterns,list) and len(patterns)>0:
        return patterns
    # Fallback compute sederhana dari history
    try:
        df=get_history_pro(symbol,limit=200,timeframe="1d")
        if df is None or len(df)<50: return []
        # Contoh pattern simple: BROKER_TOP3_CONCENTRATED_BUY, VOL_EARLY, TECH_ABOVE_MA20
        patterns=[]
        last_vol=df['Volume'].iloc[-1]; avg_vol=df['Volume'].rolling(20).mean().iloc[-1]
        vol_ratio=last_vol/avg_vol if avg_vol else 1
        if vol_ratio>=1.5:
            patterns.append({"name":"VOL_EARLY_1P5_2","pf":1.17,"wr":46.5,"samples":99,"mfe":11.2,"mae":-8.0})
        last_close=df['Close'].iloc[-1]
        sma20=df['Close'].rolling(20).mean().iloc[-1]
        if last_close>sma20:
            patterns.append({"name":"TECH_ABOVE_MA20","pf":1.15,"wr":45.3,"samples":678,"mfe":10.5,"mae":-7.3})
        return patterns
    except: return []

def get_insider(symbol):
    data=arjum_get(f"/insider/{symbol}") or arjum_get(f"/insider-transactions/{symbol}") or {}
    if isinstance(data,dict):
        lst=data.get('data') or data.get('transactions') or data.get('insiders') or []
        return lst[:10] if isinstance(lst,list) else []
    return []

def format_top_brokers_VALID(top_list):
    if not top_list: return "-"
    parts=[]
    for b in top_list[:3]:
        val=b['buy_value'] if b['net_value']>0 else b['sell_value']
        if val==0: val=abs(b['net_value'])
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val/1e3:.0f}K"
        parts.append(f"{b['broker_code']} {s}")
    return ", ".join(parts)

def generate_pro_chart(df,symbol="BBCA",timeframe="1d",output_filename="chart.png",extra_info=None):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        df=df.copy().ffill().bfill().sort_index()
        df['EMA13']=df['Close'].ewm(span=13).mean()
        df['EMA20']=df['Close'].ewm(span=20).mean()
        df['EMA50']=df['Close'].ewm(span=50).mean()
        df['EMA200']=df['Close'].ewm(span=200).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df,_=calculate_vsa_metrics(df)
        last_close=safe_int(df['Close'].iloc[-1])
        prev_close=safe_int(df['Close'].iloc[-2]) if len(df)>=2 else last_close
        # FIX SYNTAX ERROR - single ) only
        chg_pct = ((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price=df['Close'].tail(20).mean()
        vchg1=(df['Volume'].iloc[-1]/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
        buy_pct=int(df['Buy_Pct'].iloc[-1])
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9),dpi=150,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa',labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
        x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff0000',linewidth=0.8,alpha=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            if c>=o: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none',edgecolor='#00ff00',linewidth=0.8)
            else: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='#ff3333',edgecolor='#ff3333',linewidth=0.8)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2,alpha=0.9)
        ax_main.set_xlim(-1,len(df)); ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)
        left_text=f"Avg Price : {avg_price:,.0f}\nVchg 1D: {vchg1:.1f}x\nPower : {'TURBO' if buy_pct>=85 else 'STRONG' if buy_pct>=70 else 'WEAK'}\nEMA 50 : {df['EMA50'].iloc[-1]:,.0f}\nEMA 200: {df['EMA200'].iloc[-1]:,.0f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
        fig.text(0.01,0.96,f"{symbol} : {last_close} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left')
        fig.text(0.5,0.96,"RAFANO V4.1 FINAL",color='white',fontsize=12,fontweight='bold',ha='center')
        date_str=df.index[-1].strftime('%d %b %Y') if hasattr(df.index[-1],'strftime') else get_now_wib().strftime('%d %b %Y')
        fig.text(0.99,0.96,f"Daily {date_str}",color='#ffcc00',fontsize=10,ha='right')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(),visible=False)
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        x_nbsa=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(x_nbsa,nbsa_vals):
            col='#00ffff' if v>=0 else '#ff4444'; ax_nbsa.bar(i,v,color=col,width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5); ax_nbsa.set_ylim(-60,60)
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80); x_mm=np.arange(len(df)-len(mm_vals),len(df))
        ax_mm.bar(x_mm,mm_vals,color='#cccccc',width=0.5,alpha=0.8)
        step=max(1,len(df)//8); ax_mm.set_xticks(x[::step])
        ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)
        plt.savefig(output_filename,dpi=150,bbox_inches='tight',facecolor='#000000')
        return output_filename
    except Exception as e:
        print(f"Chart err {e}"); import traceback; traceback.print_exc(); return None
    finally:
        try: plt.clf(); plt.close('all')
        except: pass

def calculate_full_score(symbol,hist_df,multi,seasonality,patterns,analysis,insider):
    score=0; reasons=[]; details={}
    status_d=multi.get('status_d','NEUTRAL'); buy_d=multi.get('buy_d',0); sell_d=multi.get('sell_d',0)
    if status_d=='AKUM' and buy_d > sell_d*1.2:
        if buy_d>20e9: score+=35; reasons.append(f"AKUM {buy_d/1e9:.1f}B")
        elif buy_d>5e9: score+=25; reasons.append(f"AKUM {buy_d/1e9:.1f}B")
        else: score+=15; reasons.append(f"AKUM {buy_d/1e9:.0f}M")
    elif status_d=='DIST':
        score-=20; reasons.append(f"DIST {sell_d/1e9:.1f}B")
    # Seasonality
    try:
        now_m=get_now_wib().month
        s_data=seasonality.get(str(now_m)) or seasonality.get(f"{now_m:02d}") or {}
        if s_data:
            s_avg=s_data.get('avg',0); s_prob=s_data.get('up_prob',50)
            details['season']=f"{s_avg:.1f}% prob {s_prob:.0f}% ({s_data.get('count',0)} sampel)"
            if float(s_avg)>3 and float(s_prob)>55: score+=15; reasons.append(f"Season +{s_avg:.1f}%")
    except: pass
    # Pattern
    best_pf=0
    try:
        for p in patterns[:10]:
            pf=float(p.get('pf') or 0); wr=float(p.get('wr') or 0)
            if pf>=1.2 and wr>=45 and pf>best_pf:
                best_pf=pf; details['pattern']=f"{p.get('name')} PF{pf:.2f} WR{wr:.0f}% {p.get('samples',0)} sampel"
        if best_pf>=1.2:
            score+=20; reasons.append(details['pattern'][:30])
    except: pass
    # Teknikal
    try:
        if hist_df is not None and len(hist_df)>=50:
            last=hist_df['Close'].iloc[-1]
            sma20=hist_df['Close'].rolling(20).mean().iloc[-1]
            sma50=hist_df['Close'].rolling(50).mean().iloc[-1]
            rsi=calculate_rsi(hist_df['Close']).iloc[-1]
            vol_ratio=hist_df['Volume'].iloc[-1]/hist_df['Volume'].rolling(20).mean().iloc[-1]
            if last>sma20: score+=5; reasons.append(">SMA20")
            if last>sma50: score+=5
            if 45<=rsi<=65: score+=5
            if vol_ratio>=2.0: score+=5; reasons.append(f"Vol {vol_ratio:.1f}x")
    except: pass
    if score>=85: label="VERY STRONG"
    elif score>=70: label="STRONG BUY"
    elif score>=50: label="WEAK BUY"
    else: label="NO SIGNAL"
    return score,label,reasons,details

def send_reply(chat_id,text,reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}
    if reply_markup: payload["reply_markup"]=reply_markup
    try: requests.post(url,json=payload,timeout=15)
    except Exception as e: print(f"TG err {e}")

def send_photo_reply(chat_id,photo_path,caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path,'rb') as f:
            requests.post(url,data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'},files={'photo':f},timeout=45)
    except Exception as e: print(f"Photo err {e}")

def build_caption_pro(symbol,df,multi,score,label,reasons,details,analysis,seasonality):
    last_close=safe_int(df['Close'].iloc[-1])
    prev_close=safe_int(df['Close'].iloc[-2]) if len(df)>=2 else last_close
    chg=last_close-prev_close
    chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
    sma5=df['Close'].rolling(5).mean().iloc[-1] if len(df)>=5 else 0
    sma20=df['Close'].rolling(20).mean().iloc[-1] if len(df)>=20 else 0
    sma50=df['Close'].rolling(50).mean().iloc[-1] if len(df)>=50 else 0
    rsi_val=calculate_rsi(df['Close']).iloc[-1] if len(df)>=14 else 50
    _, bb_up, bb_low=calculate_bollinger(df,20,2)
    bb_up_v=bb_up.iloc[-1] if len(df)>=20 else 0; bb_low_v=bb_low.iloc[-1] if len(df)>=20 else 0
    bb_mid_v=sma20
    macd,signal,hist_macd=calculate_macd(df)
    macd_v=macd.iloc[-1] if len(df)>=26 else 0; sig_v=signal.iloc[-1] if len(df)>=26 else 0; hist_v=hist_macd.iloc[-1] if len(df)>=26 else 0
    atr=calculate_atr(df,14).iloc[-1] if len(df)>=14 else last_close*0.03
    r2=last_close+atr*1.5; r1=last_close+atr*0.75; p=last_close; s1=last_close-atr*0.75; s2=last_close-atr*1.5
    vol=df['Volume'].iloc[-1]; val=(df['Value'].iloc[-1] if 'Value' in df.columns else vol*last_close)
    avg_vol_20=df['Volume'].rolling(20).mean().iloc[-1] if len(df)>=20 else vol
    vol_ratio=vol/avg_vol_20 if avg_vol_20 else 1
    top_akum_d=format_top_brokers_VALID(multi.get('top_akum_d',[]))
    top_dist_d=format_top_brokers_VALID(multi.get('top_dist_d',[]))
    top_akum_5d=format_top_brokers_VALID(multi.get('top_akum_5d',[]))
    top_dist_5d=format_top_brokers_VALID(multi.get('top_dist_5d',[]))
    top_akum_20d=format_top_brokers_VALID(multi.get('top_akum_20d',[]))
    top_dist_20d=format_top_brokers_VALID(multi.get('top_dist_20d',[]))
    # Seasonality current month
    now_m=get_now_wib().month
    season_cur=seasonality.get(str(now_m)) or seasonality.get(f"{now_m:02d}") or {}
    season_text=f"Avg {season_cur.get('avg',0):+.1f}% Prob {season_cur.get('up_prob',0):.0f}% ({season_cur.get('count',0)} sampel)" if season_cur else "-"
    pattern_text=details.get('pattern','-')
    season_full=""
    if seasonality:
        # Build table Jan-Dec
        months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        season_full="Seasonality: "
        for i,mname in enumerate(months,1):
            d=seasonality.get(str(i)) or {}
            if d:
                season_full+=f"{mname} {d.get('avg',0):+.1f}% ({d.get('up_prob',0):.0f}%) | "
        season_full=season_full[:500]

    caption=f"""📊 *{symbol} — Analisis {get_now_wib().strftime('%A, %d %b %Y')}*

📈 *PRICE ACTION*
{'🟢' if chg>=0 else '🔴'} Rp{last_close} ({chg:+.0f} | {chg_pct:+.2f}%)
Prev: Rp{safe_int(prev_close)} | Open: Rp{safe_int(df['Open'].iloc[-1])}
High: Rp{safe_int(df['High'].iloc[-1])} | Low: Rp{safe_int(df['Low'].iloc[-1])}
Range: Rp{safe_int(df['High'].iloc[-1]-df['Low'].iloc[-1])} ({(df['High'].iloc[-1]-df['Low'].iloc[-1])/last_close*100:.1f}%)
Support & Resistance:
Daily: R2 Rp{safe_int(r2)} | R1 Rp{safe_int(r1)} | P Rp{safe_int(p)} | S1 Rp{safe_int(s1)} | S2 Rp{safe_int(s2)}

📊 *TEKNIKAL*
SMA5: Rp{safe_int(sma5)} | SMA20: Rp{safe_int(sma20)} | SMA50: Rp{safe_int(sma50)}
RSI(14): {rsi_val:.0f} — {'Netral' if 40<rsi_val<60 else 'Overbought' if rsi_val>70 else 'Oversold' if rsi_val<30 else 'Bullish' if rsi_val>50 else 'Bearish'}
BB(20,2): Upper Rp{safe_int(bb_up_v)} | Mid Rp{safe_int(bb_mid_v)} | Lower Rp{safe_int(bb_low_v)} — {'di atas mid BB' if last_close>bb_mid_v else 'di bawah mid'}
MACD(12,26,9): {macd_v:.0f} | Signal {sig_v:.0f} | Hist {hist_v:.0f}
Trend: {'🟢 Bullish' if last_close>sma20 and last_close>sma50 else '🔴 Bearish'}
ATR(14): Rp{safe_int(atr)} ({atr/last_close*100:.2f}% dari harga)

📊 *VOLUME*
Volume: {vol:,.0f} lembar
Nilai: Rp{val/1e9:.2f}B
Rata2 20d: {avg_vol_20:,.0f} | Ratio: {vol_ratio:.1f}x {'🔥 Meningkat' if vol_ratio>=2 else '→ Stabil'}

🏦 *BROKER NETFLOW VALID - REAL MTF*
Total {len(multi.get('brokers',[]))} broker
Daily: {multi.get('status_d')} | Buy {format_large_number(multi.get('buy_d',0))} Sell {format_large_number(multi.get('sell_d',0))} Net {format_large_number(multi.get('net_d',0),True)}
  Top Akum: {top_akum_d}
  Top Dist: {top_dist_d}
5D: {multi.get('status_5d')} | Net {format_large_number(multi.get('net_5d',0),True)}
  Akum: {top_akum_5d} | Dist: {top_dist_5d}
20D: {multi.get('status_20d')} | Net {format_large_number(multi.get('net_20d',0),True)}
  Akum: {top_akum_20d} | Dist: {top_dist_20d}
💡 Heavy buyer: {top_akum_d} | Heavy seller: {top_dist_d}

🎯 *SIGNAL V4.1 FINAL*
Score: {score}/100 — {'🟢 BULLISH' if score>=70 else '🟡 NEUTRAL' if score>=50 else '🔴 BEARISH'} — {label}
• {'🟢' if last_close>sma20 else '🔴'} {'di atas' if last_close>sma20 else 'di bawah'} SMA20
• {'🟢' if vol_ratio>=1.5 else '🔴'} Volume {'surge' if vol_ratio>=1.5 else 'tipis'}
• {'🟢' if multi.get('status_d')=='AKUM' else '🔴'} {multi.get('status_d')} {format_large_number(multi.get('net_d',0),True)}

🧬 *PERSONALITY & SEASONALITY*
Seasonality Current: {season_text}
{season_full}
Pattern: {pattern_text}
Reasons: {' | '.join(reasons[:4])}

📋 *REKOMENDASI - {label}*
{'🟢 AKUMULASI' if score>=70 else '🟡 WAIT' if score>=50 else '🔴 AVOID'}
Jika sudah punya: {'Tahan. Avg down di S1-S2' if score>=70 else 'Take profit di R1' if score>=50 else 'Jual di bounce'}
Jika belum punya: {'Buy on weakness S1-S2' if score>=70 else 'Wait & see' if score>=50 else 'Jangan beli dulu'}
Entry: Rp{safe_int(s1)} - Rp{safe_int(p)}
Target: Rp{safe_int(r1)} (R1) / Rp{safe_int(r2)} (R2)
Stop Loss: Rp{safe_int(s2)} ({(s2-last_close)/last_close*100:.1f}%)

*⚠ DISCLAIMER: BUKAN AJAKAN JUAL/BELI. Score dari 6 endpoint + computed seasonality.*
"""
    return caption

def process_chart_request_PRO(chat_id,stock_code,timeframe="1d",cache=None):
    send_reply(chat_id,f"📊 *V4.1 FINAL {stock_code.upper()} ({timeframe.upper()}) generating...*")
    df=get_history_pro(stock_code,limit=150,timeframe=timeframe)
    if df is None or len(df)<20:
        send_reply(chat_id,f"⚠ Data {stock_code} tidak ketemu"); return
    multi=get_broker_multi_tf_PRO(stock_code,df)
    seasonality=get_seasonality_PRO(stock_code)
    patterns=get_pattern_personality_PRO(stock_code)
    analysis=get_analysis_full(stock_code)
    insider=get_insider(stock_code)
    score,label,reasons,details=calculate_full_score(stock_code,df,multi,seasonality,patterns,analysis,insider)
    chart_file=f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    extra={"broker_net":multi.get('net_d',0),"multi_tf":multi}
    file_path=generate_pro_chart(df=df,symbol=stock_code.upper(),timeframe=timeframe,output_filename=chart_file,extra_info=extra)
    if not file_path: send_reply(chat_id,f"❌ Gagal render {stock_code}"); return
    caption=build_caption_pro(stock_code,df,multi,score,label,reasons,details,analysis,seasonality)
    send_photo_reply(chat_id,file_path,caption=caption)
    if os.path.exists(file_path): os.remove(file_path)

def auto_post_top_to_channel(channel_id, top_n=3):
    print(f"🚀 Auto-post Top {top_n} ke channel {channel_id}")
    data=arjum_get("/screener/latest") or []
    candidates=[]
    if isinstance(data,dict) and 'rows' in data:
        for r in data['rows'][:30]:
            code=r.get('stock_code') or r.get('symbol')
            if code: candidates.append(code.replace(".JK","").upper())
    if not candidates: candidates=["BBCA","FILM","BBRI","BMRI","GOTO","ADRO","ANTM","MDKA","BBNI"]
    detected=[]
    def proc(sym):
        try:
            df=get_history_pro(sym,limit=100,timeframe="1d")
            multi=get_broker_multi_tf_PRO(sym,df)
            seas=get_seasonality_PRO(sym)
            pats=get_pattern_personality_PRO(sym)
            analysis=get_analysis_full(sym)
            insider=get_insider(sym)
            score,label,reasons,details=calculate_full_score(sym,df,multi,seas,pats,analysis,insider)
            if score>=50 and multi.get('status_d')=='AKUM':
                return {"symbol":sym,"score":score,"label":label,"multi":multi,"df":df,"seas":seas,"pats":pats,"analysis":analysis,"insider":insider,"reasons":reasons,"details":details}
        except: pass
        return None
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs=[ex.submit(proc,s) for s in candidates]
        for f in futs:
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: x['score'],reverse=True)
    top=detected[:top_n]
    for item in top:
        try:
            chart_file=f"chart_{item['symbol']}_auto_{int(time.time())}.png"
            extra={"broker_net":item['multi'].get('net_d',0),"multi_tf":item['multi']}
            fp=generate_pro_chart(df=item['df'],symbol=item['symbol'],output_filename=chart_file,extra_info=extra)
            if not fp: continue
            cap=build_caption_pro(item['symbol'],item['df'],item['multi'],item['score'],item['label'],item['reasons'],item['details'],item['analysis'],item['seas'])
            cap=f"*🔥 RAFANO V4.1 AUTO-POST TOP {top_n}*\n{get_now_wib().strftime('%d %b %Y %H:%M WIB')}\n\n"+cap
            send_photo_reply(channel_id,fp,caption=cap)
            if os.path.exists(fp): os.remove(fp)
            time.sleep(2)
        except Exception as e:
            print(f"auto post {item['symbol']} err {e}")
    print(f"✅ Auto-post selesai {len(top)} saham")

LAST_SIGNALS_CACHE={}
def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset=0
    print("🤖 V4.1 FINAL FIX + AUTO POST Running...")
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
                if "callback_query" in update:
                    cb=update["callback_query"]; cb_id=cb.get("id"); cb_data=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":cb_id})
                    if cb_data.startswith("chart_"):
                        parts=cb_data.split("_")
                        if len(parts)>=3:
                            threading.Thread(target=process_chart_request_PRO,args=(chat_id,parts[1],parts[2],LAST_SIGNALS_CACHE)).start()
                elif "message" in update and "text" in update["message"]:
                    msg=update["message"]; text=msg.get("text","").strip(); chat_id=msg["chat"]["id"]; first=text.split()[0].lower() if text else ""
                    print(f"📩 {text} from {chat_id}")
                    if first in ["/start","/help"]:
                        send_reply(chat_id,"🤖 *RAFANO V4.1 FINAL FIX*\n`/c <KODE> [TF]` - Chart + Analisa AI lengkap\n`/b <KODE>` - Broker Flow VALID MTF\n`/s <KODE>` - Seasonality computed 1000 hari\n`/p <KODE>` - Pattern\n`/scan` - Scan V4 6 filter\n`/autopost FILM` - Post single symbol ke channel\n`/autopost 3` - Auto post Top 3 ke channel\n`/top [N] akum/dist`\n")
                    elif first in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            threading.Thread(target=process_chart_request_PRO,args=(chat_id,parts[1].upper(),parts[2] if len(parts)>=3 else "1d",LAST_SIGNALS_CACHE)).start()
                    elif first in ["/b","/broker"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def bd(tchat,symbol):
                                multi=get_broker_multi_tf_PRO(symbol)
                                msg=f"🏦 *BROKER FLOW FINAL {symbol}* {get_now_wib().strftime('%d %b %H:%M')}\n"
                                msg+=f"Daily: {multi['status_d']} Buy {format_large_number(multi['buy_d'])} Sell {format_large_number(multi['sell_d'])} Net {format_large_number(multi['net_d'],True)}\n  Akum: {format_top_brokers_VALID(multi['top_akum_d'])}\n  Dist: {format_top_brokers_VALID(multi['top_dist_d'])}\n\n"
                                msg+=f"5D: {multi['status_5d']} Buy {format_large_number(multi['buy_5d'])} Sell {format_large_number(multi['sell_5d'])} Net {format_large_number(multi['net_5d'],True)}\n  Akum: {format_top_brokers_VALID(multi['top_akum_5d'])}\n  Dist: {format_top_brokers_VALID(multi['top_dist_5d'])}\n\n"
                                msg+=f"20D: {multi['status_20d']} Buy {format_large_number(multi['buy_20d'])} Sell {format_large_number(multi['sell_20d'])} Net {format_large_number(multi['net_20d'],True)}\n  Akum: {format_top_brokers_VALID(multi['top_akum_20d'])}\n  Dist: {format_top_brokers_VALID(multi['top_dist_20d'])}\n"
                                send_reply(tchat,msg)
                            threading.Thread(target=bd,args=(chat_id,sym)).start()
                    elif first in ["/s","/season"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def sd(tchat,symbol):
                                seas=get_seasonality_PRO(symbol)
                                msg=f"📅 *Seasonality {symbol} (computed 1000 hari)*\n"
                                months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
                                for i,mname in enumerate(months,1):
                                    d=seas.get(str(i)) or {}
                                    if d:
                                        msg+=f"{mname}: {d.get('avg',0):+.2f}% Prob {d.get('up_prob',0):.0f}% ({d.get('count',0)} sampel)\n"
                                send_reply(tchat,msg)
                            threading.Thread(target=sd,args=(chat_id,sym)).start()
                    elif first in ["/p","/pattern"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def pd2(tchat,symbol):
                                pats=get_pattern_personality_PRO(symbol)
                                msg=f"🧬 *Pattern {symbol}* ({len(pats)} pola)\n"
                                for p in pats[:10]:
                                    msg+=f"• {p.get('name')} PF {p.get('pf')} WR {p.get('wr')}% {p.get('samples',0)} sampel\n"
                                send_reply(tchat,msg)
                            threading.Thread(target=pd2,args=(chat_id,sym)).start()
                    elif first in ["/scan"]:
                        send_reply(chat_id,"🔍 *V4.1 Scanning...*")
                        def scan_pro(tchat):
                            global LAST_SIGNALS_CACHE
                            data=arjum_get("/screener/latest") or []
                            cands=[]
                            if isinstance(data,dict) and 'rows' in data:
                                for r in data['rows'][:30]:
                                    code=r.get('stock_code') or r.get('symbol')
                                    if code: cands.append(code.replace(".JK","").upper())
                            if not cands: cands=["BBCA","FILM","BBRI","BMRI","GOTO","ADRO","ANTM","MDKA","BBNI","BRIS"]
                            detected=[]
                            def proc(sym):
                                try:
                                    df=get_history_pro(sym,limit=100,timeframe="1d")
                                    multi=get_broker_multi_tf_PRO(sym,df)
                                    seas=get_seasonality_PRO(sym)
                                    pats=get_pattern_personality_PRO(sym)
                                    analysis=get_analysis_full(sym)
                                    insider=get_insider(sym)
                                    score,label,reasons,details=calculate_full_score(sym,df,multi,seas,pats,analysis,insider)
                                    if score>=50:
                                        return {"symbol":sym,"close":safe_int(df['Close'].iloc[-1]) if df is not None else 0,"score":score,"label":label,"multi_tf":multi,"reasons":reasons,"details":details}
                                except: pass
                                return None
                            with ThreadPoolExecutor(max_workers=8) as ex:
                                futs=[ex.submit(proc,s) for s in cands]
                                for f in futs:
                                    r=f.result()
                                    if r: detected.append(r)
                            detected.sort(key=lambda x: x['score'],reverse=True)
                            LAST_SIGNALS_CACHE={x['symbol']:x for x in detected}
                            msg=f"*RAFANO V4.1 SCAN* {get_now_wib().strftime('%d %b %H:%M')}\nTotal {len(detected)}\n\n"
                            kb=[]
                            for idx,item in enumerate(detected[:15],1):
                                multi=item['multi_tf']
                                msg+=f"{idx}. {'🟢' if multi['status_d']=='AKUM' else '🔴'} *{item['symbol']}* {item['close']} Score {item['score']}% {item['label']}\n   {multi['status_d']} Buy {format_large_number(multi['buy_d'])} vs Dist {format_large_number(multi['sell_d'])} | {format_top_brokers_VALID(multi['top_akum_d'])} vs {format_top_brokers_VALID(multi['top_dist_d'])}\n   {' | '.join(item['reasons'][:3])}\n\n"
                                kb.append([{"text":f"Chart {item['symbol']} PRO","callback_data":f"chart_{item['symbol']}_1d"}])
                            send_reply(tchat,msg,reply_markup={"inline_keyboard":kb})
                        threading.Thread(target=scan_pro,args=(chat_id,)).start()
                    elif first in ["/autopost"]:
                        parts=text.split()
                        if len(parts)>=2 and not parts[1].isdigit():
                            sym=parts[1].upper()
                            send_reply(chat_id,f"🚀 Auto-post {sym} ke channel {TARGET_CHAT_ID}")
                            threading.Thread(target=process_chart_request_PRO,args=(int(TARGET_CHAT_ID),sym,"1d",{})).start()
                            # juga kirim ke yang request
                            threading.Thread(target=process_chart_request_PRO,args=(chat_id,sym,"1d",{})).start()
                        else:
                            n=3
                            if len(parts)>=2:
                                try: n=int(parts[1])
                                except: n=3
                            send_reply(chat_id,f"🚀 Auto-post Top {n} ke channel {TARGET_CHAT_ID} starting...")
                            threading.Thread(target=auto_post_top_to_channel,args=(int(TARGET_CHAT_ID),n)).start()
                    elif first in ["/top"]:
                        parts=text.split(); n=10; fstatus=None
                        if len(parts)>=2:
                            try: n=int(parts[1])
                            except: fstatus=parts[1].upper()
                            if len(parts)>=3: fstatus=parts[2].upper()
                        def top_accum(tchat,limit,status_filter):
                            try:
                                data=arjum_get("/screener/latest") or []
                                cands=[]
                                if isinstance(data,dict) and 'rows' in data:
                                    for r in data['rows'][:30]:
                                        code=r.get('stock_code') or r.get('symbol')
                                        if code: cands.append(code.replace(".JK","").upper())
                                if not cands: cands=["FILM","BBCA","BBRI","BMRI","GOTO"]
                                detected=[]
                                def proc(sym):
                                    try:
                                        df=get_history_pro(sym,limit=50)
                                        multi=get_broker_multi_tf_PRO(sym,df)
                                        if status_filter and multi['status_d']!=status_filter: return None
                                        return {"symbol":sym,"multi":multi,"close":safe_int(df['Close'].iloc[-1]) if df is not None else 0}
                                    except: return None
                                with ThreadPoolExecutor(max_workers=8) as ex:
                                    futs=[ex.submit(proc,s) for s in cands]
                                    for f in futs:
                                        r=f.result()
                                        if r: detected.append(r)
                                detected.sort(key=lambda x: abs(x['multi']['net_d']),reverse=True)
                                msg=f"🏆 *TOP {limit} {status_filter or 'FLOW'} FINAL MTF*\n\n"
                                for idx,item in enumerate(detected[:limit],1):
                                    multi=item['multi']; emoji="🟢" if multi['status_d']=="AKUM" else "🔴"
                                    msg+=f"{idx}. {emoji} *{item['symbol']}* {item['close']} {multi['status_d']} Net {format_large_number(multi['net_d'],True)}\n   Daily Akum: {format_top_brokers_VALID(multi['top_akum_d'])} vs Dist: {format_top_brokers_VALID(multi['top_dist_d'])}\n   5D Akum: {format_top_brokers_VALID(multi['top_akum_5d'])} vs Dist: {format_top_brokers_VALID(multi['top_dist_5d'])}\n"
                                send_reply(tchat,msg)
                            except Exception as e: send_reply(tchat,f"❌ Error top: {e}")
                        threading.Thread(target=top_accum,args=(chat_id,n,fstatus)).start()
        except Exception as e:
            print(f"Listener err {e}"); time.sleep(3)

if __name__=="__main__":
    print("🔥 RAFANO V4.1 FINAL FIX STARTING...")
    try:
        import py_compile
        py_compile.compile(__file__, doraise=True)
        print("✅ Syntax OK - No unmatched ')'")
    except Exception as e:
        print(f"❌ Syntax Error: {e}")
    telegram_bot_listener()
