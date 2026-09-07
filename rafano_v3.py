"""
RAFANO V4 PRO MAX - Full 6 Endpoint Arjum
- Broker Summary with from/to (B.val & S.val) - VALID AKUM vs DIST
- Seasonality Pattern (Avg, Up Prob per bulan)
- Pattern Personality (PF, WR, MFE, MAE)
- Historical Data Full (F Buy, F Sell, Value, Volume, Freq)
- Analisa AI (SMA, RSI, BB, MACD, Score)
- Insider Transaction
- Pro Chart OKE SAHAM Style

Command: /c FILM -> Chart + Caption lengkap kayak web Arjum
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

def round_to_ihsg_fraction(price):
    if pd.isna(price) or price<=0: return 0
    price=float(price)
    if price<200: tick=1
    elif price<500: tick=2
    elif price<2000: tick=5
    elif price<5000: tick=10
    else: tick=25
    return int(round(price/tick)*tick)

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

# ========== ARJUM CLIENT PRO ==========
BROKER_CACHE={}
def arjum_get(path,params=None,use_cache=True):
    # cache 5 menit
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
            print(f"⚠ {path} {params} -> {r.status_code} {r.text[:200]}")
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

def get_broker_flow_PRO(symbol,days=1):
    """FIX PRO: pakai from/to biar Daily, 5D, 20D beda, dan parse B.val & S.val"""
    from datetime import timedelta
    end=get_now_wib().date()
    start=end - timedelta(days=days-1)
    api_key=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or ""
    headers={"X-API-Key":api_key.strip()}
    # Coba 3 varian params yang dipakai web Arjum
    tries=[
        {"from":start.isoformat(),"to":end.isoformat(),"broker_limit":30,"net":"false","flow":"all","all_data":"false"},
        {"from":start.isoformat(),"to":end.isoformat(),"broker_limit":30,"flow":"all"},
        {"broker_limit":30,"flow":"all","net":"false"},
        {"broker_limit":30}
    ]
    brokers_raw=[]
    for p in tries:
        data=arjum_get(f"/broker-summary/{symbol}",params=p,use_cache=False)
        if data and isinstance(data,dict):
            lst=data.get('brokers') or data.get('data') or data.get('result') or []
            if lst and len(lst)>0 and isinstance(lst[0],dict):
                # cek apakah ada buy & sell value
                if any(('bval' in x or 'buy_value' in x or 'B.val' in x) for x in lst):
                    brokers_raw=lst
                    print(f"✅ BROKER {symbol} {days}D from={p.get('from')} got {len(lst)} with B.val & S.val")
                    break
    # Fallback accumulation
    if not brokers_raw:
        try:
            r=requests.get(f"{ARJUM_BASE}/broker-accumulation/{symbol}",headers=headers,params={"top":30,"days":days},timeout=12)
            if r.status_code==200:
                j=r.json()
                tb=j.get('top_buyers') or []; ts=j.get('top_sellers') or []
                brokers_raw=tb+ts
                if not brokers_raw and j.get('series'):
                    for ser in j.get('series',[])[:30]:
                        code=ser.get('broker_code'); pts=ser.get('points') or []
                        if not pts: continue
                        sl=pts[-days:] if len(pts)>=days else pts
                        sum_b=sum([float(x.get('bval',0)) for x in sl])
                        sum_s=sum([float(x.get('sval',0)) for x in sl])
                        sum_n=sum([float(x.get('nval',0)) for x in sl])
                        brokers_raw.append({"broker_code":code,"bval":sum_b,"sval":sum_s,"nval":sum_n,"bavg":sl[-1].get('bavg',0) if sl else 0})
        except Exception as e: print(f"acc fallback {symbol} err {e}")

    if not brokers_raw:
        return 0,0,0,"NEUTRAL",[],[],[]

    def norm(b):
        code=(b.get('broker_code') or b.get('code') or '??').upper()
        bval=float(b.get('bval') or b.get('buy_value') or b.get('buy_val') or 0)
        sval=float(b.get('sval') or b.get('sell_value') or b.get('sell_val') or 0)
        nval=float(b.get('nval') or b.get('net_value') or b.get('net_val') or (bval-sval))
        bavg=float(b.get('bavg') or b.get('avg_price') or b.get('buy_avg') or 0)
        savg=float(b.get('savg') or b.get('sell_avg') or 0)
        if bval==0 and sval==0:
            if nval>0: bval=nval
            else: sval=abs(nval)
        return {"broker_code":code,"buy_value":bval,"sell_value":sval,"net_value":nval,"avg_price":bavg if bavg>0 else savg,"bavg":bavg,"savg":savg}

    parsed=[norm(x) for x in brokers_raw if isinstance(x,dict)]
    akum=[x for x in parsed if x['net_value']>0]
    dist=[x for x in parsed if x['net_value']<0]
    akum=sorted(akum,key=lambda x: x['buy_value'],reverse=True)
    dist=sorted(dist,key=lambda x: x['sell_value'],reverse=True)
    top3_akum=akum[:3]; top3_dist=dist[:3]
    top3_akum_val=sum([x['buy_value'] for x in top3_akum])
    top3_dist_val=sum([x['sell_value'] for x in top3_dist])
    if top3_dist_val>top3_akum_val and top3_dist_val>0:
        status="DIST"; net=top3_akum_val-top3_dist_val
    elif top3_akum_val>top3_dist_val:
        status="AKUM"; net=top3_akum_val-top3_dist_val
    else:
        status="NEUTRAL"; net=0
    print(f"FLOW {symbol} {days}D: AKUM {top3_akum_val/1e9:.2f}B vs DIST {top3_dist_val/1e9:.2f}B => {status}")
    return top3_akum_val,top3_dist_val,net,status,top3_akum,top3_dist,parsed

def get_broker_multi_tf_PRO(symbol,hist_df=None):
    akum_d,dist_d,net_d,status_d,ta_d,td_d,all_d=get_broker_flow_PRO(symbol,days=1)
    time.sleep(0.4)
    akum_5d,dist_5d,net_5d,status_5d,ta_5d,td_5d,all_5d=get_broker_flow_PRO(symbol,days=5)
    time.sleep(0.4)
    akum_20d,dist_20d,net_20d,status_20d,ta_20d,td_20d,all_20d=get_broker_flow_PRO(symbol,days=20)
    # VSA
    vsa_1d=vsa_5d=vsa_20d=0
    if hist_df is not None and len(hist_df)>=5:
        try:
            if 'Net_Val_VSA' not in hist_df.columns:
                hist_df,_=calculate_vsa_metrics(hist_df)
            vsa_1d=float(hist_df['Net_Val_VSA'].iloc[-1]*hist_df['Close'].iloc[-1])
            vsa_5d=float((hist_df['Net_Val_VSA'].tail(5)*hist_df['Close'].tail(5)).sum())
        except: pass
    def avg_from(lst,hist,days_slice):
        try:
            vals=[x['avg_price'] for x in lst if x['avg_price']>0]
            if vals: return float(np.mean(vals))
            if hist is not None: return float(hist['Close'].tail(days_slice).mean())
        except: pass
        return 0
    return {
        "buy_d":float(akum_d),"sell_d":float(dist_d),"net_d":float(net_d),"status_d":status_d,
        "buy_5d":float(akum_5d),"sell_5d":float(dist_5d),"net_5d":float(net_5d),"status_5d":status_5d,
        "buy_20d":float(akum_20d),"sell_20d":float(dist_20d),"net_20d":float(net_20d),"status_20d":status_20d,
        "avg_d":avg_from(ta_d or all_d,hist_df,1),"avg_5d":avg_from(ta_5d or all_5d,hist_df,5),"avg_20d":avg_from(ta_20d or all_20d,hist_df,20),
        "top_akum_d":ta_d,"top_dist_d":td_d,"top_akum_5d":ta_5d,"top_dist_5d":td_5d,"top_akum_20d":ta_20d,"top_dist_20d":td_20d,
        "brokers":all_d,"brokers_5d":all_5d,"brokers_20d":all_20d,
        "vsa_1d":vsa_1d,"vsa_5d":vsa_5d,"accum_d":float(akum_d),"status":status_d
    }

def get_analysis_full(symbol):
    data=arjum_get(f"/analysis/{symbol}") or {}
    # Arjum kadang return di key 'data'
    if isinstance(data,dict) and 'data' in data and isinstance(data['data'],dict):
        data=data['data']
    return data if isinstance(data,dict) else {}

def get_seasonality(symbol):
    # Endpoint bisa /seasonality/{symbol} atau /analysis sudah include
    data=arjum_get(f"/seasonality/{symbol}")
    if data and isinstance(data,dict):
        # format: {"month": {"01": {"avg":16.2, "up_prob":71.4}, ...}}
        if 'data' in data: data=data['data']
        return data
    # coba dari analysis
    analysis=get_analysis_full(symbol)
    if 'seasonality' in analysis: return analysis['seasonality']
    if 'seasonal' in analysis: return analysis['seasonal']
    return {}

def get_pattern_personality(symbol):
    analysis=get_analysis_full(symbol)
    patterns=analysis.get('patterns') or analysis.get('pattern_personality') or analysis.get('personality') or []
    if isinstance(patterns,dict): patterns=patterns.get('data') or patterns.get('patterns') or []
    return patterns if isinstance(patterns,list) else []

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

# ========== CHART GENERATOR V4 ==========
def generate_pro_chart(df,symbol="BBCA",timeframe="1d",sector_info="IHSG",output_filename="chart.png",extra_info=None):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        extra_info=extra_info or {}
        df=df.copy().ffill().bfill().sort_index()
        df['EMA13']=df['Close'].ewm(span=13).mean()
        df['EMA20']=df['Close'].ewm(span=20).mean()
        df['EMA50']=df['Close'].ewm(span=50).mean()
        df['EMA200']=df['Close'].ewm(span=200).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df,_=calculate_vsa_metrics(df)
        last_close,last_open,last_high,last_low=df['Close'].iloc[-1],df['Open'].iloc[-1],df['High'].iloc[-1],df['Low'].iloc[-1]
        last_vol,prev_close=df['Volume'].iloc[-1],df['Close'].iloc[-2] if len(df)>1 else df['Close'].iloc[-1]
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0)
        avg_price,vchg1=df['Close'].tail(20).mean(), (last_vol/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
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
        fig.text(0.01,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left')
        fig.text(0.5,0.96,"RAFANO V4 PRO MAX",color='white',fontsize=12,fontweight='bold',ha='center')
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

# ========== SCORING PRO MAX ==========
def calculate_full_score(symbol,hist_df,multi,seasonality,patterns,analysis,insider):
    score=0; reasons=[]; details={}
    # 1. Broker 35%
    status_d=multi.get('status_d','NEUTRAL'); buy_d=multi.get('buy_d',0); sell_d=multi.get('sell_d',0); net_d=multi.get('net_d',0)
    if status_d=='AKUM' and buy_d > sell_d*1.2:
        if buy_d>20e9: score+=35; reasons.append(f"AKUM {buy_d/1e9:.1f}B")
        elif buy_d>5e9: score+=25; reasons.append(f"AKUM {buy_d/1e9:.1f}B")
        else: score+=15; reasons.append(f"AKUM {buy_d/1e9:.0f}M")
    elif status_d=='DIST':
        score-=20; reasons.append(f"DIST {sell_d/1e9:.1f}B")
        details['heavy_dist']=True
    # 2. Seasonality 15%
    try:
        now_month=get_now_wib().month
        # seasonality bisa dict bulan -> {avg, up_prob}
        s_data=None
        if isinstance(seasonality,dict):
            # coba key "9" atau "Sep" atau list
            for k in [str(now_month), f"{now_month:02d}", datetime.datetime.now().strftime("%b")]:
                if k in seasonality:
                    s_data=seasonality[k]
                    break
            # kalau format list of months
            if not s_data and 'months' in seasonality:
                s_data=seasonality['months'].get(str(now_month))
        if s_data:
            s_avg=s_data.get('avg') or s_data.get('Avg') or 0
            s_prob=s_data.get('up_prob') or s_data.get('Up Prob') or 50
            if isinstance(s_avg,str): s_avg=float(s_avg.replace('%','').replace('+',''))
            details['season']=f"{s_avg:.1f}% prob {s_prob}%"
            if float(s_avg)>5 and float(s_prob)>60: score+=15; reasons.append(f"Season +{s_avg:.1f}%")
            elif float(s_avg)<-5: score-=10
    except Exception as e: print(f"season parse err {e}")
    # 3. Pattern Personality 20%
    best_pf=0; best_wr=0; best_name="-"
    try:
        for p in patterns[:10]:
            pf=float(p.get('pf') or p.get('PF') or 0)
            wr=float(p.get('wr') or p.get('WR') or p.get('win_rate') or 0)
            if pf>=1.4 and wr>=50 and pf>best_pf:
                best_pf=pf; best_wr=wr; best_name=p.get('name') or p.get('pattern') or p.get('pola') or "COMBO"
        if best_pf>=1.4:
            score+=20; reasons.append(f"{best_name} PF{best_pf:.2f} WR{best_wr:.0f}%")
            details['pattern']=f"{best_name} PF{best_pf:.2f} WR{best_wr:.0f}% {p.get('samples',0)} sampel"
    except: pass
    # 4. Teknikal 15%
    try:
        if hist_df is not None and len(hist_df)>=50:
            last=hist_df['Close'].iloc[-1]
            sma5=hist_df['Close'].rolling(5).mean().iloc[-1]
            sma20=hist_df['Close'].rolling(20).mean().iloc[-1]
            sma50=hist_df['Close'].rolling(50).mean().iloc[-1]
            rsi=calculate_rsi(hist_df['Close']).iloc[-1]
            vol_ratio=hist_df['Volume'].iloc[-1]/hist_df['Volume'].rolling(20).mean().iloc[-1]
            details['tech']=f"SMA5 {sma5:.0f} SMA20 {sma20:.0f} SMA50 {sma50:.0f} RSI {rsi:.0f} Vol {vol_ratio:.1f}x"
            if last>sma20: score+=5; reasons.append(">SMA20")
            if last>sma50: score+=5
            if 45<=rsi<=65: score+=5
            if vol_ratio>=2.0: score+=5; reasons.append(f"Vol {vol_ratio:.1f}x")
    except: pass
    # 5. Foreign + Insider 15%
    try:
        f_net=0
        if hist_df is not None and 'F_Buy' in hist_df.columns:
            f_net=(hist_df['F_Buy'].tail(5).sum() - hist_df['F_Sell'].tail(5).sum()) if 'F_Sell' in hist_df.columns else 0
        else:
            # dari analysis
            f_net=analysis.get('foreign_net') or analysis.get('net_foreign') or 0
        if f_net>0: score+=8; reasons.append(f"Foreign +{format_large_number(f_net)}")
        details['foreign']=f_net
        if insider and len(insider)>0:
            buy_ins=[x for x in insider if 'BUY' in str(x.get('aksi') or x.get('action') or '').upper()]
            if buy_ins: score+=7; reasons.append(f"Insider BUY {len(buy_ins)}x")
    except: pass

    if score>=85: label="VERY STRONG"
    elif score>=70: label="STRONG BUY"
    elif score>=50: label="WEAK BUY"
    else: label="NO SIGNAL"
    return score,label,reasons,details

# ========== TELEGRAM ==========
LAST_SENT_SIGNALS={}; COOLDOWN=3600; LAST_RESET=""

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
            requests.post(url,data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'},files={'photo':f},timeout=30)
    except Exception as e: print(f"Photo err {e}")

def process_chart_request_PRO(chat_id,stock_code,timeframe="1d",cache=None):
    send_reply(chat_id,f"📊 *V4 PRO MAX {stock_code.upper()} ({timeframe.upper()}) generating...*\n_Broker + Season + Pattern + Foreign + Teknikal_")
    df=get_history_pro(stock_code,limit=150,timeframe=timeframe)
    if df is None or len(df)<20:
        send_reply(chat_id,f"⚠ Data {stock_code} tidak ketemu TF {timeframe}"); return
    # 6 endpoint
    multi=get_broker_multi_tf_PRO(stock_code,df)
    seasonality=get_seasonality(stock_code)
    patterns=get_pattern_personality(stock_code)
    analysis=get_analysis_full(stock_code)
    insider=get_insider(stock_code)
    score,label,reasons,details=calculate_full_score(stock_code,df,multi,seasonality,patterns,analysis,insider)
    # Chart
    chart_file=f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    extra={"broker_net":multi.get('net_d',0),"multi_tf":multi}
    file_path=generate_pro_chart(df=df,symbol=stock_code.upper(),timeframe=timeframe,sector_info=f"{stock_code.upper()} | IHSG",output_filename=chart_file,extra_info=extra)
    # Build caption kayak web Arjum
    last_close=safe_int(df['Close'].iloc[-1])
    prev_close=df['Close'].iloc[-2] if len(df)>=2 else last_close
    chg=last_close-prev_close; chg_pct=(chg/prev_close*100) if prev_close else 0
    # Teknikal detail
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
    # Support resistance sederhana
    r2=last_close+atr*1.5; r1=last_close+atr*0.75; p=last_close; s1=last_close-atr*0.75; s2=last_close-atr*1.5
    # Volume
    vol=df['Volume'].iloc[-1]; val=(df['Value'].iloc[-1] if 'Value' in df.columns else vol*last_close)
    avg_vol_20=df['Volume'].rolling(20).mean().iloc[-1] if len(df)>=20 else vol
    vol_ratio=vol/avg_vol_20 if avg_vol_20 else 1
    # Foreign
    f_buy=df['F_Buy'].iloc[-1] if 'F_Buy' in df.columns else 0
    f_sell=df['F_Sell'].iloc[-1] if 'F_Sell' in df.columns else 0
    f_net=f_buy-f_sell if 'F_Buy' in df.columns else details.get('foreign',0)
    # Seasonality text
    season_text=details.get('season','-')
    pattern_text=details.get('pattern','-')
    # Broker text VALID
    top_akum_d=format_top_brokers_VALID(multi.get('top_akum_d',[]))
    top_dist_d=format_top_brokers_VALID(multi.get('top_dist_d',[]))
    top_akum_5d=format_top_brokers_VALID(multi.get('top_akum_5d',[]))
    top_dist_5d=format_top_brokers_VALID(multi.get('top_dist_5d',[]))

    # Build caption mirip web Arjum
    caption=f"""📊 *{stock_code.upper()} — Analisis {get_now_wib().strftime('%A, %d %b %Y')}*
_MD Entertainment Tbk._

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
Posisi: {'di atas SMA5 ✅' if last_close>sma5 else 'di bawah SMA5 ❌'}, {'di atas SMA20 ✅' if last_close>sma20 else 'di bawah SMA20 ❌'}

📊 *VOLUME*
Volume: {vol:,.0f} lembar ({vol/100:.0f} lot)
Nilai: Rp{val/1e9:.2f}B
Freq: {safe_int(df['Freq'].iloc[-1]) if 'Freq' in df.columns else '-'} transaksi
Rata2 20d: {avg_vol_20:,.0f} | Ratio: {vol_ratio:.1f}x {'🔥 Meningkat' if vol_ratio>=2 else '→ Stabil'}
Trend: → Stabil

🌍 *FOREIGN FLOW*
Net {'Beli' if f_net>0 else 'Jual'} {format_large_number(abs(f_net))} lembar
F Buy {format_large_number(f_buy)} | F Sell {format_large_number(f_sell)}
5-hari: Rata2 {format_large_number(details.get('foreign',0))}

🏦 *BROKER NETFLOW - VALID AKUM vs DIST*
Total {len(multi.get('brokers',[]))} broker | Net: {format_large_number(multi.get('net_d',0),True)}
Daily: {multi.get('status_d')} | Buy {format_large_number(multi.get('buy_d',0))} Sell {format_large_number(multi.get('sell_d',0))} Net {format_large_number(multi.get('net_d',0),True)}
  Top Akum: {top_akum_d}
  Top Dist: {top_dist_d}
5D: {multi.get('status_5d')} | Buy {format_large_number(multi.get('buy_5d',0))} Sell {format_large_number(multi.get('sell_5d',0))} Net {format_large_number(multi.get('net_5d',0),True)}
  Top Akum: {top_akum_5d} | Top Dist: {top_dist_5d}
💡 Heavy buyer: {top_akum_d} | Heavy seller: {top_dist_d}

🎯 *SIGNAL - V4 PRO MAX*
Score: {score}/100 — {'🟢 BULLISH' if score>=70 else '🟡 NEUTRAL' if score>=50 else '🔴 BEARISH'} — {label}
• {'🟢' if last_close>sma20 else '🔴'} {'di atas' if last_close>sma20 else 'di bawah'} SMA20
• {'🟢' if vol_ratio>=1.5 else '🔴'} Volume {'surge' if vol_ratio>=1.5 else 'tipis'} + harga {'naik' if chg>0 else 'turun'}
• {'🟢' if f_net>0 else '🔴'} Foreign {'beli' if f_net>0 else 'jual'}
• {'🟢' if multi.get('status_d')=='AKUM' else '🔴'} {multi.get('status_d')} {format_large_number(multi.get('net_d',0),True)}

🧬 *PERSONALITY HISTORIS & SEASONALITY*
Seasonality: {season_text} | Bulan ini Avg {season_text}
Pattern: {pattern_text}
Insider: {len(insider)} transaksi terakhir | {', '.join([f"{x.get('aksi') or x.get('action')} {x.get('broker') or ''}" for x in insider[:3]]) if insider else '-'}

📋 *REKOMENDASI - {label}*
{'🟢 AKUMULASI' if score>=70 else '🟡 WAIT' if score>=50 else '🔴 DISTRIBUSI / AVOID'}
Jika sudah punya: {'Tahan. Bisa average down di S1-S2' if score>=70 else 'Pertimbangkan take profit' if score>=50 else 'Jual di bounce R1'}
Jika belum punya: {'Buy on weakness di S1-S2' if score>=70 else 'Wait & see' if score>=50 else 'Jangan beli dulu'}
Entry: Rp{safe_int(s1)} - Rp{safe_int(p)}
Target: Rp{safe_int(r1)} (R1) / Rp{safe_int(r2)} (R2)
Stop Loss: Rp{safe_int(s2)} ({(s2-last_close)/last_close*100:.1f}%)
Reasons: {' | '.join(reasons[:4])}

*⚠ DISCLAIMER: BUKAN AJAKAN JUAL/BELI. Score dari 6 endpoint Arjum.*
*FILM example: 04 Sep 2026 940 +14.6% Vol 103M Value 92.9M Foreign +64K*
"""

    send_photo_reply(chat_id,file_path,caption=caption)
    if os.path.exists(file_path): os.remove(file_path)

# ========== MAIN LOOP V4 ==========
LAST_SIGNALS_CACHE={}
def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset=0
    print("🤖 V4 PRO MAX Listener Running...")
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
                        send_reply(chat_id,"🤖 *RAFANO V4 PRO MAX*\n6 Endpoint Arjum:\n`/c <KODE> [TF]` - Chart + Analisa AI lengkap kayak web\n`/b <KODE>` - Broker Flow VALID B.val vs S.val\n`/s <KODE>` - Seasonality\n`/p <KODE>` - Pattern Personality\n`/scan` - Scan V4 dengan 6 filter\n")
                    elif first in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            threading.Thread(target=process_chart_request_PRO,args=(chat_id,parts[1].upper(),parts[2] if len(parts)>=3 else "1d",LAST_SIGNALS_CACHE)).start()
                        else: send_reply(chat_id,"⚠ `/c FILM` atau `/c BBCA 5m`")
                    elif first in ["/b","/broker"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def bd(tchat,symbol):
                                multi=get_broker_multi_tf_PRO(symbol)
                                msg=f"🏦 *BROKER FLOW PRO {symbol}* {get_now_wib().strftime('%d %b %H:%M')}\n"
                                msg+=f"Daily: {multi['status_d']} Buy {format_large_number(multi['buy_d'])} Sell {format_large_number(multi['sell_d'])} Net {format_large_number(multi['net_d'],True)}\n  Akum: {format_top_brokers_VALID(multi['top_akum_d'])}\n  Dist: {format_top_brokers_VALID(multi['top_dist_d'])}\n\n"
                                msg+=f"5D: {multi['status_5d']} Net {format_large_number(multi['net_5d'],True)}\n  Akum: {format_top_brokers_VALID(multi['top_akum_5d'])}\n  Dist: {format_top_brokers_VALID(multi['top_dist_5d'])}\n\n"
                                msg+=f"20D: {multi['status_20d']} Net {format_large_number(multi['net_20d'],True)}\n  Akum: {format_top_brokers_VALID(multi['top_akum_20d'])}\n  Dist: {format_top_brokers_VALID(multi['top_dist_20d'])}\n"
                                send_reply(tchat,msg)
                            threading.Thread(target=bd,args=(chat_id,sym)).start()
                    elif first in ["/s","/season"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def sd(tchat,symbol):
                                seas=get_seasonality(symbol)
                                msg=f"📅 *Seasonality {symbol}*\n{json.dumps(seas,indent=2)[:3000]}"
                                send_reply(tchat,msg)
                            threading.Thread(target=sd,args=(chat_id,sym)).start()
                    elif first in ["/p","/pattern"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def pd2(tchat,symbol):
                                pats=get_pattern_personality(symbol)
                                msg=f"🧬 *Pattern {symbol}* ({len(pats)} pola)\n"
                                for p in pats[:10]:
                                    msg+=f"• {p.get('name') or p.get('pattern')} PF {p.get('pf')} WR {p.get('wr')}% {p.get('samples')} sampel MFE {p.get('mfe')}%\n"
                                send_reply(tchat,msg)
                            threading.Thread(target=pd2,args=(chat_id,sym)).start()
                    elif first in ["/scan"]:
                        send_reply(chat_id,"🔍 *V4 PRO MAX Scanning 6 endpoint...*")
                        def scan_pro(tchat):
                            global LAST_SIGNALS_CACHE
                            # ambil screener
                            data=arjum_get("/screener/latest") or []
                            candidates=[]
                            if isinstance(data,dict) and 'rows' in data:
                                for r in data['rows'][:30]:
                                    code=r.get('stock_code') or r.get('symbol')
                                    if code: candidates.append(code.replace(".JK","").upper())
                            if not candidates: candidates=["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","FILM","ADRO","ANTM","MDKA"]
                            detected=[]
                            def proc(sym):
                                try:
                                    df=get_history_pro(sym,limit=100,timeframe="1d")
                                    multi=get_broker_multi_tf_PRO(sym,df)
                                    seas=get_seasonality(sym)
                                    pats=get_pattern_personality(sym)
                                    analysis=get_analysis_full(sym)
                                    insider=get_insider(sym)
                                    score,label,reasons,details=calculate_full_score(sym,df,multi,seas,pats,analysis,insider)
                                    if score>=50:
                                        return {"symbol":sym,"close":safe_int(df['Close'].iloc[-1]) if df is not None else 0,"score":score,"label":label,"multi_tf":multi,"reasons":reasons,"details":details,"history_df":df}
                                except Exception as e:
                                    print(f"scan {sym} err {e}")
                                return None
                            with ThreadPoolExecutor(max_workers=10) as ex:
                                futs=[ex.submit(proc,s) for s in candidates]
                                for f in futs:
                                    r=f.result()
                                    if r: detected.append(r)
                            detected.sort(key=lambda x: x['score'],reverse=True)
                            LAST_SIGNALS_CACHE={x['symbol']:x for x in detected}
                            msg=f"*RAFANO V4 PRO MAX SCAN* {get_now_wib().strftime('%d %b %H:%M')}\nTotal {len(detected)} | Filter 6 endpoint\n\n"
                            kb=[]
                            for idx,item in enumerate(detected[:15],1):
                                multi=item['multi_tf']
                                msg+=f"{idx}. {'🟢' if multi['status_d']=='AKUM' else '🔴'} *{item['symbol']}* {item['close']} Score {item['score']}% {item['label']}\n   {multi['status_d']} Buy {format_large_number(multi['buy_d'])} vs Dist {format_large_number(multi['sell_d'])} | Top Akum {format_top_brokers_VALID(multi['top_akum_d'])} vs Dist {format_top_brokers_VALID(multi['top_dist_d'])}\n   {item['details'].get('season','')} | {item['details'].get('pattern','')[:60]}\n   {' | '.join(item['reasons'][:3])}\n\n"
                                kb.append([{"text":f"Chart {item['symbol']} PRO MAX","callback_data":f"chart_{item['symbol']}_1d"}])
                            send_reply(tchat,msg,reply_markup={"inline_keyboard":kb})
                        threading.Thread(target=scan_pro,args=(chat_id,)).start()
        except Exception as e:
            print(f"Listener err {e}"); time.sleep(3)

if __name__=="__main__":
    print("🔥 RAFANO V4 PRO MAX STARTING...")
    telegram_bot_listener()
