
import sqlite3, os, pandas as pd, numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt, matplotlib.gridspec as gridspec
from matplotlib.patches import Rectangle
from datetime import datetime
DB_PATH="rafano.db"
CHART_DIR="charts_v417"
os.makedirs(CHART_DIR, exist_ok=True)

def get_df(symbol, limit=200):
    conn=sqlite3.connect(DB_PATH)
    df=pd.read_sql_query(f"SELECT date,open,high,low,close,volume FROM ohlcv WHERE symbol='{symbol}' ORDER BY date ASC", conn)
    if df.empty or len(df)<60:
        conn.close()
        return None,None,None
    df['date']=pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df=df.tail(limit).copy()
    df['EMA13']=df['close'].ewm(span=13).mean()
    df['EMA20']=df['close'].ewm(span=20).mean()
    df['EMA50']=df['close'].ewm(span=50).mean()
    df['EMA200']=df['close'].ewm(span=200).mean()
    df['BB_MA']=df['close'].rolling(20).mean()
    df['BB_STD']=df['close'].rolling(20).std()
    df['BB_UP']=df['BB_MA']+2*df['BB_STD']
    df['BB_LOW']=df['BB_MA']-2*df['BB_STD']
    df['VOL_MA20']=df['volume'].rolling(20).mean()
    df['VCHG_1']=df['volume']/df['volume'].shift(1)
    df['VCHG_5']=df['volume']/df['volume'].rolling(5).mean()
    df['buy_vol']=np.where(df['close']>=df['open'], df['volume']*0.6, df['volume']*0.4)
    bandar=pd.read_sql_query(f"SELECT * FROM bandar_daily WHERE symbol='{symbol}' ORDER BY date DESC LIMIT 10", conn)
    broker=pd.read_sql_query(f"SELECT * FROM broker_summary WHERE symbol='{symbol}' ORDER BY date DESC LIMIT 30", conn)
    conn.close()
    return df,bandar,broker

def detect_breakout_ma20(df):
    if len(df)<25: return False
    last=df.iloc[-1]
    prev=df.iloc[-2]
    return (prev['close']<prev['EMA20']) and (last['close']>last['EMA20']) and (last['volume']>last['VOL_MA20']*1.2)

def scan_top10():
    conn=sqlite3.connect(DB_PATH)
    symbols=[r[0] for r in conn.execute("SELECT DISTINCT symbol FROM ohlcv").fetchall()]
    conn.close()
    print(f"Scanning {len(symbols)} saham...")
    cands=[]
    for sym in symbols:
        try:
            df,bandar,broker=get_df(sym,200)
            if df is None: continue
            if not detect_breakout_ma20(df): continue
            if bandar is None or bandar.empty: continue
            b_last=bandar.iloc[0]
            if "ACCUM" not in str(b_last['accum_type']).upper(): continue
            if float(b_last['foreign_net'] or 0) < 1e9:
                if bandar['foreign_net'].head(5).sum() < 2e9: continue
            cands.append({"symbol":sym,"close":df.iloc[-1]['close'],"chg":(df.iloc[-1]['close']-df.iloc[-2]['close'])/df.iloc[-2]['close']*100,"volume":df.iloc[-1]['volume'],"vchg_1":df.iloc[-1]['VCHG_1'],"vchg_5":df.iloc[-1]['VCHG_5'],"ema13":df.iloc[-1]['EMA13'],"ema20":df.iloc[-1]['EMA20'],"ema50":df.iloc[-1]['EMA50'],"ema200":df.iloc[-1]['EMA200'],"foreign_str":b_last['foreign_str'],"accum_type":b_last['accum_type'],"net":float(b_last['foreign_net'] or 0),"score":float(b_last['foreign_net'] or 0),"df":df,"bandar":bandar,"broker":broker})
        except: continue
    cands=sorted(cands, key=lambda x: x['score'], reverse=True)[:10]
    print(f"TOP {len(cands)} BIG ACCUM + BO MA20")
    for c in cands:
        print(f"{c['symbol']} {c['close']:.0f} {c['chg']:+.2f}% {c['foreign_str']} {c['accum_type']} Vol {c['vchg_1']:.1f}x")
    return cands

def plot_v417(c):
    sym=c['symbol']
    df=c['df']
    fig=plt.figure(figsize=(16,9), facecolor='black')
    gs=gridspec.GridSpec(4,1,height_ratios=[3.5,0.8,0.8,0.8], hspace=0.05)
    ax1=plt.subplot(gs[0], facecolor='black')
    ax2=plt.subplot(gs[1], facecolor='black', sharex=ax1)
    ax3=plt.subplot(gs[2], facecolor='black', sharex=ax1)
    ax4=plt.subplot(gs[3], facecolor='black', sharex=ax1)
    x=np.arange(len(df))
    for i in range(len(df)):
        o,h,l,cl=df['open'].iloc[i],df['high'].iloc[i],df['low'].iloc[i],df['close'].iloc[i]
        color='#00ff00' if cl>=o else '#ff3333'
        ax1.plot([i,i],[l,h],color=color,linewidth=0.8)
        ax1.add_patch(Rectangle((i-0.3,min(o,cl)),0.6,max(abs(cl-o),1),facecolor=color,edgecolor=color))
    ax1.plot(x,df['EMA13'],color='white',linewidth=0.8)
    ax1.plot(x,df['EMA20'],color='yellow',linewidth=1.0)
    ax1.plot(x,df['EMA50'],color='red',linewidth=1.0)
    ax1.plot(x,df['EMA200'],color='#aa00ff',linewidth=1.2)
    ax1.plot(x,df['BB_UP'],color='#444488',linestyle='--',linewidth=0.7,alpha=0.6)
    ax1.plot(x,df['BB_LOW'],color='#444488',linestyle='--',linewidth=0.7,alpha=0.6)
    ax1.text(len(df)-0.5,df['close'].iloc[-1],f" {df['close'].iloc[-1]:.0f} ",color='black',bbox=dict(facecolor='white'),fontsize=8)
    fig.suptitle("RAFANO V4.17 ANTI-429",color='white',fontsize=14,fontweight='bold',y=0.98)
    fig.text(0.005,0.96,f"{sym} : {df['close'].iloc[-1]:.0f} ({c['chg']:+.2f}%)",color='yellow',fontsize=14,fontweight='bold')
    fig.text(0.995,0.96,f"Daily | {datetime.now().strftime('%d %b %Y')} | {c['accum_type']} {c['foreign_str']}",color='#ffcc00',fontsize=9,ha='right')
    ax2.bar(x,df['volume']/1e6,color=['#00aa00' if df['close'].iloc[i]>=df['open'].iloc[i] else '#aa0000' for i in range(len(df))],width=0.8)
    ax2.plot(x,df['VOL_MA20']/1e6,color='white',linewidth=0.7)
    if c['bandar'] is not None and not c['bandar'].empty:
        vals=c['bandar'].iloc[::-1]['foreign_net'].values/1e9
        ax3.bar(range(len(vals)),vals,color=['#00ffff' if v>=0 else '#ff5555' for v in vals],width=0.6)
    ax3.set_title(f"NBSA {c['foreign_str']}",loc='left',color='white',fontsize=7)
    if c['broker'] is not None and not c['broker'].empty:
        try:
            mm=c['broker'].groupby('date')['nval'].sum().tail(len(df)).values/1e9
            ax4.bar(range(len(mm)),mm,color=['#bbbbbb' if v>=0 else '#777777' for v in mm],width=0.8)
        except: pass
    ax4.set_title("Market Maker",loc='left',color='white',fontsize=7)
    plt.tight_layout(rect=[0,0,1,0.95])
    save_path=os.path.join(CHART_DIR,f"{sym}_V417.png")
    plt.savefig(save_path,dpi=180,facecolor='black',bbox_inches='tight')
    plt.close()
    print(f"Chart {save_path}")
    return save_path

if __name__=="__main__":
    # test download DB
    import os
    if not os.path.exists(DB_PATH):
        os.system("wget -q https://raw.githubusercontent.com/sesriko/rafano-v3/main/rafano.db -O rafano.db")
    top10=scan_top10()
    for c in top10:
        plot_v417(c)
