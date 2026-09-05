"""
RAFANO V3 - FINAL: Arjum Pro + Pro Chart Generator
Gabungan:
- Screener/latest + broker-accumulation + broker-summary (REAL DATA)
- Chart Generator V1 (candlestick custom, EMA 8/21/50/125, dashboard kiri-kanan)
- Telegram Listener + Auto Screener Loop
"""
import os
import time
import logging
import datetime
import threading
import requests
import pytz
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")
ARJUM_API_KEY = os.getenv("ARJUM_API_KEY")

ARJUM_BASE = "https://stock.arjum.com/api"
HEADERS_ARJUM = {"X-API-Key": ARJUM_API_KEY, "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)

# ========== HELPERS UMUM ==========
def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val):
            return default
        return int(val)
    except:
        return default

def format_large_number(val, show_sign=False):
    if pd.isna(val) or val == 0:
        return "0"
    abs_val = abs(val)
    sign = "+" if (show_sign and val > 0) else ("-" if val < 0 else "")
    if abs_val >= 1_000_000_000:
        return f"{sign}{abs_val / 1_000_000_000:.2f}B"
    elif abs_val >= 1_000_000:
        return f"{sign}{abs_val / 1_000_000:,.0f}M"
    elif abs_val >= 1_000:
        return f"{sign}{abs_val / 1_000:,.0f}K"
    else:
        return f"{sign}{val:,.0f}"

def round_to_ihsg_fraction(price):
    if pd.isna(price) or price <= 0:
        return 0
    price = float(price)
    if price < 200:
        tick = 1
    elif price < 500:
        tick = 2
    elif price < 2000:
        tick = 5
    elif price < 5000:
        tick = 10
    else:
        tick = 25
    return int(round(price / tick) * tick)

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 0.00001)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50)

def calculate_atr(df, period=14):
    high, low, close = df['High'], df['Low'], df['Close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def calculate_vsa_metrics(df):
    price_range = (df['High'] - df['Low']).replace(0, 0.1)
    body_move = df['Close'] - df['Open']
    buy_ratio = np.where(
        price_range <= 0.1, 0.50,
        np.where(df['Close'] >= df['Open'], 
                 0.55 + (body_move / price_range) * 0.4, 
                 0.45 + (body_move / price_range) * 0.4)
    )
    buy_ratio = np.clip(buy_ratio, 0.05, 0.95)
    df['Vol_Buy'] = df['Volume'] * buy_ratio
    df['Vol_Sell'] = df['Volume'] - df['Vol_Buy']
    df['Net_Vol_VSA'] = df['Vol_Buy'] - df['Vol_Sell']
    df['Net_Val_VSA'] = df['Net_Vol_VSA'] * df['Close']
    return df, buy_ratio

def calculate_buy_signal_strength(df):
    if len(df) < 20:
        return 0, "NO DATA"
    last_row = df.iloc[-1]
    last_close, last_open, last_vol = last_row['Close'], last_row['Open'], last_row['Volume']
    avg_vol_v1 = last_row.get('V1', last_row['Volume'])
    df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
    ema_50 = df['EMA50'].iloc[-1]
    df, buy_ratios = calculate_vsa_metrics(df)
    last_buy_ratio = buy_ratios[-1]
    net_5d_val = df['Net_Val_VSA'].tail(5).sum()
    score = 0
    if last_close > ema_50: score += 25
    vol_multiple = last_vol / avg_vol_v1 if avg_vol_v1 > 0 else 0
    if vol_multiple >= 2.5: score += 25
    elif vol_multiple >= 2.0: score += 20
    elif vol_multiple >= 1.8: score += 15
    if last_buy_ratio >= 0.75: score += 20
    elif last_buy_ratio >= 0.65: score += 15
    elif last_buy_ratio >= 0.55: score += 10
    if net_5d_val > 0: score += 20
    if last_close > last_open: score += 10
    if score >= 85: label = "VERY STRONG"
    elif score >= 70: label = "STRONG BUY"
    elif score >= 50: label = "WEAK BUY"
    else: label = "NO SIGNAL"
    return score, label

def is_market_open():
    now = get_now_wib()
    weekday = now.weekday()
    if weekday >= 5:
        return False
    current_time = now.time()
    if weekday == 4:
        s1_start, s1_end = datetime.time(9, 0), datetime.time(11, 30)
        s2_start, s2_end = datetime.time(14, 0), datetime.time(15, 50)
    else:
        s1_start, s1_end = datetime.time(9, 0), datetime.time(12, 0)
        s2_start, s2_end = datetime.time(13, 30), datetime.time(15, 50)
    return (s1_start <= current_time <= s1_end) or (s2_start <= current_time <= s2_end)

# ========== ARJUM PRO WRAPPER ==========
def arjum_get(path, params=None):
    url = f"{ARJUM_BASE}{path}"
    try:
        r = requests.get(url, headers=HEADERS_ARJUM, params=params, timeout=8)
        if r.status_code == 200:
            return r.json()
        return None
    except:
        return None

def get_screener_latest():
    data = arjum_get("/screener/latest")
    if not data:
        return []
    if isinstance(data, dict):
        return data.get('data') or data.get('results') or data.get('stocks') or []
    return data if isinstance(data, list) else []

def get_broker_accumulation(symbol, top=3):
    data = arjum_get(f"/broker-accumulation/{symbol}", params={"top": top})
    if not data:
        return 0, []
    if isinstance(data, dict):
        accum = data.get('total_accum') or data.get('accumulation') or data.get('net_value') or data.get('total') or 0
        brokers = data.get('brokers') or data.get('data') or []
        return accum, brokers
    return 0, []

def get_broker_summary(symbol):
    params = {"net": "true", "broker_limit": 5, "level_limit": 5, "all_data": "false", "flow": "all"}
    data = arjum_get(f"/broker-summary/{symbol}", params=params)
    if not data:
        return 0, "NEUTRAL", []
    try:
        if isinstance(data, dict):
            brokers = data.get('brokers') or data.get('data') or []
            net_value = data.get('net_buy') or data.get('net_value') or 0
            if brokers:
                top3_net = sum([b.get('net',0) or b.get('net_value',0) or b.get('value',0) for b in brokers[:3]])
                net_value = top3_net if top3_net !=0 else net_value
            status = "ACCUM" if net_value > 0 else "DISTRIB" if net_value <0 else "NEUTRAL"
            return net_value, status, brokers
    except:
        pass
    return 0, "NEUTRAL", []

def get_analysis(symbol):
    data = arjum_get(f"/analysis/{symbol}")
    return data if isinstance(data, dict) else {}

def get_history_pro(symbol, limit=150):
    data = arjum_get(f"/history/{symbol}", params={"limit": limit, "frame": "daily"})
    if not data:
        return None
    rows = []
    if isinstance(data, dict):
        rows = data.get('data') or data.get('history') or data.get('results') or []
    elif isinstance(data, list):
        rows = data
    if not rows:
        return None
    try:
        df = pd.DataFrame(rows)
        # mapping
        rename_map = {}
        for c in df.columns:
            cl = str(c).lower()
            if cl in ['o','open']: rename_map[c]='Open'
            elif cl in ['h','high']: rename_map[c]='High'
            elif cl in ['l','low']: rename_map[c]='Low'
            elif cl in ['c','close','close_price']: rename_map[c]='Close'
            elif cl in ['v','volume','vol']: rename_map[c]='Volume'
            elif cl in ['date','time','t','datetime']: rename_map[c]='Date'
        df.rename(columns=rename_map, inplace=True)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        df = df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['Close'])
        return df
    except Exception as e:
        print(f"History parse {symbol}: {e}")
        return None

# ========== CHART GENERATOR V3 (INTEGRATED REAL DATA) ==========
def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    """
    extra_info: dict dari broker -> {'accum_value': 25B, 'broker_net': 12B, 'score': 88}
    """
    try:
        extra_info = extra_info or {}
        tf_clean = timeframe.lower()
        is_intraday = tf_clean in ['1m','5m','15m','30m','1h']

        df = df.copy()
        df = df.ffill().bfill()
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index()

        last_close = df['Close'].iloc[-1]
        last_open = df['Open'].iloc[-1]
        last_high = df['High'].iloc[-1]
        last_low = df['Low'].iloc[-1]
        last_vol = df['Volume'].iloc[-1]

        df['EMA8'] = df['Close'].ewm(span=8, adjust=False).mean()
        df['EMA21'] = df['Close'].ewm(span=21, adjust=False).mean()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA125'] = df['Close'].ewm(span=125, adjust=False).mean()
        df['RSI14'] = calculate_rsi(df['Close'], period=14)
        df['ATR'] = calculate_atr(df, period=14)
        df['Pivot_High'] = df['High'].rolling(window=12, min_periods=1).max()
        df['Pivot_Low'] = df['Low'].rolling(window=12, min_periods=1).min()
        df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
        df, buy_ratios = calculate_vsa_metrics(df)
        net_5d_val = df['Net_Val_VSA'].tail(5).sum()
        net_val_today = df['Net_Val_VSA'].iloc[-1]
        last_rsi = round(df['RSI14'].iloc[-1], 2)
        signal_score, score_lbl = calculate_buy_signal_strength(df)

        # Override score jika ada extra_info dari Arjum Pro
        real_score = extra_info.get('score', signal_score)
        real_label = extra_info.get('score_label', score_lbl)

        if 'MM' not in df.columns:
            df['MM'] = (df['Close'] - df['EMA50']) / df['EMA50'] * 1000 + np.sin(np.linspace(0, 10, len(df))) * 15 - 10.9

        plt.style.use('dark_background')
        fig = plt.figure(figsize=(18, 10), dpi=300, facecolor='#000000')
        gs = gridspec.GridSpec(4, 1, height_ratios=[4, 0.25, 1.2, 0.8], hspace=0.04)
        ax_main = fig.add_subplot(gs[0])
        ax_bar = fig.add_subplot(gs[1], sharex=ax_main)
        ax_vol = fig.add_subplot(gs[2], sharex=ax_main)
        ax_mm = fig.add_subplot(gs[3], sharex=ax_main)
        fig.subplots_adjust(left=0.065, right=0.93, top=0.94, bottom=0.06)

        color_up, color_down, color_neutral = '#00ff00', '#ff0000', '#888888'
        for ax in [ax_main, ax_bar, ax_vol, ax_mm]:
            ax.set_facecolor('#000000')
            ax.grid(True, color='#1e1e1e', linestyle=':', linewidth=0.6)
            ax.tick_params(colors='white', labelsize=10)
            ax.yaxis.tick_right()

        x_indices = np.arange(len(df))
        for i in range(len(df)):
            open_p, high_p, low_p, close_p = df['Open'].iloc[i], df['High'].iloc[i], df['Low'].iloc[i], df['Close'].iloc[i]
            if close_p >= open_p:
                body_top, body_bottom = close_p, open_p
                body_height = max(0.2, close_p - open_p)
                ax_main.plot([i, i], [high_p, body_top], color=color_up, linewidth=1.2)
                ax_main.plot([i, i], [low_p, body_bottom], color=color_up, linewidth=1.2)
                rect = patches.Rectangle((i - 0.35, body_bottom), 0.7, body_height, linewidth=1.2, edgecolor=color_up, facecolor='none')
                ax_main.add_patch(rect)
            else:
                body_top, body_bottom = open_p, close_p
                body_height = max(0.2, open_p - close_p)
                ax_main.plot([i, i], [low_p, high_p], color=color_down, linewidth=1.2)
                rect = patches.Rectangle((i - 0.35, body_bottom), 0.7, body_height, linewidth=1.2, edgecolor=color_down, facecolor=color_down)
                ax_main.add_patch(rect)

        ax_main.plot(x_indices, df['EMA8'], color='#00ffff', linewidth=0.8, label='EMA 8')
        ax_main.plot(x_indices, df['EMA21'], color='#ff00ff', linewidth=1.0, label='EMA 21')
        ax_main.plot(x_indices, df['EMA50'], color='#ffff00', linewidth=1.1, label='EMA 50')
        ax_main.plot(x_indices, df['EMA125'], color='#ffffff', linewidth=1.3, label='EMA 125')
        ax_main.step(x_indices, df['Pivot_High'], where='mid', color='#555555', linestyle='--', linewidth=1.0)
        ax_main.step(x_indices, df['Pivot_Low'], where='mid', color='#444444', linestyle=':', linewidth=1.0)

        latest_setup = {"status": "WAIT & SEE", "entry": 0, "tp1": 0, "tp2": 0, "danger": 0}
        last_signal_idx = -10
        for i in range(5, len(df)):
            c_price, o_price = df['Close'].iloc[i], df['Open'].iloc[i]
            vol_curr, vol_avg = df['Volume'].iloc[i], df['V1'].iloc[i]
            ema_50 = df['EMA50'].iloc[i]
            b_ratio = buy_ratios[i]
            atr_val = df['ATR'].iloc[i]
            rsi_val = df['RSI14'].iloc[i]
            net_5d_val_i = df['Net_Val_VSA'].iloc[max(0, i-4):i+1].sum()
            is_bandar_accum_i = net_5d_val_i > 0
            is_accum_trend = (c_price > 50) and (c_price > ema_50) and (rsi_val <= 75) and (vol_curr >= vol_avg * 2.0) and is_bandar_accum_i and (b_ratio > 0.65) and (c_price > o_price)
            if is_accum_trend and (i - last_signal_idx >= 4):
                buy_price = round_to_ihsg_fraction(c_price)
                tp1_price = round_to_ihsg_fraction(buy_price * 1.035)
                tp2_price = round_to_ihsg_fraction(buy_price + (1.5 * atr_val))
                swing_low = df['Pivot_Low'].iloc[i]
                danger_price = round_to_ihsg_fraction(min(swing_low, buy_price - (1.0 * atr_val)))
                ax_main.plot(i, df['Low'].iloc[i] * 0.985, marker='^', color='#00ff00', markersize=7, zorder=6)
                if i >= len(df) - 3:
                    ax_main.text(i, df['Low'].iloc[i] * 0.96, f"BUY @ {buy_price}", color='#00ff00', fontsize=8, fontweight='bold', ha='center',
                                 bbox=dict(boxstyle='round,pad=0.2', facecolor='#000000', alpha=0.75, edgecolor='#00ff00'))
                latest_setup = {"status": "BUY ACCUMULATION", "entry": buy_price, "tp1": tp1_price, "tp2": tp2_price, "danger": danger_price}
                last_signal_idx = i

        max_high = df['High'].max()
        min_low = df['Low'].min()
        ax_main.set_ylim(min_low * 0.95, max_high * 1.25)
        ax_main.set_xlim(-0.5, len(df) - 0.5)

        status_color = "#00ff00" if latest_setup["status"] == "BUY ACCUMULATION" else "#ffff00"
        entry_val = latest_setup['entry'] if latest_setup['entry'] > 0 else last_close
        tp1_val = latest_setup['tp1'] if latest_setup['tp1'] > 0 else round_to_ihsg_fraction(last_close*1.035)
        tp2_val = latest_setup['tp2'] if latest_setup['tp2'] > 0 else round_to_ihsg_fraction(last_close*1.07)
        sl_val = latest_setup['danger'] if latest_setup['danger'] > 0 else round_to_ihsg_fraction(last_close*0.95)

        # REAL BROKER DATA
        real_accum = extra_info.get('accum_value', 0)
        real_net = extra_info.get('broker_net', 0)
        
        dashboard_text = (
            f"RAFANO V3 - REAL DATA\n"
            f"-----------------------\n"
            f"O:{safe_int(last_open)} H:{safe_int(last_high)} L:{safe_int(last_low)} C:{safe_int(last_close)}\n"
            f"VOL: {format_large_number(last_vol)}\n"
            f"-----------------------\n"
            f"SCORE VSA: {signal_score}% ({score_lbl})\n"
            f"SCORE REAL: {real_score}% ({real_label})\n"
            f"STATUS : {latest_setup['status']}\n"
            f"ENTRY  : {entry_val} | TP1:{tp1_val}\n"
            f"TP2:{tp2_val} | SL:{sl_val}\n"
            f"-----------------------\n"
            f"AKUM 3B: {format_large_number(real_accum, True)}\n"
            f"NET BROK: {format_large_number(real_net, True)}"
        )
        ax_main.text(0.01, 0.96, dashboard_text, transform=ax_main.transAxes, verticalalignment='top', horizontalalignment='left',
                     fontfamily='monospace', fontsize=8, color=status_color,
                     bbox=dict(boxstyle='round,pad=0.3', facecolor='#000000', alpha=0.8, edgecolor='#333333'))

        stat_text_right = (
            f"RSI (14)   : {last_rsi}\n"
            f"BANDAR 1W  : {'ACCUM' if net_5d_val > 0 else 'DISTRIB'}\n"
            f"VAL 1D     : {format_large_number(net_val_today, show_sign=True)}\n"
            f"VSA BUY    : {safe_int(buy_ratios[-1]*100)}%\n"
            f"REAL STAT  : {extra_info.get('broker_status','-')}"
        )
        ax_main.text(0.985, 0.96, stat_text_right, transform=ax_main.transAxes, verticalalignment='top', horizontalalignment='right',
                     fontfamily='monospace', fontsize=8.5, color='#00ffff',
                     bbox=dict(boxstyle='round,pad=0.3', facecolor='#000000', alpha=0.75, edgecolor='#333333'))

        latest_ph, latest_pl = df['Pivot_High'].iloc[-1], df['Pivot_Low'].iloc[-1]
        ax_main.text(1.01, latest_ph, f" {safe_int(latest_ph)} ", transform=ax_main.get_yaxis_transform(),
                     color='black', backgroundcolor='#ffff00', fontsize=8.5, fontweight='bold', va='center', ha='left', clip_on=False)
        ax_main.text(1.01, latest_pl, f" {safe_int(latest_pl)} ", transform=ax_main.get_yaxis_transform(),
                     color='black', backgroundcolor='#00ffff', fontsize=8.5, fontweight='bold', va='center', ha='left', clip_on=False)

        fig.text(0.01, 0.975, f"{symbol}", color='#ffffff', fontsize=16, fontweight='bold')
        fig.text(0.45, 0.975, "RAFANO TRADER V3 PRO", color='#ffffff', fontsize=15, fontweight='bold')
        last_date_str = get_now_wib().strftime('%d %b %Y')
        fig.text(0.88, 0.975, f"{tf_clean.upper()} {last_date_str}", color='#ffff00', fontsize=10, fontweight='bold', ha='right')
        sub_header = f"{sector_info} | Akum:{format_large_number(real_accum, True)} Net:{format_large_number(real_net, True)}"
        fig.text(0.01, 0.945, sub_header, color='#888888', fontsize=8.5)

        for i in range(len(df)):
            c, o = df['Close'].iloc[i], df['Open'].iloc[i]
            bar_color = '#888888' if abs(c - o) / max(1, o) < 0.0005 else ('#00ff00' if c >= o else '#ff0000')
            ax_bar.add_patch(patches.Rectangle((i - 0.5, 0), 1.0, 1.0, color=bar_color))
        ax_bar.set_ylim(0, 1)
        ax_bar.axis('off')

        ax_vol.bar(x_indices, df['Vol_Sell'], color='#ff0000', width=0.8, align='center')
        ax_vol.bar(x_indices, df['Vol_Buy'], bottom=df['Vol_Sell'], color='#00ff00', width=0.8, align='center')
        ax_vol.plot(x_indices, df['V1'], color='#ffffff', linewidth=1.0, linestyle='-')
        net_val_str = format_large_number(net_val_today, show_sign=True)
        net_5d_val_str = format_large_number(net_5d_val, show_sign=True)
        last_buy_pct = safe_int(buy_ratios[-1] * 100)
        vol_text = (f"Buy: {last_buy_pct}% Sell: {100 - last_buy_pct}% Val 1D: {net_val_str} Val 5D: {net_5d_val_str} | REAL Akum:{format_large_number(real_accum, True)}")
        ax_vol.text(0.01, 0.85, vol_text, transform=ax_vol.transAxes, color='#00ffff', fontsize=8, fontweight='bold')
        ax_vol.set_ylim(0, df['Volume'].max() * 1.35)

        mm_colors = ['#ffff00' if v >= 0 else '#555555' for v in df['MM']]
        ax_mm.bar(x_indices, df['MM'], color=mm_colors, width=0.4)
        ax_mm.text(0.01, 0.80, "Market Maker", transform=ax_mm.transAxes, color='#ffff00', fontsize=8, fontweight='bold')
        ax_mm.text(1.01, df['MM'].iloc[-1], f" {df['MM'].iloc[-1]:.2f} ", transform=ax_mm.get_yaxis_transform(),
                   color='black', backgroundcolor='#ffff00', fontsize=8.5, fontweight='bold', va='center', ha='left', clip_on=False)

        step = max(1, len(df) // 8)
        ax_mm.set_xticks(x_indices[::step])
        if isinstance(df.index, pd.DatetimeIndex):
            fmt = "%H:%M" if is_intraday else "%b %Y"
            ax_mm.set_xticklabels([df.index[k].strftime(fmt) for k in range(0, len(df), step)])

        plt.setp(ax_main.get_xticklabels(), visible=False)
        plt.setp(ax_vol.get_xticklabels(), visible=False)

        plt.savefig(output_filename, dpi=300, bbox_inches='tight', pad_inches=0.05, facecolor=fig.get_facecolor(), format='png')
        return output_filename
    finally:
        plt.clf()
        plt.close('all')

# ========== TELEGRAM & SCANNER V3 ==========
LAST_SENT_SIGNALS = {}
COOLDOWN_SECONDS = 3600
LAST_RESET_DATE = ""

def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE, LAST_SENT_SIGNALS
    current_time = time.time()
    today_str = get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE != today_str:
        LAST_SENT_SIGNALS.clear()
        LAST_RESET_DATE = today_str
    filtered = []
    for sig in signals:
        sym = sig['symbol']
        last_sent = LAST_SENT_SIGNALS.get(sym, 0)
        if (current_time - last_sent) >= COOLDOWN_SECONDS:
            filtered.append(sig)
            LAST_SENT_SIGNALS[sym] = current_time
    return filtered

def calculate_score_v2(symbol, history_df, accum_value, broker_net, analysis_data):
    score = 0
    reasons = []
    score += 30
    reasons.append("Screener")
    if accum_value > 20_000_000_000:
        score += 30
        reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value > 5_000_000_000:
        score += 20
        reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value > 0:
        score += 10
        reasons.append("Akum Tipis")
    if broker_net > 10_000_000_000:
        score += 20
        reasons.append(f"Net {broker_net/1e9:.1f}B")
    elif broker_net > 0:
        score += 10
        reasons.append("Net+")
    try:
        if analysis_data.get('trend') == 'BULLISH':
            score += 20
            reasons.append("BULLISH")
        elif history_df is not None and len(history_df) > 50:
            ema50 = history_df['Close'].ewm(span=50).mean().iloc[-1]
            if history_df['Close'].iloc[-1] > ema50:
                score += 15
                reasons.append(">EMA50")
            score += 5
    except:
        pass
    if score >= 85:
        label = "VERY STRONG"
    elif score >= 70:
        label = "STRONG BUY"
    elif score >= 50:
        label = "WEAK BUY"
    else:
        label = "NO SIGNAL"
    return score, label, reasons

def scan_v3():
    print(f"[{get_now_wib()}] 🚀 V3 Scan...")
    screener_data = get_screener_latest()
    if not screener_data:
        print("⚠ Screener kosong, fallback 15 saham")
        candidates = ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS","TLKM","UNTR","ICBP"]
        screener_map = {s: {} for s in candidates}
    else:
        candidates = []
        screener_map = {}
        for item in screener_data:
            sym = item.get('symbol') or item.get('code') or item.get('stock')
            if sym:
                sym = sym.replace(".JK","").upper()
                candidates.append(sym)
                screener_map[sym] = item
        candidates = candidates[:30]
        print(f"  -> Kandidat: {candidates[:10]}")

    detected = []
    def process_symbol(sym):
        try:
            accum_val, _ = get_broker_accumulation(sym, top=3)
            if accum_val < -10_000_000_000:
                return None
            broker_net, broker_status, _ = get_broker_summary(sym)
            hist_df = get_history_pro(sym, limit=120)
            analysis = get_analysis(sym)
            score, label, reasons = calculate_score_v2(sym, hist_df, accum_val, broker_net, analysis)
            if score >= 60:
                last_close = 0
                change_pct = 0
                if hist_df is not None and len(hist_df) >= 2:
                    last_close = int(hist_df['Close'].iloc[-1])
                    prev = hist_df['Close'].iloc[-2]
                    change_pct = ((last_close/prev)-1)*100 if prev else 0
                return {
                    "symbol": sym,
                    "close": last_close,
                    "change_pct": change_pct,
                    "score": score,
                    "score_label": label,
                    "accum_value": accum_val,
                    "broker_net": broker_net,
                    "broker_status": broker_status,
                    "reasons": reasons,
                    "history_df": hist_df
                }
        except Exception as e:
            print(f"Error {sym}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_symbol, s) for s in candidates]
        for f in futures:
            res = f.result()
            if res:
                detected.append(res)
    detected.sort(key=lambda x: x['score'], reverse=True)
    print(f"✅ V3 Scan: {len(detected)} sinyal")
    return detected

def send_reply(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"TG Error: {e}")

def send_photo_reply(chat_id, photo_path, caption=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, 'rb') as photo:
            requests.post(url, data={'chat_id': chat_id, 'caption': caption, 'parse_mode': 'Markdown'}, files={'photo': photo}, timeout=30)
    except Exception as e:
        print(f"Photo Error: {e}")

def broadcast_v3(signals):
    if not signals:
        send_reply(TARGET_CHAT_ID, "🔍 V3 Scan: Tidak ada sinyal REAL ACCUM hari ini.")
        return
    now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header = f"*RAFANO V3 PRO - REAL ACCUM*\n{now_str}\nTotal: {len(signals)} | Cooldown 60m\n============================\n\n"
    msg = header
    keyboard = []
    for idx, item in enumerate(signals, 1):
        def fmt(v):
            return format_large_number(v, True)
        reasons_str = " | ".join(item['reasons'][:3])
        item_str = (
            f"{idx}. *{item['symbol']}* — {item['close']} ({item['change_pct']:+.2f}%)\n"
            f"   ├ Score: *{item['score']}% ({item['score_label']})*\n"
            f"   ├ Akum 3B: {fmt(item['accum_value'])} | Net: {fmt(item['broker_net'])} ({item['broker_status']})\n"
            f"   └ {reasons_str}\n\n"
        )
        keyboard.append([{"text": f"📈 {item['symbol']} Pro Chart", "callback_data": f"chart_{item['symbol']}_1d"}])
        if len(msg) + len(item_str) > 3500:
            send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard": keyboard})
            msg = item_str
            keyboard = []
        else:
            msg += item_str
    if msg:
        send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard": keyboard})

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_info_cache=None):
    send_reply(chat_id, f"📊 *Generating Pro Chart {stock_code.upper()} ({timeframe.upper()}) + REAL DATA...*")
    # Ambil data
    df = get_history_pro(stock_code, limit=150)
    if df is None or len(df) < 20:
        send_reply(chat_id, f"⚠ Data {stock_code} tidak ketemu")
        return
    
    # Ambil real data buat ditempel di chart
    if extra_info_cache and stock_code in extra_info_cache:
        extra = extra_info_cache[stock_code]
    else:
        accum_val, _ = get_broker_accumulation(stock_code, top=3)
        broker_net, broker_status, _ = get_broker_summary(stock_code)
        # Score cepat
        score = 70 if accum_val > 5e9 else 50
        extra = {"accum_value": accum_val, "broker_net": broker_net, "broker_status": broker_status, "score": score, "score_label": "REAL"}

    chart_file = f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path = generate_pro_chart(df=df, symbol=stock_code.upper(), timeframe=timeframe, sector_info=f"{stock_code.upper()} | IHSG", output_filename=chart_file, extra_info=extra)
        caption = (
            f"*{stock_code.upper()}* — {safe_int(df['Close'].iloc[-1])}\n"
            f"Score REAL: {extra.get('score',0)}% | Akum: {format_large_number(extra.get('accum_value',0), True)}\n"
            f"Net Broker: {format_large_number(extra.get('broker_net',0), True)} ({extra.get('broker_status')})\n"
            f"Timeframe: {timeframe.upper()}"
        )
        send_photo_reply(chat_id, file_path, caption=caption)
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        send_reply(chat_id, f"❌ Gagal render: `{e}`")

# Cache sinyal terakhir biar chart bisa ambil data real tanpa request lagi
LAST_SIGNALS_CACHE = {}

def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset = 0
    print("🤖 Telegram Listener V3 Running...")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res = requests.get(url, timeout=25)
            if res.status_code == 200:
                data = res.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    if "callback_query" in update:
                        cb = update["callback_query"]
                        cb_id = cb.get("id")
                        cb_data = cb.get("data","")
                        chat_id = cb["message"]["chat"]["id"]
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": cb_id})
                        if cb_data.startswith("chart_"):
                            parts = cb_data.split("_")
                            if len(parts) >=3:
                                sym = parts[1]
                                tf = parts[2]
                                threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                    elif "message" in update and "text" in update["message"]:
                        msg = update["message"]
                        text = msg.get("text","").strip()
                        chat_id = msg["chat"]["id"]
                        first_word = text.split()[0].lower() if text else ""
                        if first_word in ["/start","/help"]:
                            help_msg = (
                                "🤖 *RAFANO V3 PRO*\n"
                                "============================\n"
                                "Perintah:\n"
                                "📈 `/c <KODE> [TF]` - Chart Pro + Real Akum\n"
                                "   Contoh: `/c BBCA` `/c ANTM 15m`\n"
                                "🔍 `/scan` - Scan V3 Real Accumulation\n"
                                "🔥 `/scanpro` - Scan + langsung chart top 3\n"
                            )
                            send_reply(chat_id, help_msg)
                        elif first_word in ["/c","/chart","!chart"]:
                            parts = text.split()
                            if len(parts) >=2:
                                sym = parts[1].upper()
                                tf = parts[2] if len(parts)>=3 else "1d"
                                threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                            else:
                                send_reply(chat_id, "⚠ Format: `/c <KODE> [TF]`")
                        elif first_word in ["/scan","!scan","/scanpro"]:
                            send_reply(chat_id, "🔍 *V3 Scanning Real Accumulation...*")
                            def manual_scan(is_pro=False, target_chat=chat_id):
                                global LAST_SIGNALS_CACHE
                                sigs = scan_v3()
                                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                                filt = filter_signals_with_cooldown(sigs)
                                broadcast_v3(filt) if target_chat == TARGET_CHAT_ID else None
                                # kirim ke yang request
                                if target_chat != TARGET_CHAT_ID:
                                    # buat broadcast khusus ke user
                                    now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
                                    header = f"*RAFANO V3 PRO - {now_str}*\nTotal: {len(filt)}\n\n"
                                    # reuse broadcast logic
                                    msg = header
                                    kb = []
                                    for idx, item in enumerate(filt,1):
                                        item_str = f"{idx}. *{item['symbol']}* {item['score']}% Akum:{format_large_number(item['accum_value'],True)}\n"
                                        kb.append([{"text": f"📈 {item['symbol']}", "callback_data": f"chart_{item['symbol']}_1d"}])
                                        msg += item_str
                                    send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                                if is_pro:
                                    # auto kirim chart top 3
                                    for top in filt[:3]:
                                        process_chart_request(target_chat, top['symbol'], "1d", LAST_SIGNALS_CACHE)
                                        time.sleep(1)
                            threading.Thread(target=manual_scan, args=(first_word=="/scanpro", target_chat=chat_id)).start()
        except Exception as e:
            print(f"Listener error: {e}")
            time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("🚀 Auto Screener V3 Active...")
    last_triggered_sesi1, last_triggered_eod = "", ""
    while True:
        try:
            if not is_market_open():
                time.sleep(300)
                continue
            now = get_now_wib()
            today_str, current_time_str = now.strftime('%Y-%m-%d'), now.strftime('%H:%M')
            weekday = now.weekday()

            # Rekap sesi
            target_sesi1 = "11:25" if weekday == 4 else "11:55"
            if current_time_str == target_sesi1 and last_triggered_sesi1 != today_str:
                sigs = scan_v3()
                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                filt = filter_signals_with_cooldown(sigs)
                broadcast_v3(filt)
                last_triggered_sesi1 = today_str

            if current_time_str == "15:55" and last_triggered_eod != today_str:
                sigs = scan_v3()
                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                filt = filter_signals_with_cooldown(sigs)
                broadcast_v3(filt)
                last_triggered_eod = today_str

            # Real-time per 10 menit
            sigs = scan_v3()
            LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
            filt = filter_signals_with_cooldown(sigs)
            if filt:
                broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            print(f"Auto loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print("==========================================")
    print("🔥 RAFANO V3 PRO STARTING...")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
