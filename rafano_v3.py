# ==============================================================================
# RAFANO V3 - ENGINE ANALISIS & SINYAL SAHAM IHSG (VERSI PERBAIKAN LENGKAP)
# ==============================================================================
# Cakupan Perbaikan:
# 1. Perhitungan Sinyal Sell Bebas Lookahead Bias (4 Pola Exit Strategy Spot)
# 2. Perhitungan Trading Plan Saham Spot (Tanpa Mutasi Palsu & Tanpa Rumus Short)
# 3. Perhitungan Broker Flow / Bandarmology (Bebas Bug abs(nval), Tersedia avg_sell)
# 4. Filter Ketat Top Seller & Top Buyer
# 5. Format Caption Alert Telegram Profesional (Holders vs Watchers)
# ==============================================================================

import math
import traceback
import pandas as pd
import numpy as np


# ==============================================================================
# 1. ATURAN FRAKSI HARGA BURSA EFEK INDONESIA (IHSG / BEI)
# ==============================================================================
def get_ihsg_tick_size(price: float) -> int:
    """Mengembalikan fraksi harga (tick size) resmi BEI sesuai rentang harga."""
    if price < 200:
        return 1
    elif price < 500:
        return 2
    elif price < 2000:
        return 5
    elif price < 5000:
        return 10
    else:
        return 25

def round_to_ihsg_fraction(price: float) -> int:
    """Membulatkan harga ke fraksi tick terdekat yang sah di BEI."""
    if price <= 0:
        return 50
    tick = get_ihsg_tick_size(price)
    rounded = int(round(price / tick) * tick)
    return max(50, rounded)


# ==============================================================================
# 2. INDIKATOR TEKNIKAL & VOLUME SPREAD ANALYSIS (VSA)
# ==============================================================================
def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Menghitung Average True Range (ATR) 14 periode."""
    high = df['High']
    low = df['Low']
    close = df['Close']
    prev_close = close.shift(1)
    
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Menghitung Relative Strength Index (RSI) 14 periode."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)

def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, num_std: float = 2.0):
    """Menghitung Bollinger Bands (Mid, Upper, Lower)."""
    mid = df['Close'].rolling(window=period, min_periods=1).mean()
    std = df['Close'].rolling(window=period, min_periods=1).std(ddof=0).fillna(0)
    upper = mid + (num_std * std)
    lower = mid - (num_std * std)
    return mid, upper, lower

def calculate_vsa_metrics(df: pd.DataFrame):
    """Menghitung metrik dasar Volume Spread Analysis (VSA)."""
    df = df.copy()
    spread = df['High'] - df['Low']
    close_pos = (df['Close'] - df['Low']) / spread.replace(0, 1.0)
    vol_mean = df['Volume'].rolling(20, min_periods=1).mean()
    
    # Net Val VSA aproksimasi volume arah transaksi
    df['Net_Val_VSA'] = np.where(close_pos >= 0.5, df['Volume'] * close_pos, -df['Volume'] * (1 - close_pos))
    df['V1'] = vol_mean
    return df, {}


# ==============================================================================
# 3. PERBAIKAN TOTAL: BROKER ACCUMULATION & BANDARMOLOGY (SELL & BUY)
# ==============================================================================
def get_broker_accumulation(symbol: str, top: int = 3, days: int = None, fetch_fn=None):
    """
    FIXED BROKER ACCUMULATION & DISTRIBUTION:
    - Menghapus bug fatal: accum_total += abs(nval) pada seller
    - Menghitung Net Flow Riil: Net = Total Buyer - Total Seller (Positif = AKUM, Negatif = DIST)
    - Menyelaraskan net_value saat broker terdaftar di kedua sisi (buyers & sellers)
    """
    try:
        data = fetch_fn(symbol, top=top, days=days) if fetch_fn else None
        if not data:
            return 0.0, []

        top_buyers = data.get('top_buyers') or []
        top_sellers = data.get('top_sellers') or []
        broker_dict = {}

        # 1. Parse Top Buyers
        if top_buyers and isinstance(top_buyers, list):
            for b in top_buyers[:20]:
                if not isinstance(b, dict):
                    continue
                code = str(b.get('broker_code') or b.get('broker') or '??').upper()
                bval = float(b.get('bval') or b.get('buy_value') or 0)
                sval = float(b.get('sval') or b.get('sell_value') or 0)
                nval = b.get('nval') or b.get('net_val') or b.get('net_value')
                nval = float(nval) if nval is not None else (bval - sval)
                avg = float(b.get('bavg') or b.get('avg_price') or 0)

                broker_dict[code] = {
                    "broker_code": code,
                    "broker": code,
                    "buy_value": bval,
                    "sell_value": sval,
                    "net_value": nval,
                    "avg_price": avg,
                    "sell_avg_price": 0.0
                }

        # 2. Parse Top Sellers (Bebas dari bug abs!)
        if top_sellers and isinstance(top_sellers, list):
            for b in top_sellers[:20]:
                if not isinstance(b, dict):
                    continue
                code = str(b.get('broker_code') or b.get('broker') or '??').upper()
                sval = float(b.get('sval') or b.get('sell_value') or 0)
                bval = float(b.get('bval') or b.get('buy_value') or 0)
                nval = b.get('nval') or b.get('net_val') or b.get('net_value')
                
                # Pastikan sisi seller menghasilkan net negatif jika murni menjual
                nval_f = float(nval) if nval is not None else (bval - sval)
                if nval_f > 0 and bval == 0:
                    nval_f = -abs(nval_f)

                savg = float(b.get('savg') or b.get('sell_avg') or b.get('avg_price') or 0)

                if code in broker_dict:
                    # Update sell_value DAN RE-KALKULASI net_value riil
                    broker_dict[code]['sell_value'] += sval
                    broker_dict[code]['net_value'] = broker_dict[code]['buy_value'] - broker_dict[code]['sell_value']
                    if savg > 0:
                        broker_dict[code]['sell_avg_price'] = savg
                else:
                    broker_dict[code] = {
                        "broker_code": code,
                        "broker": code,
                        "buy_value": bval,
                        "sell_value": sval if sval > 0 else abs(nval_f),
                        "net_value": nval_f,
                        "avg_price": 0.0,
                        "sell_avg_price": savg
                    }

        raw_brokers = list(broker_dict.values())
        
        # Net Accum Total = Selisih Arus Uang Riil (Positif = AKUM, Negatif = DIST)
        accum_total = sum(b['net_value'] for b in raw_brokers)
        return float(accum_total), raw_brokers
    except Exception as e:
        print(f"get_broker_accumulation error: {e}")
        return 0.0, []

def calculate_bandars_avg(brokers: list):
    """
    FIXED BANDAR AVERAGE PRICE:
    Menghitung avg_buy (Harga Rata-rata Beli Bandar) DAN avg_sell (Harga Rata-rata Jual Bandar).
    Kembalian tuple: (avg_buy, avg_sell)
    """
    try:
        if not brokers or not isinstance(brokers, list):
            return 0.0, 0.0

        total_buy_val, total_buy_vol = 0.0, 0.0
        total_sell_val, total_sell_vol = 0.0, 0.0

        for b in brokers:
            if not isinstance(b, dict):
                continue
            net = float(b.get('net_value', 0) or 0)
            avg_p = float(b.get('avg_price', 0) or 0)
            savg_p = float(b.get('sell_avg_price', 0) or 0)

            # Sisi Pembeli
            if net > 0 and avg_p > 0:
                total_buy_val += avg_p * abs(net)
                total_buy_vol += abs(net)

            # Sisi Penjual (Level Resistensi Buang Barang)
            if net < 0:
                sell_p = savg_p if savg_p > 0 else avg_p
                if sell_p > 0:
                    total_sell_val += sell_p * abs(net)
                    total_sell_vol += abs(net)

        avg_buy = float(total_buy_val / total_buy_vol) if total_buy_vol > 0 else 0.0
        avg_sell = float(total_sell_val / total_sell_vol) if total_sell_vol > 0 else 0.0
        return avg_buy, avg_sell
    except Exception as e:
        print(f"calculate_bandars_avg error: {e}")
        return 0.0, 0.0

def format_top_brokers(brokers: list, top: int = 3, status: str = "AKUM") -> str:
    """
    FIXED: Memisahkan Top Buyer vs Top Seller secara ketat.
    Top Seller HANYA mencantumkan broker dengan net_value < 0 (BUKAN pembeli yang terselip).
    """
    if not brokers or not isinstance(brokers, list):
        return "-"

    if status in ["DIST", "DISTRIB"]:
        # Filter khusus seller murni
        sellers = [b for b in brokers if float(b.get('net_value', 0) or 0) < 0]
        if not sellers:
            sellers = [b for b in brokers if float(b.get('sell_value', 0) or 0) > 0]
        sellers.sort(key=lambda x: abs(float(x.get('net_value', 0) or x.get('sell_value', 0) or 0)), reverse=True)
        
        parts = []
        for b in sellers[:top]:
            code = b.get('broker_code') or b.get('broker') or "??"
            val = abs(float(b.get('net_value', 0) or b.get('sell_value', 0) or 0))
            if val >= 1e9:
                s = f"{val/1e9:.1f}B"
            elif val >= 1e6:
                s = f"{val/1e6:.0f}M"
            else:
                s = f"{val:.0f}"
            parts.append(f"{code} -{s}")
        return ", ".join(parts) if parts else "-"
    else:
        # Filter khusus buyer murni
        buyers = [b for b in brokers if float(b.get('net_value', 0) or 0) > 0]
        buyers.sort(key=lambda x: float(x.get('net_value', 0) or x.get('buy_value', 0) or 0), reverse=True)
        parts = []
        for b in buyers[:top]:
            code = b.get('broker_code') or b.get('broker') or "??"
            val = float(b.get('net_value', 0) or b.get('buy_value', 0) or 0)
            if val >= 1e9:
                s = f"{val/1e9:.1f}B"
            elif val >= 1e6:
                s = f"{val/1e6:.0f}M"
            else:
                s = f"{val:.0f}"
            parts.append(f"{code} +{s}")
        return ", ".join(parts) if parts else "-"


# ==============================================================================
# 4. DETEKSI SINYAL SELL (4 POLA EXIT STRATEGY SPOT IHSG)
# ==============================================================================
def detect_sell_signals(df: pd.DataFrame, multi_tf: dict = None):
    """
    DETEKSI SINYAL EXIT STRATEGY LENGKAP:
    1. SOS BB: Rejection di Upper Bollinger Band (Take Profit on Strength)
    2. BD EMA50: Breakdown support tren utama EMA50 (Emergency Exit / Cut Loss)
    3. TS EMA20: Trailing stop jebol di bawah EMA20 (Kunci Sisa Keuntungan Swing)
    4. DIST CLIMAX: Volume meledak tapi candle tertahan/distribusi masif (Bandar Keluar)
    """
    signals = []
    if df is None or len(df) < 25:
        return signals, df

    try:
        df = df.copy()
        if 'EMA50' not in df.columns:
            df['EMA13'] = df['Close'].ewm(span=13, adjust=False).mean()
            df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
            df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
            df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
            df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
            df['ATR'] = calculate_atr(df, 14)
            bb_mid, bb_upper, bb_lower = calculate_bollinger_bands(df, 20, 2)
            df['BB_MID'] = bb_mid
            df['BB_UPPER'] = bb_upper
            df['BB_LOWER'] = bb_lower
            df, _ = calculate_vsa_metrics(df)
            df['RSI'] = calculate_rsi(df['Close'], 14)

        for i in range(20, len(df)):
            close = float(df['Close'].iloc[i])
            open_ = float(df['Open'].iloc[i])
            high = float(df['High'].iloc[i])
            low = float(df['Low'].iloc[i])
            vol = float(df['Volume'].iloc[i])
            v1 = float(df['V1'].iloc[i]) if df['V1'].iloc[i] > 0 else 1.0
            ema20 = float(df['EMA20'].iloc[i])
            ema50 = float(df['EMA50'].iloc[i])
            ema200 = float(df['EMA200'].iloc[i])
            bb_up = float(df['BB_UPPER'].iloc[i]) if not pd.isna(df['BB_UPPER'].iloc[i]) else 0.0
            atr = float(df['ATR'].iloc[i]) if not pd.isna(df['ATR'].iloc[i]) else close * 0.03
            rsi = float(df['RSI'].iloc[i]) if 'RSI' in df.columns and not pd.isna(df['RSI'].iloc[i]) else 50.0

            prev_close = float(df['Close'].iloc[i - 1])
            prev_open = float(df['Open'].iloc[i - 1])
            prev_ema50 = float(df['EMA50'].iloc[i - 1])
            prev_ema20 = float(df['EMA20'].iloc[i - 1])
            prev2_close = float(df['Close'].iloc[i - 2]) if i >= 2 else prev_close
            prev2_ema20 = float(df['EMA20'].iloc[i - 2]) if i >= 2 else prev_ema20

            # Bebas lookahead bias: Gunakan rolling 5-bar VSA untuk candle lampau
            start_j = max(0, i - 4)
            net_5d_rolling = float(df['Net_Val_VSA'].iloc[start_j:i + 1].sum()) if 'Net_Val_VSA' in df.columns else 0.0
            is_current_candle = (i == len(df) - 1)
            is_dist = (multi_tf.get('net_5d', 0) < 0 or multi_tf.get('status_5d') == 'DIST') if (is_current_candle and multi_tf) else (net_5d_rolling < 0)

            vol_multiple = vol / v1
            is_red = close < open_
            body = abs(close - open_)
            upper_wick = high - max(open_, close)
            lower_wick = min(open_, close) - low

            # Target Penurunan / Area Serok Kembali (Support)
            recent_low = float(df['Low'].iloc[max(0, i - 15):i].min()) if i > 5 else low
            s1 = round_to_ihsg_fraction(max(recent_low, close - atr * 1.5))
            s2 = round_to_ihsg_fraction(ema200 if ema200 > 0 else (s1 - atr * 1.5))

            # ------------------------------------------------------------------
            # POLA 1: BD EMA50 (Breakdown Support Tren & Emergency Cut Loss)
            # ------------------------------------------------------------------
            is_bd_ema50 = (prev_close >= prev_ema50 and close < ema50) or (prev_close < prev_ema50 and close < ema50 and close < df['Low'].iloc[i - 1])
            if is_bd_ema50 and vol_multiple >= 1.3 and (is_red or is_dist):
                signals.append({
                    'index': i,
                    'date': df.index[i],
                    'type': 'BD EMA50',
                    'side': 'SELL',
                    'entry': float(close),
                    'sl': float(round_to_ihsg_fraction(max(high, ema50 + atr * 0.5))),
                    's1': float(s1),
                    's2': float(s2),
                    'action': 'CUT_LOSS',
                    'reason': f'Breakdown EMA50 (Vol {vol_multiple:.1f}x) + Distribusi konfirmasi tren jebol',
                    'strength': 92 if is_dist else 82
                })
                continue

            # ------------------------------------------------------------------
            # POLA 2: SOS BB (Take Profit on Strength / Rejection Upper BB)
            # ------------------------------------------------------------------
            if bb_up > 0:
                dist_to_bb_up = ((high - bb_up) / bb_up) * 100
                is_pinbar = upper_wick >= body * 1.2 and upper_wick > lower_wick * 1.5
                is_engulfing = is_red and prev_close > prev_open and close < prev_open and body > (prev_close - prev_open)
                if dist_to_bb_up >= 0.5 and (is_pinbar or is_engulfing) and (rsi > 65 or is_red):
                    signals.append({
                        'index': i,
                        'date': df.index[i],
                        'type': 'SOS BB',
                        'side': 'SELL',
                        'entry': float(round_to_ihsg_fraction(high * 0.99)),
                        'sl': float(round_to_ihsg_fraction(high + round_to_ihsg_fraction(high * 0.015))),
                        's1': float(round_to_ihsg_fraction(df['BB_MID'].iloc[i] if 'BB_MID' in df.columns else close - atr)),
                        's2': float(s1),
                        'action': 'TAKE_PROFIT',
                        'reason': f'SOS: Rejection Upper BB (+{dist_to_bb_up:.1f}%) + RSI {rsi:.0f} overbought pinbar',
                        'strength': 90 if rsi > 75 else 84
                    })
                    continue

            # ------------------------------------------------------------------
            # POLA 3: TS EMA20 (Trailing Stop Hit - Kunci Keuntungan Reli)
            # ------------------------------------------------------------------
            was_above_ema20 = prev_close >= prev_ema20 and prev2_close >= prev2_ema20
            broke_ema20 = was_above_ema20 and close < ema20 and is_red
            if broke_ema20 and close > ema50:
                signals.append({
                    'index': i,
                    'date': df.index[i],
                    'type': 'TS EMA20',
                    'side': 'SELL',
                    'entry': float(close),
                    'sl': float(round_to_ihsg_fraction(max(high, ema20 + atr * 0.5))),
                    's1': float(round_to_ihsg_fraction(ema50)),
                    's2': float(s1),
                    'action': 'TRAILING_STOP',
                    'reason': f'Trailing Stop Hit: Tutup di bawah EMA20 ({close:.0f} < {ema20:.0f}), amankan profit',
                    'strength': 80
                })
                continue

            # ------------------------------------------------------------------
            # POLA 4: DIST CLIMAX (Upthrust / Volume Meledak Distribusi Bandar)
            # ------------------------------------------------------------------
            if vol_multiple >= 2.0 and (is_red or upper_wick > body) and is_dist:
                signals.append({
                    'index': i,
                    'date': df.index[i],
                    'type': 'DIST CLIMAX',
                    'side': 'SELL',
                    'entry': float(close),
                    'sl': float(round_to_ihsg_fraction(high * 1.02)),
                    's1': float(s1),
                    's2': float(s2),
                    'action': 'AVOID',
                    'reason': f'Distribution Climax: Volume {vol_multiple:.1f}x meledak tapi harga tertahan/jual masif',
                    'strength': 88
                })

        # Filter jarak 4 bar agar rapi dan tidak saling tumpuk
        filtered = []
        last_idx = -20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index'] - last_idx >= 4:
                filtered.append(sig)
                last_idx = sig['index']
            elif filtered and sig['strength'] > filtered[-1]['strength']:
                filtered[-1] = sig
                last_idx = sig['index']
        return filtered, df
    except Exception as e:
        print(f"detect_sell_signals error: {e}")
        return [], df


# ==============================================================================
# 5. DETEKSI SINYAL BUY (STANDAR VSA RAFANO)
# ==============================================================================
def detect_buy_signals(df: pd.DataFrame, multi_tf: dict = None):
    """Deteksi sinyal Buy (BOS, BO EMA50, Rebound)."""
    signals = []
    if df is None or len(df) < 25:
        return signals, df
    try:
        df = df.copy()
        for i in range(20, len(df)):
            close = float(df['Close'].iloc[i])
            prev_close = float(df['Close'].iloc[i-1])
            ema50 = float(df['EMA50'].iloc[i])
            prev_ema50 = float(df['EMA50'].iloc[i-1])
            vol = float(df['Volume'].iloc[i])
            v1 = float(df['V1'].iloc[i]) if df['V1'].iloc[i] > 0 else 1.0

            # Breakout EMA50 dengan konfirmasi volume
            if prev_close < prev_ema50 and close > ema50 and vol >= 1.3 * v1:
                signals.append({
                    'index': i,
                    'date': df.index[i],
                    'type': 'BO EMA50',
                    'side': 'BUY',
                    'entry': float(close),
                    'sl': float(round_to_ihsg_fraction(df['Low'].iloc[i] * 0.98)),
                    'strength': 85,
                    'reason': 'Breakout EMA50 dengan konfirmasi volume'
                })
        return signals, df
    except Exception as e:
        print(f"detect_buy_signals error: {e}")
        return [], df


# ==============================================================================
# 6. PERHITUNGAN TRADING PLAN (SPOT EXIT STRATEGY MODEL)
# ==============================================================================
def calculate_trading_plan(df: pd.DataFrame, signals: list = None, multi_tf: dict = None, timeframe: str = "1d"):
    """
    FIXED TRADING PLAN - MODEL EXIT STRATEGY SAHAM SPOT IHSG:
    - Tidak ada mutasi harga fiktif (entry = sl * 0.97 DIHAPUS)
    - Menghitung Area Jual Realistis (Pasar / HAKI Jual)
    - Menyediakan Target Penurunan Terhindar (Downside Avoided %)
    - Menyediakan Area Serok Kembali (Support S1 & S2)
    - Memisahkan panduan Pemegang Saham (Holders) vs Calon Pembeli (Watchers)
    """
    try:
        if df is None or len(df) < 20:
            return None
        last_close = float(df['Close'].iloc[-1])
        last_high = float(df['High'].iloc[-1])
        last_low = float(df['Low'].iloc[-1])
        atr = float(calculate_atr(df, 14).iloc[-1]) if 'ATR' not in df.columns else float(df['ATR'].iloc[-1])
        if pd.isna(atr) or atr == 0:
            atr = last_close * 0.03
        ema20 = float(df['Close'].ewm(span=20).mean().iloc[-1])
        ema50 = float(df['Close'].ewm(span=50).mean().iloc[-1])
        ema200 = float(df['Close'].ewm(span=200).mean().iloc[-1])

        if signals is None:
            buy_sigs, _ = detect_buy_signals(df, multi_tf)
            sell_sigs, _ = detect_sell_signals(df, multi_tf)
            signals = buy_sigs + sell_sigs
        else:
            buy_sigs = [s for s in signals if s.get('side') == 'BUY']
            sell_sigs = [s for s in signals if s.get('side') == 'SELL']

        mtf_confirm = "NEUTRAL"
        is_mtf_dist = False
        if multi_tf:
            st_5d = multi_tf.get('status_5d', 'NEUTRAL')
            st_20d = multi_tf.get('status_20d', 'NEUTRAL')
            net_5d = multi_tf.get('net_5d', 0)
            net_20d = multi_tf.get('net_20d', 0)
            if (st_5d == "DIST" or net_5d < 0) and (st_20d == "DIST" or net_20d < 0):
                mtf_confirm = "HEAVY DISTRIBUTION (Weekly & Monthly)"
                is_mtf_dist = True
            elif st_5d == "DIST" or net_5d < 0:
                mtf_confirm = "BEARISH DISTRIBUTION (Weekly 5D)"
                is_mtf_dist = True
            elif st_5d == "AKUM" and st_20d == "AKUM":
                mtf_confirm = "STRONG BULLISH MTF"

        recent_buy = [s for s in buy_sigs if s['index'] >= len(df) - 10]
        recent_sell = [s for s in sell_sigs if s['index'] >= len(df) - 10]

        # Prioritaskan sinyal teraktual
        if recent_sell and (not recent_buy or recent_sell[-1]['index'] >= recent_buy[-1]['index']):
            last_sig = recent_sell[-1]
            side = "SELL"
            signal_type = last_sig['type']
            signal_reason = last_sig['reason']
            signal_strength = last_sig['strength']
            signal_date = last_sig['date']
            if is_mtf_dist:
                signal_strength = min(100, signal_strength + 10)
                signal_reason += " + MTF Distribusi Masif"

            # Area Jual yang Realistis
            if signal_type == "SOS BB":
                ideal_sell = round_to_ihsg_fraction(max(last_high, last_close))
                min_sell = round_to_ihsg_fraction(last_close)
                action_title = "REKOMENDASI: TAKE PROFIT ON STRENGTH (SOS)"
                holder_guide = "Manfaatkan lonjakan harga untuk jual bertahap (TP 50%-100%)."
                watcher_guide = "Harga di area overbought/pucuk. Jangan FOMO, tunggu koreksi."
            elif signal_type == "BD EMA50":
                ideal_sell = round_to_ihsg_fraction(last_close)
                min_sell = round_to_ihsg_fraction(last_close - atr * 0.5)
                action_title = "REKOMENDASI: EMERGENCY EXIT / CUT LOSS"
                holder_guide = "Tutup posisi segera (100% Cash Out). Struktur tren naik rusak."
                watcher_guide = "DILARANG TANGKAP PISAU JATUH. Pantau level serok di S1/S2."
            elif signal_type == "TS EMA20":
                ideal_sell = round_to_ihsg_fraction(last_close)
                min_sell = round_to_ihsg_fraction(last_close - atr * 0.3)
                action_title = "REKOMENDASI: TRAILING STOP HIT"
                holder_guide = "Kunci sisa profit swing Anda, tren jangka pendek melemah."
                watcher_guide = "Tunggu fase konsolidasi atau pantulan di EMA50."
            else:
                ideal_sell = round_to_ihsg_fraction(last_close)
                min_sell = round_to_ihsg_fraction(last_close - atr * 0.5)
                action_title = "REKOMENDASI: AMBIL PROFIT & KURANGI POSISI"
                holder_guide = "Jual bertahap untuk mengamankan modal."
                watcher_guide = "Wait & See, jangan masuk posisi baru."

            # Batas Invalidation SL & Trailing Stop
            invalidation_sl = round_to_ihsg_fraction(last_sig.get('sl', last_high + atr * 0.5))
            trailing_stop = round_to_ihsg_fraction(max(ema20, last_close - atr * 1.2))

            # Target Support Downside (Area Serok Kembali)
            s1 = round_to_ihsg_fraction(last_sig.get('s1', last_close - atr * 1.5))
            s2 = round_to_ihsg_fraction(last_sig.get('s2', last_close - atr * 3.0))
            if s2 >= s1:
                s2 = round_to_ihsg_fraction(s1 - max(atr, s1 * 0.03))

            drop_pct = round(((ideal_sell - s1) / ideal_sell) * 100, 2)

            return {
                "side": "SELL",
                "entry": int(ideal_sell),
                "sell_ideal": int(ideal_sell),
                "sell_min": int(min_sell),
                "sl": int(invalidation_sl),
                "trailing_stop": int(trailing_stop),
                "tp1": int(s1),
                "tp2": int(s2),
                "support": int(s1),
                "resistance": int(invalidation_sl),
                "potential_drop_pct": drop_pct,
                "action_title": action_title,
                "holder_guide": holder_guide,
                "watcher_guide": watcher_guide,
                "atr": float(atr),
                "risk_pct": round(((invalidation_sl - ideal_sell) / ideal_sell) * 100, 2),
                "rr1": 0,
                "rr2": 0,
                "trend": f"DOWNTREND + {mtf_confirm}" if is_mtf_dist else "WEAKENING TREND",
                "signal_type": signal_type,
                "signal_reason": signal_reason,
                "signal_strength": signal_strength,
                "signal_date": signal_date,
                "is_buy_signal": False,
                "is_sell_signal": signal_strength >= 75,
                "mtf_confirm": mtf_confirm
            }

        elif recent_buy:
            last_sig = recent_buy[-1]
            entry = round_to_ihsg_fraction(last_sig['entry'])
            sl = round_to_ihsg_fraction(last_sig['sl'])
            tp1 = round_to_ihsg_fraction(entry + atr * 1.5)
            tp2 = round_to_ihsg_fraction(entry + atr * 3.0)
            return {
                "side": "BUY",
                "entry": int(entry),
                "sl": int(sl),
                "tp1": int(tp1),
                "tp2": int(tp2),
                "support": int(sl),
                "resistance": int(tp2),
                "atr": float(atr),
                "risk_pct": round(((entry - sl) / entry) * 100, 2),
                "trend": "UPTREND",
                "signal_type": last_sig['type'],
                "signal_reason": last_sig['reason'],
                "signal_strength": last_sig['strength'],
                "signal_date": last_sig['date'],
                "is_buy_signal": True,
                "is_sell_signal": False,
                "mtf_confirm": mtf_confirm
            }
        else:
            return {
                "side": "WAIT",
                "entry": int(round_to_ihsg_fraction(last_close)),
                "sl": int(round_to_ihsg_fraction(last_close * 0.95)),
                "tp1": int(round_to_ihsg_fraction(last_close * 1.04)),
                "tp2": int(round_to_ihsg_fraction(last_close * 1.08)),
                "support": int(round_to_ihsg_fraction(last_close - atr * 1.5)),
                "resistance": int(round_to_ihsg_fraction(last_close + atr * 1.5)),
                "signal_type": "NO SIGNAL",
                "signal_reason": "Tunggu konfirmasi trigger BO/BOS (Buy) atau SOS/BD (Sell)",
                "signal_strength": 0,
                "is_buy_signal": False,
                "is_sell_signal": False,
                "trend": "SIDEWAYS / NEUTRAL"
            }
    except Exception as e:
        print(f"calculate_trading_plan error: {e}")
        return None


# ==============================================================================
# 7. FORMATTER TELEGRAM ALERT LENGKAP
# ==============================================================================
def format_telegram_alert(stock_code: str, df: pd.DataFrame, tp: dict, multi_tf: dict = None, tf_label: str = "Daily") -> str:
    """Format pesan Telegram yang jelas, elegan, dan informatif."""
    if not tp:
        return f"*{stock_code.upper()}* - Tidak ada data trading plan."

    last_close = int(df['Close'].iloc[-1])
    side = tp.get('side', 'WAIT')
    sig_strength = tp.get('signal_strength', 0)
    sig_reason = tp.get('signal_reason', '')
    mtf_confirm = tp.get('mtf_confirm', 'NEUTRAL')

    grade_label = "STRONG SELL" if sig_strength >= 85 else ("MODERATE SELL" if sig_strength >= 75 else "NEUTRAL")
    sig_emoji = "🚨" if side == "SELL" else ("🟢" if side == "BUY" else "⚪")

    # Ambil baris ringkasan multi timeframe jika tersedia
    daily_line = f"Net: {multi_tf.get('net_d', 0):,.0f} ({multi_tf.get('status_d', 'NEUTRAL')})" if multi_tf else "-"
    weekly_line = f"Net: {multi_tf.get('net_5d', 0):,.0f} ({multi_tf.get('status_5d', 'NEUTRAL')})" if multi_tf else "-"

    if side == "SELL":
        return (
            f"*{stock_code.upper()}* -- Rp {last_close:,} | {tp.get('trend', 'DOWNTREND')}\n"
            f"{sig_emoji} Grade: *{grade_label}* | Strength: {sig_strength}%\n"
            f"Daily: {daily_line}\n"
            f"Weekly 5D: {weekly_line}\n"
            f"Timeframe: {tf_label}\n"
            f"----------------------------------------\n"
            f"TRIGGER: {sig_reason}\n"
            f"----------------------------------------\n"
            f"*{tp.get('action_title', 'REKOMENDASI: AMBIL PROFIT & KELUAR')}*\n"
            f"📍 Area Jual Eksekusi: *{tp['entry']}* (Pasar / HAKI)\n"
            f"🛡️ Trailing Stop / Invalidation: *{tp['trailing_stop']}* (SL: {tp['sl']})\n"
            f"📉 Target Penurunan Terhindar: *-{tp.get('potential_drop_pct', 0)}%*\n"
            f"🎯 Area Serok Kembali (Support Target):\n"
            f"   └ S1 (Rebound Watch): *{tp['tp1']}*\n"
            f"   └ S2 (Base Support): *{tp['tp2']}*\n\n"
            f"👥 *PANDUAN EKSEKUSI:*\n"
            f"• *Holders (Punya Barang):* {tp.get('holder_guide', 'Jual bertahap untuk amankan modal.')}\n"
            f"• *Watchers (Belum Punya):* {tp.get('watcher_guide', 'Dilarang masuk posisi baru.')}\n"
        )
    elif side == "BUY":
        return (
            f"*{stock_code.upper()}* -- Rp {last_close:,} | {tp.get('trend', 'UPTREND')}\n"
            f"🟢 Grade: *BUY* | Strength: {sig_strength}%\n"
            f"Timeframe: {tf_label}\n"
            f"----------------------------------------\n"
            f"TRIGGER: {sig_reason}\n"
            f"----------------------------------------\n"
            f"TRADING PLAN - BUY\n"
            f"Entry: {tp['entry']} | SL: {tp['sl']} ({tp.get('risk_pct', 0)}%)\n"
            f"TP1: {tp['tp1']} | TP2: {tp['tp2']}\n"
            f"Sup: {tp['support']} | Res: {tp['resistance']}\n"
        )
    else:
        return (
            f"*{stock_code.upper()}* -- Rp {last_close:,} | SIDEWAYS / NEUTRAL\n"
            f"Status: ⏸️ WAIT - Belum ada trigger konfirmasi valid.\n"
            f"Timeframe: {tf_label}\n"
            f"Sup: {tp.get('support', 0)} | Res: {tp.get('resistance', 0)}\n"
        )


# ==============================================================================
# CONTOH CARA PENGGUNAAN MANDIRI / TESTING
# ==============================================================================
if __name__ == "__main__":
    # Dummy data pengujian saham BBCA
    data = {
        'Open': [9800, 9825, 9850, 9900, 9950, 10000, 10050, 10025, 9950, 9850, 9700, 9550] * 3,
        'High': [9850, 9875, 9900, 9975, 10025, 10100, 10150, 10050, 9975, 9875, 9725, 9575] * 3,
        'Low':  [9775, 9800, 9825, 9875, 9925, 9950, 9975, 9900, 9825, 9675, 9525, 9425] * 3,
        'Close':[9825, 9850, 9875, 9950, 10000, 10050, 10000, 9925, 9850, 9700, 9550, 9450] * 3,
        'Volume':[15e6, 18e6, 16e6, 22e6, 25e6, 30e6, 35e6, 40e6, 48e6, 60e6, 75e6, 80e6] * 3
    }
    df_test = pd.DataFrame(data)
    
    # Mock data broker dengan distribusi ZP, BK, CS
    mock_mtf = {
        'status_5d': 'DIST',
        'net_5d': -185_000_000_000,
        'status_d': 'DIST',
        'net_d': -45_000_000_000
    }

    # 1. Jalankan Deteksi Sinyal Sell
    signals, df_calc = detect_sell_signals(df_test, mock_mtf)
    print(f"Jumlah Sinyal Sell Terdeteksi: {len(signals)}")
    for s in signals:
        print(f" -> [{s['type']}] Tanggal/Bar: {s['index']} | Entry: Rp {s['entry']:,} | Aksi: {s['action']}")

    # 2. Hitung Trading Plan
    plan = calculate_trading_plan(df_calc, signals, mock_mtf)
    print("\n--- OUTPUT TRADING PLAN SPOT ---")
    print(f"Side: {plan['side']}")
    print(f"Rekomendasi: {plan['action_title']}")
    print(f"Area Jual Pasar: Rp {plan['entry']:,}")
    print(f"Target Support Penurunan (S1): Rp {plan['tp1']:,} (-{plan['potential_drop_pct']}%)")
    print(f"Invalidation Cut Loss (SL): Rp {plan['sl']:,}")

    # 3. Format Caption Telegram
    alert_text = format_telegram_alert("BBCA", df_calc, plan, mock_mtf)
    print("\n--- CAPTION TELEGRAM ---")
    print(alert_text)
