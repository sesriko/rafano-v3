"""
RAFANO V3 FREE INSTITUTIONAL - Parallel Scanner tanpa Redis/Celery
Pakai ThreadPoolExecutor (sudah ada di Python, gratis)

Before: 900 saham sequential 10 menit
After: 900 saham parallel 20 workers 30 detik

GRATIS, jalan di Colab
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
from datetime import datetime
import pytz

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')

# Lock untuk thread safety
lock = threading.Lock()

def scan_single_stock_free(symbol, get_history_pro_func, get_broker_multi_tf_func):
    """Scan 1 saham - thread safe"""
    try:
        df = get_history_pro_func(symbol, limit=50, timeframe="1d")
        if df is None or len(df) < 20:
            return None
        
        multi = get_broker_multi_tf_func(symbol, df)
        net_d = multi.get('net_d',0)
        status_d = multi.get('status_d','')
        
        # Score sederhana
        score = 0
        if status_d == "AKUM":
            score += 40
            if net_d > 500_000_000:
                score += 30
            if net_d > 1_000_000_000:
                score += 20
        
        if score < 30:
            return None
        
        return {
            'symbol': symbol,
            'close': float(df['Close'].iloc[-1]) if df is not None else 0,
            'change_pct': 0,
            'score': score,
            'score_label': 'REAL',
            'multi_tf': multi,
            'brokers': multi.get('brokers',[]),
            'trading_plan': {},
            'last_scan': datetime.now(TIMEZONE_WIB).isoformat()
        }
    except Exception as e:
        # print(f"❌ Error {symbol}: {e}")
        return None

def scan_all_parallel_free(symbols, get_history_pro_func, get_broker_multi_tf_func, max_workers=20):
    """
    Scan parallel GRATIS pakai ThreadPoolExecutor
    900 saham / 20 workers = 45 batch * 0.7 detik = ~31 detik
    """
    start = time.time()
    print(f"🚀 FREE Parallel scan {len(symbols)} saham dengan {max_workers} workers...")
    
    results = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit semua
        future_to_symbol = {
            executor.submit(scan_single_stock_free, sym, get_history_pro_func, get_broker_multi_tf_func): sym 
            for sym in symbols
        }
        
        for future in as_completed(future_to_symbol):
            sym = future_to_symbol[future]
            try:
                result = future.result(timeout=30)
                if result:
                    with lock:
                        results.append(result)
                completed += 1
                if completed % 50 == 0:
                    print(f"  Progress: {completed}/{len(symbols)} ({completed/len(symbols)*100:.0f}%) - {len(results)} valid")
            except Exception as e:
                print(f"❌ Error {sym}: {e}")
    
    elapsed = time.time() - start
    results = sorted(results, key=lambda x: x.get('multi_tf',{}).get('net_d',0), reverse=True)
    
    print(f"✅ FREE Parallel scan selesai: {len(symbols)} saham dalam {elapsed:.1f} detik")
    print(f"   Valid: {len(results)} | Speedup: {len(symbols)*0.7/elapsed:.1f}x vs sequential")
    
    # Simpan ke SQLite (gratis)
    try:
        from rafano_db_free import db_free
        db_free.save_scan_cache(results)
        print(f"✅ Saved {len(results)} ke SQLite {db_free.db_path}")
    except Exception as e:
        print(f"⚠ Gagal save ke DB: {e}")
    
    return results

def get_all_symbols_free():
    """Ambil list saham IDX - bisa dari file atau API"""
    # List saham LQ45 + IDX80 + custom - 900 saham
    # Untuk demo, pakai 100 saham dulu biar cepat di Colab
    base = [
        "BBCA","BBRI","BMRI","BBNI","BRIS","BREN","CUAN","AMMN","ANTM","ADRO",
        "TLKM","ASII","UNVR","ICBP","INDF","KLBF","SIDO","MDKA","PGAS","PTBA",
        "INCO","INCO","BRPT","TPIA","GOTO","BUKA","EMTK","ACES","AKRA","ARTO",
        "BANK","BBTN","BMTR","BRMS","CPIN","DOID","ELSA","ESSA","EXCL","HRUM",
        "INDY","ITMG","JPFA","JSMR","LSIP","MAPI","MEDC","MIKA","MNCN","PGAS",
        "PTBA","SCMA","SMGR","TBIG","TKIM","TPIA","UNTR","WIKA","WSKT","WTON"
    ]
    # Duplicate untuk simulasi 900
    return (base * 15)[:900]  # 900 saham

if __name__ == "__main__":
    # Test tanpa dependensi rafano_v3 - mock functions
    def mock_history(symbol, limit=50, timeframe="1d"):
        import pandas as pd
        import numpy as np
        import random
        dates = pd.date_range(end=datetime.now(), periods=limit)
        df = pd.DataFrame({
            'Open': [random.randint(100,10000) for _ in range(limit)],
            'High': [random.randint(100,10000) for _ in range(limit)],
            'Low': [random.randint(100,10000) for _ in range(limit)],
            'Close': [random.randint(100,10000) for _ in range(limit)],
            'Volume': [random.randint(1000000,10000000) for _ in range(limit)],
        }, index=dates)
        df['High'] = df[['Open','Close']].max(axis=1) + 10
        df['Low'] = df[['Open','Close']].min(axis=1) - 10
        return df
    
    def mock_broker(symbol, df):
        import random
        return {
            'status_d': 'AKUM' if random.random()>0.5 else 'DIST',
            'net_d': random.randint(-1_000_000_000, 2_000_000_000),
            'buy_d': random.randint(100_000_000, 1_000_000_000),
            'sell_d': random.randint(50_000_000, 500_000_000),
            'avg_d': random.randint(100,5000),
            'status_5d': 'AKUM',
            'net_5d': random.randint(-1_000_000_000, 2_000_000_000),
            'status_20d': 'AKUM',
            'net_20d': random.randint(-2_000_000_000, 5_000_000_000),
            'brokers': []
        }
    
    symbols = get_all_symbols_free()[:50]  # Test 50 saham
    results = scan_all_parallel_free(symbols, mock_history, mock_broker, max_workers=10)
    print(f"\nTop 5 results:")
    for r in results[:5]:
        print(f"  {r['symbol']} - Net {r['multi_tf']['net_d']}")
