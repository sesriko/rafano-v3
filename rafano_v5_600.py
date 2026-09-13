
"""
RAFANO V5 600 SAHAM - Opsi B Full IDX
- Auto fetch 600 ticker dari GitHub list + fallback 600 hardcode
- OHLCV 2 tahun + broker_summary
- Chunk 100 saham per commit biar gak timeout
"""
import os, time, sqlite3, datetime, requests, io, csv
import yfinance as yf

DB_PATH = "rafano.db"
ARJUM_BASE = "https://stock.arjum.com/api"
ARJUM_KEY = os.getenv("ARJUM_API_KEY") or ""

# --- 1. Ambil list 600 dari internet ---
def get_600_tickers():
    urls = [
        "https://raw.githubusercontent.com/sarrahman-me/idx-ranker/main/list-saham.csv",
        "https://raw.githubusercontent.com/boyank28/saham-mcp/main/data/tickers.txt",
        "https://raw.githubusercontent.com/bimamaulana/IDX-Stock-Data/master/idx_tickers.txt",
    ]
    tickers = []
    for url in urls:
        try:
            print(f"Trying {url}")
            r = requests.get(url, timeout=10)
            if r.status_code==200 and len(r.text)>100:
                # CSV semicolon
                if ";" in r.text:
                    f = io.StringIO(r.text)
                    reader = csv.reader(f, delimiter=";")
                    for row in reader:
                        if row and row[0].strip().isalpha() and len(row[0])<=5:
                            tickers.append(row[0].strip().upper())
                else:
                    for line in r.text.splitlines():
                        t = line.strip().upper().replace(".JK","")
                        if t.isalpha() and 3<=len(t)<=5:
                            tickers.append(t)
                if len(tickers)>300:
                    print(f"Got {len(tickers)} from {url}")
                    return sorted(list(set(tickers)))[:600]
        except Exception as e:
            print(f"Fail {url} {e}")

    # FALLBACK 600 hardcode - LQ45 + IDX80 + Kompas100 + papan utama
    fallback = """AALI,ABBA,ABDA,ABMM,ACES,ACST,ADCP,ADHI,ADMG,ADMR,ADRO,AGAR,AGII,AGRO,AHAP,AIMS,AKRA,ALDO,ALKA,ALMI,AMAG,AMAN,AMAR,AMMN,AMMS,AMRT,ANJT,ANTM,APEX,APIC,APLN,ARCI,ARGO,ARKO,ARNA,ARTA,ARTO,ASII,ASRI,ASRM,ATAP,ATLA,ATPK,AUTO,AVIA,AYLS,BABP,BACA,BAJA,BANK,BAPA,BAYU,BBCA,BBHI,BBKP,BBMD,BBRI,BBNI,BBTN,BCAP,BCIP,BDMN,BEKS,BELL,BEST,BFIN,BHIT,BINA,BIPI,BIRD,BISI,BJBR,BJTM,BKSL,BMRI,BMTR,BNGA,BNLI,BOGA,BOLT,BPTR,BRAM,BREN,BRIS,BRMS,BRPT,BSDE,BSSR,BTPS,BUKA,BULL,BUMI,BVIC,BWPT,BYAN,CASA,CBMF,CEKA,CENT,CFIN,CITA,CMNP,CMRY,COAL,CPIN,CSAP,CTRA,CUAN,DART,DEWA,DGIK,DILD,DMAS,DOID,DSNG,DSSA,DUTI,ECII,ELSA,EMTK,ENRG,ERAA,ESSA,EXCL,FASW,FILM,FMII,FOOD,FREN,GAMA,GDST,GGRM,GJTL,GLOB,GMFI,GOLD,GOOD,GPRA,GULA,HATM,HERO,HMSP,HRUM,IATA,IBST,ICBP,ICON,IGAR,IKAN,IKBI,IMAS,IMPC,INAF,INCO,INDF,INDY,INKP,INPC,INTP,IPCM,IRRA,ISAT,ISSP,ITMG,JAWA,JAYA,JPFA,JSMR,KAEF,KBLI,KDSI,KIJA,KINO,KLBF,KOBX,KOCI,KOPI,KPIG,KRAS,LABA,LAND,LCKM,LINK,LION,LPCK,LPKR,LPPF,LSIP,LTLS,MAPI,MARK,MASA,MAYA,MBAP,MBMA,MCOL,MDKA,MEDC,MEGA,MFIN,MICE,MIKA,MINE,MITI,MKPI,MLBI,MLPL,MNCN,MPPA,MPMX,MRAT,MSKY,MTDL,MTEL,MTLA,MYOR,NCKL,NISP,NRCA,OBMD,OASA,OCAP,PADI,PALM,PANI,PANS,PBRX,PGAS,PGEO,PICO,PJAA,PNBN,PNIN,POLY,PPRE,PRDA,PTBA,PTPP,PTRO,PURA,RAJA,RALS,SCMA,SDMU,SIDO,SILO,SIMA,SMAR,SMBR,SMDR,SMGR,SMRA,SMSM,SOCI,SOHO,SRTG,SSIA,SSMS,STAR,TARA,TBIG,TBLA,TCID,TELE,TINS,TKIM,TMAS,TOBA,TOWR,TPIA,TRAM,TRIM,TRIN,TRST,ULTJ,UNTR,UNVR,VICO,VIVA,VOKS,WAPO,WEGE,WIKA,WINS,WOMF,WOOD,WSBP,WSKT,WTON,YELO,YPAS,ZATA,ZONE,ADXA,AMMS,AMOR,ATLA,BREN,CUAN,CUAT,DEFI,DOOH,EMAS,GOTO,GTSI,HATM,HRTA,IBFN,INET,JGLE,KBRI,KEEN,KETR,KICI,KMDS,MBSS,MDLA,MUTU,NASI,NPGF,PGEO,PTDU,PTIS,RATU,RUNS,SAMF,SMKM,SGER,TNCA,UVCR,VKTR,WIFI,BRMS,BIPI,BNLI,DEWA,ELSA,BEKS,BTPS,BKSL,BELL,BEST,BFIN,BGTG,BHIT,BIKA,BINA,BIPP,BKDP,BLTA,BMAS,BMPP,BMSR,BNBA,BOSS,BPFI,BPII,BRNA,BSIM,BSML,BTON,BTPN,BUAH,BUAN,BUKK,BUVA,BWPT,CASS,CBRE,CCIT,CINT,CLAY,CMPP,CMNT,CNKO,COCO,CPRO,CSIS,CUAT,DNET,DPNS,DSFI,DYAN,EAST,ELTY,EMDE,ENZO,EPMT,ERTX,ESTA,FAPA,FIRE,FISH,FORU,FORZ,FWCT,GEMA,GEMS,GLOB,GLVA,GMTD,GOLL,GPSA,GWSA,HDFA,HITS,HKMU,HOMI,HOPE,HRME,IBFN,IDPR,IFII,IFSH,IIKP,IKAI,IMJS,INAI,INCF,INCI,INDO,INDR,INDS,INDX,INPP,INPS,INRU,INTA,INTD,IPAC,IPCC,IPOL,IPTV,ITMA,JAST,JECC,JKSW,JPFA,JSPT,KBLM,KBLV,KBRI,KDSI,KEEN,KETR,KIAS,KICI,KIJA,KINO,KKGI,KMTR,KOIN,KONM,KPAL,KRAS,KREN,LABU,LCGP,LEAD,LFLO,LMAS,LMPI,LMSH,LPLI,LPPS,LRNA,MABA,MAGP,MAMI,MARI,MASA,MAYA,MBSS,MCAS,MKPI,MLIA,MMLP,MPOW,MPRO,MREI,MSIN,MTPS,MTRA,MYOH,MYRX,NAYZ,NELY,NFCX,NIKL,NOBU,NZIA,OMED,OILS,OKAS,PALM,PANI,PANR,PBID,PBSA,PBOY,PDES,PEGE,PGJO,PKPK,PLIN,PNBS,PNLF,PNSE,POLA,POLL,POLLU,PRIM,PSAB,PSDN,PSGO,PTDU,PTIS,PUDP,PURI,PZZA,RAAM,RANC,RBMS,RCCC,RELI,RIGS,RMKE,RMKO,RONY,ROTI,RUIS,SAME,SDPC,SIMA,SIPD,SKBM,SKLT,SKRN,SMAR,SMCA,SMIL,SMMA,SMRU,SOSS,SPMA,SPTO,SSTM,STAA,SWAT,TAXI,TBMS,TFCO,TGKA,TLDN,TMPI,TPMA,TRIL,TRIS,TRUS,UNIC,UVCR,VINS,VKTR,WAPO,WICO,WIMA,WOMF,YULI,ZYRX""".split(",")
    return sorted(list(set([x.strip() for x in fallback if x.strip()])))[:600]

SYMBOLS = get_600_tickers()
print(f"TOTAL SYMBOLS: {len(SYMBOLS)}")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS ohlcv (symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY(symbol, date))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS broker_summary (symbol TEXT, date TEXT, broker_code TEXT, bval REAL, sval REAL, nval REAL, bvol INTEGER, svol INTEGER, PRIMARY KEY(symbol, date, broker_code))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS bandar_daily (symbol TEXT, date TEXT PRIMARY KEY, foreign_net_1w REAL, foreign_str_1w TEXT, accum_type TEXT, total_bval REAL, total_sval REAL)""")
    conn.commit(); conn.close()

def update_ohlcv():
    conn = sqlite3.connect(DB_PATH)
    for i, sym in enumerate(SYMBOLS):
        try:
            ticker = yf.Ticker(f"{sym}.JK")
            hist = ticker.history(period="2y", auto_adjust=False)
            if hist.empty:
                print(f"{i+1}/{len(SYMBOLS)} {sym} empty")
                continue
            hist = hist.reset_index()
            for _, row in hist.iterrows():
                conn.execute("INSERT OR REPLACE INTO ohlcv VALUES (?,?,?,?,?,?,?)",
                    (sym, row['Date'].strftime("%Y-%m-%d"), float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']), int(row['Volume'])))
            if i%50==0:
                conn.commit()
                print(f"OHLCV {i+1}/{len(SYMBOLS)} {sym} {len(hist)} rows")
        except Exception as e:
            print(f"{sym} OHLCV err {e}")
        time.sleep(0.2)
    conn.commit(); conn.close()

def update_bandar():
    if not ARJUM_KEY:
        print("ARJUM_API_KEY kosong")
        return
    conn = sqlite3.connect(DB_PATH)
    end = datetime.date.today()
    start = end - datetime.timedelta(days=12)
    for i, sym in enumerate(SYMBOLS):
        try:
            url = f"{ARJUM_BASE}/broker-summary/{sym}"
            params = {"start_date": start.strftime("%Y-%m-%d"), "end_date": end.strftime("%Y-%m-%d"), "flow": "all", "all_data": "true", "broker_limit": 15}
            headers = {"X-API-Key": ARJUM_KEY}
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code==429:
                print(f"429 {sym} sleep 7s")
                time.sleep(7)
                continue
            if r.status_code!=200:
                time.sleep(1.2); continue
            data = r.json()
            brokers = data.get('brokers', [])
            if not brokers:
                time.sleep(1.2); continue
            total_b = sum([float(b.get('bval') or 0) for b in brokers])
            total_s = sum([float(b.get('sval') or 0) for b in brokers])
            net_1w = sum([float(b.get('nval') or 0) for b in brokers])
            def fmt(v):
                if abs(v)>=1e12: return f"{'+' if v>0 else ''}{v/1e12:.2f}T"
                elif abs(v)>=1e9: return f"{'+' if v>0 else ''}{v/1e9:.2f}B"
                elif abs(v)>=1e6: return f"{'+' if v>0 else ''}{v/1e6:.0f}M"
                else: return f"{int(v)}"
            if net_1w>5e9: atype="BIG ACCUM"
            elif net_1w>1e9: atype="ACCUM"
            elif net_1w<-5e9: atype="BIG DIST"
            elif net_1w<-1e9: atype="DIST"
            else: atype="NEUTRAL"
            today = datetime.date.today().strftime("%Y-%m-%d")
            for b in brokers:
                conn.execute("INSERT OR REPLACE INTO broker_summary VALUES (?,?,?,?,?,?,?,?)",
                    (sym, today, b.get('broker_code'), float(b.get('bval') or 0), float(b.get('sval') or 0), float(b.get('nval') or 0), int(b.get('bvol') or 0), int(b.get('svol') or 0)))
            conn.execute("INSERT OR REPLACE INTO bandar_daily VALUES (?,?,?,?,?,?,?)",
                (sym, today, net_1w, fmt(net_1w), atype, total_b, total_s))
            if i%20==0:
                conn.commit()
            print(f"BANDAR {i+1}/{len(SYMBOLS)} {sym} {fmt(net_1w)} {atype}")
            time.sleep(1.3)
        except Exception as e:
            print(f"{sym} bandar err {e}")
            time.sleep(1.3)
    conn.commit(); conn.close()

if __name__=="__main__":
    init_db()
    update_ohlcv()
    update_bandar()
    print("DONE 600")
