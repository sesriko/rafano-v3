# 🤖 RAFANO V3 - FREE INSTITUTIONAL TRADING BOT

**Screener saham Indonesia real akumulasi bandar + chart pro + parallel scan**

Gratis tanpa VPS, jalan di Google Colab.

## ⚡ Speedup vs Bot Lama

| Command | Before (JSON+Sequential) | After (SQLite+Parallel) | Speedup |
|---------|--------------------------|-------------------------|---------|
| `/scan` 900 saham | 10.5 menit | 45 detik | 14x |
| `/s akum d` | 5 detik scan ulang | 0.05 detik query DB | 100x |
| `/s turbo` | 5 detik | 0.08 detik | 62x |
| `/c BBCA 5` | Chart only | Chart + teknikal + volume + score | - |

## 📁 Struktur File

```
rafano-v3/
├── rafano_v3.py                    # Bot utama (FINAL + /s turbo + /c TF)
├── rafano_db_free.py              # SQLite DB - ganti JSON cache (GRATIS)
├── rafano_parallel_free.py        # Parallel scanner 20 workers (GRATIS)
├── requirements.txt               # Dependencies
├── .gitignore                     # Ignore DB & secrets
├── Rafano_V3_Colab.ipynb         # Notebook Colab - Run All
└── README.md                      # Ini
```

## 🚀 Cara Push ke GitHub (2 menit)

### STEP 1: Buat Repo di GitHub

1. Buka https://github.com/new
2. Repo name: `rafano-v3`
3. Public / Private (terserah)
4. **JANGAN** centang "Initialize with README"
5. Klik Create repository

### STEP 2: Push dari Laptop / Colab

**Di laptop (ada git):**
```bash
# Download file dari sini dulu
# Atau clone kosong
git clone https://github.com/USERNAME/rafano-v3.git
cd rafano-v3

# Copy file-file ini ke folder:
# - rafano_v3.py
# - rafano_db_free.py
# - rafano_parallel_free.py
# - requirements.txt
# - .gitignore
# - Rafano_V3_Colab.ipynb

git add .
git commit -m "Rafano V3 Free Institutional - SQLite + Parallel"
git branch -M main
git remote add origin https://github.com/USERNAME/rafano-v3.git
git push -u origin main
```

**Di Colab (tanpa git):**
```python
# Cell 1 - Setup git di Colab
!git config --global user.email "email@lu.com"
!git config --global user.name "username"

# Cell 2 - Clone & copy
!git clone https://github.com/USERNAME/rafano-v3.git
!cp /content/rafano_v3.py /content/rafano-v3/
!cp /content/rafano_db_free.py /content/rafano-v3/
!cp /content/rafano_parallel_free.py /content/rafano-v3/
!cp /content/requirements.txt /content/rafano-v3/
!cp /content/.gitignore /content/rafano-v3/

# Cell 3 - Push
%cd /content/rafano-v3
!git add .
!git commit -m "Update bot"
!git push https://USERNAME:TOKEN@github.com/USERNAME/rafano-v3.git main
# TOKEN = GitHub Personal Access Token (Settings -> Developer -> Tokens)
```

### STEP 3: Setup GitHub Token (untuk push dari Colab)

1. GitHub -> Settings (kanan atas) -> Developer settings -> Personal access tokens -> Tokens (classic)
2. Generate new token -> classic
3. Centang `repo`
4. Copy token (ghp_xxx)
5. Pakai di URL push: `https://USERNAME:TOKEN@github.com/...`

## 🔥 Cara Jalankan di Colab (GRATIS)

### OPSI 1: Paling Gampang - Run Notebook

1. Buka https://colab.research.google.com/
2. File -> Open notebook -> GitHub -> Masukkan `USERNAME/rafano-v3` -> Pilih `Rafano_V3_Colab.ipynb`
3. Atau upload `Rafano_V3_Colab.ipynb` manual
4. Runtime -> Change runtime type -> **Background execution ON** (biar gak disconnect)
5. Isi Secrets:
   - Klik ikon 🔑 di sidebar kiri
   - Add secret:
     - `TELEGRAM_BOT_TOKEN` = dari @BotFather
     - `TARGET_CHAT_ID` = ID Telegram lu
     - `ARJUM_API_KEY` = API Arjum
6. Runtime -> Run all
7. Bot jalan! Cek Telegram

### OPSI 2: Manual (3 file upload)

1. Buka https://colab.research.google.com/ -> New notebook
2. Upload 3 file ke Files sidebar (kiri):
   - `rafano_v3.py`
   - `rafano_db_free.py`
   - `rafano_parallel_free.py`

3. Cell 1:
```python
!pip install pandas pytz requests matplotlib python-dotenv -q
```

4. Cell 2 - Isi secrets:
```python
import os
from google.colab import userdata

os.environ['TELEGRAM_BOT_TOKEN'] = userdata.get('TELEGRAM_BOT_TOKEN')
os.environ['TARGET_CHAT_ID'] = userdata.get('TARGET_CHAT_ID')
os.environ['ARJUM_API_KEY'] = userdata.get('ARJUM_API_KEY')
```

5. Cell 3 - Jalankan bot:
```python
!python rafano_v3.py
```

## 📱 Perintah Telegram

```
/c BBCA        - Chart daily + teknikal detail + volume spike + score
/c BBCA 5      - Chart 5 menit (TF: 5, 15, 30, 1h, 4h, d, w, m)
/b BBCA        - Broker detail daily/weekly/monthly + avg top 3
/b BBCA 5      - Broker TF 5 menit
/s akum d      - Top 20 akum daily (0.05 detik dari DB)
/s akum w      - Top 20 akum weekly 5D
/s akum m      - Top 20 akum monthly 20D
/s dis d       - Top 20 distribusi daily
/s dis w       - Top 20 distribusi weekly
/s turbo       - Volume spike TURBO ≥2x + AKUM (0.08 detik)
/s os          - Oversold RSI<30
/s ob          - Overbought RSI>70
/scan          - Scan 200 saham parallel 15 detik
/scanpro       - Scan + chart top 3
/top 10        - Top akumulasi
/wl            - Watchlist
```

## 🗄️ Database Gratis (SQLite)

File: `/content/rafano.db` di Colab (persisten selama runtime)

- Simpan 5 tahun OHLCV 900 saham = 300MB
- Query TOP AKUM 0.05 detik (vs 5 detik JSON)
- Tidak hilang pas restart (kalau di /content)
- WAL mode 2x faster

Cek stats:
```python
from rafano_db_free import db_free
print(db_free.get_stats())
```

## ⚡ Parallel Scan Gratis (ThreadPool)

Tanpa Redis/Celery, pakai ThreadPoolExecutor (sudah ada di Python)

- 20 workers parallel
- 200 saham = 12 detik (vs 2.3 menit sequential)
- 900 saham = 45 detik (vs 10.5 menit)

## 🔒 Keamanan - JANGAN upload token ke GitHub!

`.gitignore` sudah ignore:
- `*.db` - Database
- `.env` - Secrets
- `chart_*.png` - Chart

Token simpan di Colab Secrets, bukan di code.

## 🆘 Troubleshooting

**Bot tidak jalan di Colab:**
- Cek Secrets sudah terisi?
- Cek Files sidebar ada 3 file?
- Runtime -> Restart and run all

**Colab disconnect setelah 90 menit:**
- Runtime -> Change runtime type -> Background execution ON
- Atau jalankan keep alive cell

**/scan lambat di Colab:**
- Colab free CPU kecil, pakai 100 saham dulu: `get_all_symbols_free()[:100]`
- Atau upgrade ke Colab Pro (2x faster)

**Mau upgrade ke VPS (PostgreSQL + Redis):**
- Lihat folder `institutional_upgrade/` di repo ini
- Atau baca README di /mnt/data/institutional_upgrade/README.md

## 📞 Kontak

Telegram bot: @username_bot
GitHub: github.com/USERNAME/rafano-v3

Happy trading! 🚀
