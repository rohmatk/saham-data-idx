# KSEI ETL to dbase.id + Streamlit

Struktur project ini mengikuti alur yang Anda minta:

1. **PDF mentah** disimpan dulu
2. **Ekstraksi ke CSV mentah**
3. **Pembersihan / standardisasi data**
4. **Load ke database dbase.id (PostgreSQL)**
5. **Analisis dan visualisasi di Streamlit**

## Struktur folder

```text
ksei_dbase_streamlit/
├── app.py
├── README.md
├── requirements.txt
├── etl/
│   ├── __init__.py
│   ├── config.py
│   └── pipeline.py
├── sql/
│   └── schema.sql
└── data/
    ├── raw/
    │   └── pdfs/
    ├── bronze/
    │   └── raw_csv/
    ├── silver/
    │   └── clean_csv/
    ├── logs/
    └── sample/
```

## Tabel bersih yang di-load ke database

- `clean_investor_type_summary`
- `clean_shareholders_above_1pct`
- `clean_shareholders_above_5pct`
- `etl_ingestion_log`

## DATABASE_URL untuk dbase.id

DBASE.ID memakai PostgreSQL. Gunakan connection string PostgreSQL pada environment variable `DATABASE_URL`.

Contoh:

```bash
export DATABASE_URL="postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME"
```

## Jalankan lokal

```bash
pip install -r requirements.txt
streamlit run app.py
```

Jika `DATABASE_URL` belum diisi, aplikasi akan fallback ke SQLite lokal untuk development.

## Catatan dataset PDF

Project ini menyesuaikan 3 struktur file KSEI yang Anda upload:
- file ringkasan tipe investor domestik vs asing dengan kolom DATE, STOCK_CODE, NUMBER_OF_SHARES, kategori CP/FD/IB/ID/IS/MF/OT/PF/SC untuk bucket `<5%` dan `>=5%` serta `TOTAL SCRIPLESS`
- file pemegang saham di atas 1% dengan kolom DATE, SHARE_CODE, ISSUER_NAME, INVESTOR_NAME, INVESTOR_TYPE, LOCAL_FOREIGN, NATIONALITY, DOMICILE, HOLDINGS_SCRIPLESS, HOLDINGS_SCRIP, TOTAL_HOLDING_SHARES, PERCENTAGE
- file pemegang saham di atas 5% yang memuat perbandingan kepemilikan per 31-MAR-2026 dan 01-APR-2026 serta perubahan saham per investor
