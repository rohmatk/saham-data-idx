from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import inspect

from etl import bootstrap_sample_data, get_database_url, get_engine, list_quality_issues, run_pipeline_from_upload
from etl.config import BASE_DIR, BRONZE_DIR, RAW_PDF_DIR, SILVER_DIR

st.set_page_config(page_title='KSEI ETL to dbase.id', layout='wide')

engine = get_engine()
DB_URL = get_database_url()
PROJECT_ROOT = BASE_DIR
SAMPLE_PDFS = [
    Path('/mnt/data/Data KSEI terkait Kepemilikan Saham Perusahaan Tercatat Berdasarkan Tipe Investor per 31 Maret 2026.pdf'),
    Path('/mnt/data/Pemegang_Saham_di_atas_1%_02_04_2026.pdf'),
    Path('/mnt/data/Pemegang_Saham_di_atas_5%_02_04_2026.pdf'),
]

inspector = inspect(engine)
if not inspector.has_table('clean_investor_type_summary'):
    bootstrap_sample_data(engine, [p for p in SAMPLE_PDFS if p.exists()])


def load_table(table_name: str) -> pd.DataFrame:
    try:
        return pd.read_sql_table(table_name, engine)
    except Exception:
        return pd.DataFrame()


st.title('KSEI Pipeline: PDF -> CSV Mentah -> Pembersihan -> dbase.id -> Analisis')

pipeline_tab, quality_tab, analysis_tab = st.tabs(['ETL Pipeline', 'Data Quality', 'Analisis & Visualisasi'])

with pipeline_tab:
    st.subheader('1) Upload PDF mentah')
    uploaded_files = st.file_uploader('Pilih PDF KSEI', type=['pdf'], accept_multiple_files=True)

    if st.button('Jalankan pipeline', type='primary', disabled=not uploaded_files):
        results = []
        for uploaded_file in uploaded_files or []:
            try:
                res = run_pipeline_from_upload(uploaded_file, engine)
                results.append({
                    'source_file': res.source_file,
                    'report_type': res.report_type,
                    'raw_rows': res.raw_rows,
                    'clean_rows': res.clean_rows,
                    'raw_csv': Path(res.raw_csv).relative_to(PROJECT_ROOT).as_posix(),
                    'clean_csv': Path(res.clean_csv).relative_to(PROJECT_ROOT).as_posix(),
                    'database_target': res.database_target,
                })
            except Exception as exc:
                results.append({'source_file': uploaded_file.name, 'report_type': 'error', 'raw_rows': 0, 'clean_rows': 0, 'raw_csv': '-', 'clean_csv': str(exc), 'database_target': '-'})
        st.success('Pipeline selesai dijalankan.')
        st.dataframe(pd.DataFrame(results), use_container_width=True)

    c1, c2, c3 = st.columns(3)
    c1.markdown(f'**RAW PDF**  \n`{RAW_PDF_DIR.relative_to(PROJECT_ROOT).as_posix()}`')
    c2.markdown(f'**BRONZE / CSV mentah**  \n`{BRONZE_DIR.relative_to(PROJECT_ROOT).as_posix()}`')
    c3.markdown(f'**SILVER / CSV bersih**  \n`{SILVER_DIR.relative_to(PROJECT_ROOT).as_posix()}`')

    st.markdown(
        '''
        **Urutan proses**
        1. PDF mentah masuk ke `data/raw/pdfs`
        2. Tabel hasil ekstraksi disimpan sebagai CSV mentah di `data/bronze/raw_csv`
        3. Cleaning dan standardisasi disimpan sebagai CSV bersih di `data/silver/clean_csv`
        4. CSV bersih di-upsert ke database PostgreSQL dbase.id
        5. Dashboard membaca tabel hasil bersih dari database
        '''
    )

    st.subheader('Preview file hasil ETL')
    raw_files = sorted(BRONZE_DIR.glob('*.csv'))[-5:]
    clean_files = sorted(SILVER_DIR.glob('*.csv'))[-5:]
    left, right = st.columns(2)
    with left:
        st.write('CSV mentah terbaru')
        st.write([p.name for p in raw_files] or ['Belum ada'])
    with right:
        st.write('CSV bersih terbaru')
        st.write([p.name for p in clean_files] or ['Belum ada'])

with quality_tab:
    st.subheader('2) Pemeriksaan kualitas data setelah cleaning')
    options = {
        'Summary tipe investor': 'clean_investor_type_summary',
        'Pemegang saham >1%': 'clean_shareholders_above_1pct',
        'Pemegang saham >5%': 'clean_shareholders_above_5pct',
    }
    selected_label = st.selectbox('Pilih dataset', list(options.keys()))
    table_name = options[selected_label]
    df = load_table(table_name)
    if df.empty:
        st.info('Belum ada data.')
    else:
        report_type = table_name.replace('clean_', '')
        checks = list_quality_issues(df, report_type)
        st.dataframe(checks, use_container_width=True)
        st.write('Sample data bersih')
        st.dataframe(df.head(50), use_container_width=True)

with analysis_tab:
    st.subheader('3) Analisis dan visualisasi')
    summary_df = load_table('clean_investor_type_summary')
    above1_df = load_table('clean_shareholders_above_1pct')
    above5_df = load_table('clean_shareholders_above_5pct')

    m1, m2, m3 = st.columns(3)
    m1.metric('Kode saham summary', int(summary_df['share_code'].nunique()) if not summary_df.empty else 0)
    m2.metric('Baris pemegang >1%', int(len(above1_df)))
    m3.metric('Baris pemegang >5%', int(len(above5_df)))

    if not summary_df.empty:
        summary_df['report_date'] = pd.to_datetime(summary_df['report_date'], errors='coerce')
        valid_dates = sorted(summary_df['report_date'].dropna().dt.date.unique())
        picked_date = st.selectbox('Tanggal report summary', valid_dates)
        date_df = summary_df[summary_df['report_date'].dt.date == picked_date].copy()
        date_df['domestic_gte5_total'] = date_df[[f'domestic_{c}_gte_5' for c in ['cp', 'fd', 'ib', 'id', 'is', 'mf', 'ot', 'pf', 'sc']]].sum(axis=1)
        date_df['foreign_gte5_total'] = date_df[[f'foreign_{c}_gte_5' for c in ['cp', 'fd', 'ib', 'id', 'is', 'mf', 'ot', 'pf', 'sc']]].sum(axis=1)
        chart_df = date_df.nlargest(15, 'number_of_shares')[['share_code', 'number_of_shares', 'domestic_gte5_total', 'foreign_gte5_total']]
        fig1 = px.bar(chart_df, x='share_code', y='number_of_shares', title='Top 15 kode saham berdasarkan jumlah saham')
        st.plotly_chart(fig1, use_container_width=True)
        stacked = chart_df.melt(id_vars='share_code', value_vars=['domestic_gte5_total', 'foreign_gte5_total'], var_name='segment', value_name='shares')
        fig2 = px.bar(stacked, x='share_code', y='shares', color='segment', barmode='stack', title='Komposisi >=5% domestik vs asing')
        st.plotly_chart(fig2, use_container_width=True)

    if not above1_df.empty:
        st.markdown('**Top holder untuk dataset >1%**')
        above1_df['report_date'] = pd.to_datetime(above1_df['report_date'], errors='coerce')
        date_opts = sorted(above1_df['report_date'].dropna().dt.date.unique())
        picked = st.selectbox('Tanggal dataset >1%', date_opts, key='a1d')
        code_opts = sorted(above1_df.loc[above1_df['report_date'].dt.date == picked, 'share_code'].dropna().unique())
        code = st.selectbox('Kode saham >1%', code_opts, key='a1c')
        filt = above1_df[(above1_df['report_date'].dt.date == picked) & (above1_df['share_code'] == code)].sort_values('percentage', ascending=False)
        fig3 = px.bar(filt.head(20), x='investor_name', y='percentage', color='local_foreign', title=f'Top pemegang saham >1% - {code}')
        st.plotly_chart(fig3, use_container_width=True)

    if not above5_df.empty:
        st.markdown('**Perubahan holder untuk dataset >5%**')
        above5_df['report_date'] = pd.to_datetime(above5_df['report_date'], errors='coerce')
        date_opts = sorted(above5_df['report_date'].dropna().dt.date.unique())
        picked = st.selectbox('Tanggal dataset >5%', date_opts, key='a5d')
        code_opts = sorted(above5_df.loc[above5_df['report_date'].dt.date == picked, 'share_code'].dropna().unique())
        code = st.selectbox('Kode saham >5%', code_opts, key='a5c')
        filt = above5_df[(above5_df['report_date'].dt.date == picked) & (above5_df['share_code'] == code)].copy()
        agg = (
            filt.groupby(['shareholder_name', 'local_foreign_status'], dropna=False)
            .agg(investor_pct_current_date=('investor_pct_current_date', 'max'), change_in_shares=('change_in_shares', 'sum'))
            .reset_index()
            .sort_values('investor_pct_current_date', ascending=False)
        )
        fig4 = px.bar(agg.head(20), x='shareholder_name', y='investor_pct_current_date', color='local_foreign_status', title=f'Holder >5% - {code}')
        st.plotly_chart(fig4, use_container_width=True)
        fig5 = px.bar(agg.head(20), x='shareholder_name', y='change_in_shares', color='local_foreign_status', title='Perubahan saham vs periode sebelumnya')
        st.plotly_chart(fig5, use_container_width=True)
