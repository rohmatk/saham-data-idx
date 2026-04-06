from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import pdfplumber
from sqlalchemy import inspect, text

from etl.config import BRONZE_DIR, LOG_DIR, RAW_PDF_DIR, SILVER_DIR

SUMMARY_CATEGORIES = ['cp', 'fd', 'ib', 'id', 'is', 'mf', 'ot', 'pf', 'sc']


@dataclass
class PipelineResult:
    source_file: str
    report_type: str
    table_name: str
    raw_rows: int
    clean_rows: int
    raw_csv: str
    clean_csv: str
    database_target: str


REPORT_MAP = {
    'summary': {
        'table_name': 'clean_investor_type_summary',
        'raw_prefix': 'raw_investor_type_summary',
        'clean_prefix': 'clean_investor_type_summary',
    },
    'above_1pct': {
        'table_name': 'clean_shareholders_above_1pct',
        'raw_prefix': 'raw_shareholders_above_1pct',
        'clean_prefix': 'clean_shareholders_above_1pct',
    },
    'above_5pct': {
        'table_name': 'clean_shareholders_above_5pct',
        'raw_prefix': 'raw_shareholders_above_5pct',
        'clean_prefix': 'clean_shareholders_above_5pct',
    },
}


def parse_number(value, decimal_hint: bool = False):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if text == '' or text.lower() in {'none', 'nan'}:
        return None

    text = text.replace(' ', '')
    if decimal_hint:
        if ',' in text and '.' not in text:
            text = text.replace(',', '.')
        elif '.' in text and ',' in text:
            text = text.replace('.', '').replace(',', '.')
        try:
            out = float(text)
            return int(out) if out.is_integer() else out
        except Exception:
            pass

    if ',' in text and '.' in text:
        if text.rfind(',') > text.rfind('.'):
            text = text.replace('.', '').replace(',', '.')
        else:
            text = text.replace(',', '')
    elif ',' in text:
        if text.count(',') == 1 and len(text.split(',')[-1]) <= 2:
            text = text.replace(',', '.')
        else:
            text = text.replace(',', '')
    elif '.' in text:
        if text.count('.') > 1:
            text = text.replace('.', '')
        else:
            rhs = text.split('.')[-1]
            if len(rhs) == 3 and text.split('.')[0].isdigit():
                text = text.replace('.', '')

    try:
        out = float(text)
        return int(out) if out.is_integer() else out
    except Exception:
        return value


MONTH_FORMATS = ('%d-%b-%Y', '%d-%B-%Y', '%d-%b-%y')


def standardize_date_token(value: str | None):
    if value is None:
        return None
    value = str(value).strip()
    if value == '':
        return None
    for fmt in MONTH_FORMATS:
        try:
            return pd.to_datetime(value, format=fmt)
        except Exception:
            continue
    try:
        return pd.to_datetime(value)
    except Exception:
        return None


def parse_date_from_header(text: str | None):
    if not text:
        return None
    match = re.search(r'(\d{2}-[A-Z]{3}-\d{4})', str(text).upper())
    if not match:
        return None
    return pd.to_datetime(match.group(1), format='%d-%b-%Y')


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = (
                out[col]
                .fillna('')
                .astype(str)
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )
    return out


def stable_row_hash(row: pd.Series, columns: Iterable[str]) -> str:
    payload = '||'.join('' if pd.isna(row.get(col)) else str(row.get(col)) for col in columns)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def detect_report_type(filename: str, first_page_text: str) -> str:
    lower_name = filename.lower()
    lower_text = first_page_text.lower()

    if 'tipe investor' in lower_name or 'domestic foreign' in lower_text:
        return 'summary'
    if 'atas_5' in lower_name or 'diatas 5%' in lower_text or 'di atas 5%' in lower_text:
        return 'above_5pct'
    if 'atas_1' in lower_name or '[publik]' in lower_text:
        return 'above_1pct'
    raise ValueError(f'Tipe PDF tidak dikenali untuk file: {filename}')


RAW_SUMMARY_COLUMNS = ['date', 'share_code', 'number_of_shares']
for group in ['domestic', 'foreign']:
    for cat in SUMMARY_CATEGORIES:
        RAW_SUMMARY_COLUMNS.extend([f'{group}_{cat}_lt_5_raw', f'{group}_{cat}_gte_5_raw'])
RAW_SUMMARY_COLUMNS.append('total_scripless_raw')


def extract_summary_raw(pdf_path: str | Path) -> pd.DataFrame:
    rows: list[list] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            for row in table:
                if not row:
                    continue
                if row[0] in (None, '', 'DATE'):
                    continue
                first = str(row[0]).strip()
                if re.match(r'\d{2}-[A-Za-z]{3}-\d{4}$', first):
                    fixed = list(row[:40])
                    if len(fixed) < 40:
                        fixed += [None] * (40 - len(fixed))
                    rows.append(fixed)
    raw_df = pd.DataFrame(rows, columns=RAW_SUMMARY_COLUMNS)
    return clean_text_columns(raw_df)


RAW_ABOVE1_COLUMNS = [
    'date', 'share_code', 'issuer_name', 'investor_name', 'investor_type', 'local_foreign',
    'nationality', 'domicile', 'holdings_scripless_raw', 'holdings_scrip_raw',
    'total_holding_shares_raw', 'percentage_raw'
]


def extract_above_1pct_raw(pdf_path: str | Path) -> pd.DataFrame:
    rows: list[list] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            for row in table:
                if not row or len(row) < 12:
                    continue
                first = str(row[0]).strip() if row[0] is not None else ''
                if first in {'DATE', '[PUBLIK]'} or first.startswith('*Penafian') or first.startswith('*Disclaimer'):
                    continue
                if re.match(r'\d{2}-[A-Za-z]{3}-\d{4}$', first):
                    rows.append(list(row[:12]))
    raw_df = pd.DataFrame(rows, columns=RAW_ABOVE1_COLUMNS)
    return clean_text_columns(raw_df)


RAW_ABOVE5_COLUMNS = [
    'no_raw', 'share_code', 'issuer_name', 'securities_account_holder', 'shareholder_name',
    'securities_account_name', 'address_line_1', 'address_line_2', 'nationality', 'domicile',
    'local_foreign_status', 'shares_prev_date_raw', 'investor_total_prev_date_raw',
    'investor_pct_prev_date_raw', 'shares_current_date_raw', 'investor_total_current_date_raw',
    'investor_pct_current_date_raw', 'change_in_shares_raw'
]


def extract_above_5pct_raw(pdf_path: str | Path) -> pd.DataFrame:
    rows: list[list] = []
    previous_date = None
    current_date = None
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            if previous_date is None and len(table) >= 2:
                previous_date = parse_date_from_header(table[0][11] if len(table[0]) > 11 else None)
                current_date = parse_date_from_header(table[0][14] if len(table[0]) > 14 else None)
            for row in table:
                if not row:
                    continue
                first = (row[0] or '').strip() if len(row) > 0 else ''
                second = (row[1] or '').strip() if len(row) > 1 else ''
                if first == 'No' or second == 'Kode Efek':
                    continue
                fixed = list(row[:18])
                if len(fixed) < 18:
                    fixed += [None] * (18 - len(fixed))
                if second or first:
                    rows.append(fixed)
    raw_df = pd.DataFrame(rows, columns=RAW_ABOVE5_COLUMNS)
    raw_df['previous_date_raw'] = previous_date
    raw_df['report_date_raw'] = current_date
    raw_df['group_id'] = raw_df['no_raw'].astype(str).str.strip().ne('').cumsum()
    fill_cols = [
        'no_raw', 'share_code', 'issuer_name', 'securities_account_holder', 'shareholder_name',
        'securities_account_name', 'address_line_1', 'address_line_2', 'nationality', 'domicile',
        'local_foreign_status', 'investor_total_prev_date_raw', 'investor_pct_prev_date_raw',
        'investor_total_current_date_raw', 'investor_pct_current_date_raw'
    ]
    for col in fill_cols:
        raw_df[col] = raw_df.groupby('group_id')[col].transform(lambda s: s.replace('', pd.NA).ffill())
    return clean_text_columns(raw_df)


def clean_summary_dataframe(raw_df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    df = raw_df.copy()
    for col in [c for c in df.columns if c not in {'date', 'share_code'}]:
        if col.endswith('_raw') or col == 'number_of_shares':
            pass
    rename_map = {col: col.replace('_raw', '') for col in df.columns if col.endswith('_raw')}
    df = df.rename(columns=rename_map)
    numeric_cols = [c for c in df.columns if c not in {'date', 'share_code'}]
    for col in numeric_cols:
        df[col] = df[col].map(parse_number)
    df['report_date'] = df['date'].map(standardize_date_token)
    df['source_file'] = source_file
    df['report_type'] = 'investor_type_summary'
    hash_cols = ['report_date', 'share_code']
    df['row_hash'] = df.apply(lambda row: stable_row_hash(row, hash_cols), axis=1)
    return clean_text_columns(df)


def clean_above_1pct_dataframe(raw_df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    df = raw_df.copy().rename(columns={
        'holdings_scripless_raw': 'holdings_scripless',
        'holdings_scrip_raw': 'holdings_scrip',
        'total_holding_shares_raw': 'total_holding_shares',
        'percentage_raw': 'percentage',
    })
    for col in ['holdings_scripless', 'holdings_scrip', 'total_holding_shares']:
        df[col] = df[col].map(parse_number)
    df['percentage'] = df['percentage'].map(lambda x: parse_number(x, decimal_hint=True))
    df['report_date'] = df['date'].map(standardize_date_token)
    df['source_file'] = source_file
    df['report_type'] = 'shareholders_above_1pct'
    hash_cols = [
        'report_date', 'share_code', 'issuer_name', 'investor_name', 'investor_type',
        'local_foreign', 'nationality', 'domicile', 'total_holding_shares', 'percentage'
    ]
    df['row_hash'] = df.apply(lambda row: stable_row_hash(row, hash_cols), axis=1)
    return clean_text_columns(df)


def clean_above_5pct_dataframe(raw_df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    df = raw_df.copy().rename(columns={
        'no_raw': 'no',
        'shares_prev_date_raw': 'shares_prev_date',
        'investor_total_prev_date_raw': 'investor_total_prev_date',
        'investor_pct_prev_date_raw': 'investor_pct_prev_date',
        'shares_current_date_raw': 'shares_current_date',
        'investor_total_current_date_raw': 'investor_total_current_date',
        'investor_pct_current_date_raw': 'investor_pct_current_date',
        'change_in_shares_raw': 'change_in_shares',
        'previous_date_raw': 'previous_date',
        'report_date_raw': 'report_date',
    })
    for col in ['shares_prev_date', 'investor_total_prev_date', 'shares_current_date', 'investor_total_current_date', 'change_in_shares']:
        df[col] = df[col].map(parse_number)
    for col in ['investor_pct_prev_date', 'investor_pct_current_date']:
        df[col] = df[col].map(lambda x: parse_number(x, decimal_hint=True))
    df['previous_date'] = pd.to_datetime(df['previous_date'], errors='coerce')
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
    df['source_file'] = source_file
    df['report_type'] = 'shareholders_above_5pct'
    hash_cols = [
        'report_date', 'share_code', 'shareholder_name', 'securities_account_holder',
        'securities_account_name', 'shares_current_date', 'investor_total_current_date',
        'investor_pct_current_date'
    ]
    df['row_hash'] = df.apply(lambda row: stable_row_hash(row, hash_cols), axis=1)
    return clean_text_columns(df.drop(columns=['group_id'], errors='ignore'))


def extract_raw_dataframe(pdf_path: str | Path) -> tuple[str, pd.DataFrame]:
    pdf_path = Path(pdf_path)
    with pdfplumber.open(str(pdf_path)) as pdf:
        first_page_text = pdf.pages[0].extract_text() or ''
    report_type = detect_report_type(pdf_path.name, first_page_text)
    if report_type == 'summary':
        return report_type, extract_summary_raw(pdf_path)
    if report_type == 'above_1pct':
        return report_type, extract_above_1pct_raw(pdf_path)
    if report_type == 'above_5pct':
        return report_type, extract_above_5pct_raw(pdf_path)
    raise ValueError('Tipe PDF tidak didukung')


def clean_dataframe(report_type: str, raw_df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    if report_type == 'summary':
        return clean_summary_dataframe(raw_df, source_file)
    if report_type == 'above_1pct':
        return clean_above_1pct_dataframe(raw_df, source_file)
    if report_type == 'above_5pct':
        return clean_above_5pct_dataframe(raw_df, source_file)
    raise ValueError('Report type tidak dikenali')


def save_uploaded_pdf(uploaded_file) -> Path:
    target = RAW_PDF_DIR / uploaded_file.name
    with open(target, 'wb') as handle:
        handle.write(uploaded_file.getbuffer())
    return target


def write_csv(df: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


def get_output_paths(report_type: str, source_file: str) -> tuple[Path, Path]:
    stem = Path(source_file).stem
    raw_name = f"{stem}_{REPORT_MAP[report_type]['raw_prefix']}.csv"
    clean_name = f"{stem}_{REPORT_MAP[report_type]['clean_prefix']}.csv"
    return BRONZE_DIR / raw_name, SILVER_DIR / clean_name


def upsert_dataframe(engine, table_name: str, incoming_df: pd.DataFrame) -> pd.DataFrame:
    inspector = inspect(engine)
    data = incoming_df.copy()
    if inspector.has_table(table_name):
        existing_df = pd.read_sql_table(table_name, engine)
        combined = pd.concat([existing_df, data], ignore_index=True)
        if 'row_hash' in combined.columns:
            combined = combined.drop_duplicates(subset=['row_hash'], keep='last')
    else:
        combined = data
    combined.to_sql(table_name, engine, if_exists='replace', index=False)
    return combined


def ensure_audit_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS etl_ingestion_log (
        id INTEGER PRIMARY KEY,
        source_file TEXT,
        report_type TEXT,
        table_name TEXT,
        raw_rows INTEGER,
        clean_rows INTEGER,
        raw_csv TEXT,
        clean_csv TEXT,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def append_audit_log(engine, result: PipelineResult) -> None:
    ensure_audit_table(engine)
    payload = pd.DataFrame([
        {
            'source_file': result.source_file,
            'report_type': result.report_type,
            'table_name': result.table_name,
            'raw_rows': result.raw_rows,
            'clean_rows': result.clean_rows,
            'raw_csv': result.raw_csv,
            'clean_csv': result.clean_csv,
        }
    ])
    payload.to_sql('etl_ingestion_log', engine, if_exists='append', index=False)


def run_pipeline_from_pdf(pdf_path: str | Path, engine) -> PipelineResult:
    pdf_path = Path(pdf_path)
    report_type, raw_df = extract_raw_dataframe(pdf_path)
    clean_df = clean_dataframe(report_type, raw_df, pdf_path.name)
    raw_csv_path, clean_csv_path = get_output_paths(report_type, pdf_path.name)
    write_csv(raw_df, raw_csv_path)
    write_csv(clean_df, clean_csv_path)
    table_name = REPORT_MAP[report_type]['table_name']
    upsert_dataframe(engine, table_name, clean_df)
    result = PipelineResult(
        source_file=pdf_path.name,
        report_type=report_type,
        table_name=table_name,
        raw_rows=len(raw_df),
        clean_rows=len(clean_df),
        raw_csv=str(raw_csv_path),
        clean_csv=str(clean_csv_path),
        database_target=table_name,
    )
    append_audit_log(engine, result)
    return result


def run_pipeline_from_upload(uploaded_file, engine) -> PipelineResult:
    saved_path = save_uploaded_pdf(uploaded_file)
    return run_pipeline_from_pdf(saved_path, engine)


def list_quality_issues(df: pd.DataFrame, report_type: str) -> pd.DataFrame:
    issues: list[dict] = []
    if df.empty:
        return pd.DataFrame(columns=['severity', 'rule', 'affected_rows'])

    if 'report_date' in df.columns:
        issues.append({
            'severity': 'warning',
            'rule': 'report_date_null',
            'affected_rows': int(df['report_date'].isna().sum()),
        })
    if report_type == 'above_1pct':
        issues.append({
            'severity': 'warning',
            'rule': 'percentage_null_or_zero',
            'affected_rows': int(df['percentage'].fillna(0).eq(0).sum()),
        })
    if report_type == 'above_5pct':
        issues.append({
            'severity': 'warning',
            'rule': 'shareholder_name_blank',
            'affected_rows': int(df['shareholder_name'].fillna('').eq('').sum()),
        })
    if report_type == 'summary':
        issues.append({
            'severity': 'warning',
            'rule': 'share_code_blank',
            'affected_rows': int(df['share_code'].fillna('').eq('').sum()),
        })
    return pd.DataFrame(issues)


def bootstrap_sample_data(engine, pdf_paths: list[Path]) -> None:
    for pdf_path in pdf_paths:
        try:
            run_pipeline_from_pdf(pdf_path, engine)
        except Exception:
            continue
