"""
Airflow DAG: Financial Data Collector

Scheduled daily to fetch Korean stock market data via FinanceDataReader
and store it in PostgreSQL.
Schedule: 7:00 AM UTC daily (4:00 PM KST, after market close)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import FinanceDataReader as fdr
import psycopg2
import pandas as pd
import logging

log = logging.getLogger(__name__)

# --- Configuration ---
APP_DB_URL = "postgresql://finance:finance@postgres-app:5432/finance_db"

KOREAN_STOCKS = {
    "005930": "Samsung Electronics",
    "000660": "SK Hynix",
    "005380": "Hyundai Motor",
    "035720": "Kakao",
    "035420": "NAVER",
    "051910": "LG Chem",
    "207940": "Samsung Biologics",
    "005490": "POSCO Holdings",
    "012330": "Hyundai Mobis",
    "068270": "Celltrion",
}

KOREAN_INDICES = {
    "KS11": "KOSPI",
    "KQ11": "KOSDAQ",
}

# --- Database Setup ---
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS stocks (
    code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(20) NOT NULL DEFAULT 'stock',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL REFERENCES stocks(code) ON DELETE CASCADE,
    date DATE NOT NULL,
    open NUMERIC(12, 2),
    high NUMERIC(12, 2),
    low NUMERIC(12, 2),
    close NUMERIC(12, 2),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(stock_code, date)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_code_date
    ON stock_prices(stock_code, date DESC);
"""


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLES_SQL)
    conn.commit()
    log.info("Database tables ensured.")


def upsert_stocks(conn, tickers: dict, ticker_type: str = "stock"):
    with conn.cursor() as cur:
        for code, name in tickers.items():
            cur.execute(
                """
                INSERT INTO stocks (code, name, type)
                VALUES (%s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET
                    name = EXCLUDED.name,
                    type = EXCLUDED.type
                """,
                (code, name, ticker_type),
            )
    conn.commit()
    log.info(f"Upserted {len(tickers)} {ticker_type}(s).")


def fetch_and_store_prices(conn, tickers: dict):
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y-%m-%d")

    for code, name in tickers.items():
        try:
            log.info(f"Fetching data for {name} ({code}) via KRX...")
            df = fdr.DataReader(code, start_date, end_date)

            if df.empty:
                log.warning(f"No data returned for {name} ({code}). Skipping.")
                continue

            df = df.reset_index()
            df["Date"] = pd.to_datetime(df["Date"]).dt.date

            rows_inserted = 0
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute(
                        """
                        INSERT INTO stock_prices (stock_code, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (stock_code, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                        """,
                        (
                            code,
                            row["Date"],
                            round(float(row["Open"]), 2) if pd.notna(row["Open"]) else None,
                            round(float(row["High"]), 2) if pd.notna(row["High"]) else None,
                            round(float(row["Low"]), 2) if pd.notna(row["Low"]) else None,
                            round(float(row["Close"]), 2) if pd.notna(row["Close"]) else None,
                            int(row["Volume"]) if pd.notna(row["Volume"]) else None,
                        ),
                    )
                    rows_inserted += 1

            conn.commit()
            log.info(f"Stored {rows_inserted} rows for {name} ({code}).")

        except Exception as e:
            log.error(f"Error fetching {name} ({code}): {e}")
            conn.rollback()


def collect_financial_data(**context):
    log.info("Starting financial data collection (KRX / FinanceDataReader)...")

    conn = psycopg2.connect(APP_DB_URL)

    try:
        ensure_tables(conn)
        upsert_stocks(conn, KOREAN_STOCKS, "stock")
        upsert_stocks(conn, KOREAN_INDICES, "index")
        fetch_and_store_prices(conn, KOREAN_STOCKS)
        fetch_and_store_prices(conn, KOREAN_INDICES)
        log.info("Financial data collection completed successfully.")
    except Exception as e:
        log.error(f"Data collection failed: {e}")
        raise
    finally:
        conn.close()


# --- DAG Definition ---
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="korean_finance_collector",
    default_args=default_args,
    description="Daily collection of Korean stock market data from KRX via FinanceDataReader",
    schedule="0 7 * * 1-5",  # Weekdays, after KSE market close
    catchup=False,
    max_active_runs=1,
    tags=["finance", "stocks", "korea", "krx"],
) as dag:

    collect_task = PythonOperator(
        task_id="collect_financial_data",
        python_callable=collect_financial_data,
        provide_context=True,
    )

    collect_task
