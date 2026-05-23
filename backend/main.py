"""
FastAPI Backend for Financial Data Dashboard

Serves:
  - GET  /              : Dashboard HTML page
  - GET  /api/stocks    : List all stocks with latest price
  - GET  /api/stocks/{code}/history : OHLCV history with SMA
"""

import os
from datetime import date, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse

# --- Configuration ---
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://finance:finance@postgres-app:5432/finance_db",
)

# --- App ---
app = FastAPI(
    title="Financial Data Dashboard API",
    description="Korean stock market data API with OHLCV history and SMA indicators",
    version="1.0.0",
)


def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(DB_URL)


# --- Database Init ---
INIT_SQL = """
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


@app.on_event("startup")
def startup():
    """Ensure database tables exist on startup."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(INIT_SQL)
        conn.commit()
        conn.close()
        print("Database tables ensured.")
    except Exception as e:
        print(f"Warning: Could not connect to database on startup: {e}")


# --- API Endpoints ---
@app.get("/")
async def root():
    """Serve the dashboard HTML page."""
    return FileResponse("static/dashboard.html")


@app.get("/api/stocks")
async def get_stocks():
    """List all tracked stocks with latest price and change information."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH latest_prices AS (
                    SELECT DISTINCT ON (stock_code)
                        stock_code,
                        date,
                        close,
                        volume
                    FROM stock_prices
                    ORDER BY stock_code, date DESC
                ),
                prev_prices AS (
                    SELECT DISTINCT ON (sp.stock_code)
                        sp.stock_code,
                        sp.close AS prev_close
                    FROM stock_prices sp
                    WHERE sp.date < (
                        SELECT lp.date FROM latest_prices lp
                        WHERE lp.stock_code = sp.stock_code
                    )
                    ORDER BY sp.stock_code, sp.date DESC
                )
                SELECT
                    s.code,
                    s.name,
                    s.type,
                    lp.date AS latest_date,
                    lp.close AS latest_close,
                    lp.volume AS latest_volume,
                    pp.prev_close,
                    CASE
                        WHEN pp.prev_close IS NOT NULL AND pp.prev_close != 0
                        THEN ROUND((lp.close - pp.prev_close)::numeric, 2)
                        ELSE NULL
                    END AS change_amount,
                    CASE
                        WHEN pp.prev_close IS NOT NULL AND pp.prev_close != 0
                        THEN ROUND(((lp.close - pp.prev_close) / pp.prev_close * 100)::numeric, 2)
                        ELSE NULL
                    END AS change_percent
                FROM stocks s
                LEFT JOIN latest_prices lp ON s.code = lp.stock_code
                LEFT JOIN prev_prices pp ON s.code = pp.stock_code
                ORDER BY s.type, s.name
            """)
            rows = cur.fetchall()

        result = []
        for row in rows:
            item = dict(row)
            item["latest_date"] = item["latest_date"].isoformat() if item["latest_date"] else None
            item["latest_close"] = float(item["latest_close"]) if item["latest_close"] else None
            item["latest_volume"] = int(item["latest_volume"]) if item["latest_volume"] else None
            item["prev_close"] = float(item["prev_close"]) if item["prev_close"] else None
            item["change_amount"] = float(item["change_amount"]) if item["change_amount"] else None
            item["change_percent"] = float(item["change_percent"]) if item["change_percent"] else None
            result.append(item)

        return {"stocks": result, "count": len(result)}

    finally:
        conn.close()


@app.get("/api/stocks/{code}/history")
async def get_stock_history(
    code: str,
    days: Optional[int] = Query(default=180, ge=30, le=730, description="Number of days of history"),
):
    """Get OHLCV history with Simple Moving Averages (SMA-5/20/60) for a stock."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT code, name, type FROM stocks WHERE code = %s", (code,))
            stock = cur.fetchone()

        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock not found: {code}")

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH price_data AS (
                    SELECT
                        date,
                        open,
                        high,
                        low,
                        close,
                        volume
                    FROM stock_prices
                    WHERE stock_code = %s
                        AND date >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER BY date ASC
                )
                SELECT
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ROUND(AVG(close) OVER (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)::numeric, 2) AS sma_5,
                    ROUND(AVG(close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)::numeric, 2) AS sma_20,
                    ROUND(AVG(close) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)::numeric, 2) AS sma_60
                FROM price_data
                ORDER BY date ASC
            """, (code, days))
            rows = cur.fetchall()

        ohlcv = []
        for row in rows:
            item = dict(row)
            item["date"] = item["date"].isoformat()
            item["open"] = float(item["open"]) if item["open"] else None
            item["high"] = float(item["high"]) if item["high"] else None
            item["low"] = float(item["low"]) if item["low"] else None
            item["close"] = float(item["close"]) if item["close"] else None
            item["volume"] = int(item["volume"]) if item["volume"] else None
            item["sma_5"] = float(item["sma_5"]) if item["sma_5"] else None
            item["sma_20"] = float(item["sma_20"]) if item["sma_20"] else None
            item["sma_60"] = float(item["sma_60"]) if item["sma_60"] else None
            ohlcv.append(item)

        return {
            "stock": dict(stock),
            "count": len(ohlcv),
            "data": ohlcv,
        }

    finally:
        conn.close()
