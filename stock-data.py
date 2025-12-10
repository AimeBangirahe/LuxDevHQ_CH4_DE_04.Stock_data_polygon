from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text
from polygon import RESTClient

TICKER = "AAPL"
POLYGON_API_KEY = ""
NEON_CONN_STRING = (
)

client = RESTClient(api_key=POLYGON_API_KEY)

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger("airflow.task")
with DAG(
    dag_id="stock_etl_neon_polygon_client",
    default_args=default_args,
    description="ETL pipeline using Polygon RESTClient and Neon PostgreSQL",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["neon", "polygon", "etl"],
) as dag:

    @task
    def extract(ticker: str) -> List[Dict[str, Any]]:
        try:
            aggs = []
            for a in client.list_aggs(
                    ticker=ticker,
                    multiplier=1,
                    timespan="day",
                    from_="2025-10-01",
                    to="2025-10-02",
                    limit=2,
            ):
                aggs.append({
                    "ticker": ticker,
                    "open": a.open,
                    "high": a.high,
                    "low": a.low,
                    "close": a.close,
                    "volume": a.volume,
                    "timestamp": a.timestamp,
                })

            if not aggs:
                logger.info(f"No data returned for {ticker}. Possibly market closed.")
                return []

            logger.info(f"Extracted {len(aggs)} records for {ticker}.")
            print(aggs, file=open(f"{ticker}.json", 'a'))
            return aggs
        except Exception as e:
            logger.error(f"Extract failed: {e}")
            raise

    @task
    def transform(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not raw_data:
            logger.warning("No raw data to transform.")
            return []

        try:
            df = pd.DataFrame(raw_data)
            df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
            df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]
            df["volume"] = df["volume"].fillna(0).astype("int64")

            records = df.to_dict(orient="records")
            logger.info(f"Transformed {len(records)} records.")
            return records
        except Exception as e:
            logger.error(f"Transform failed: {e}")
            raise

    @task
    def load(transformed_data: List[Dict[str, Any]]) -> None:
        if not transformed_data:
            logger.warning("No data to load.")
            return

        try:
            engine = create_engine(NEON_CONN_STRING, echo=False)

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_prices (
                ticker VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open NUMERIC(12,4),
                high NUMERIC(12,4),
                low NUMERIC(12,4),
                close NUMERIC(12,4),
                volume BIGINT,
                PRIMARY KEY (ticker, date)
            );
            """

            insert_sql = """
            INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
            VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
            ON CONFLICT (ticker, date) DO NOTHING;
            """

            with engine.begin() as conn:
                conn.execute(text(create_table_sql))
                conn.execute(text(insert_sql), transformed_data)

            logger.info(f"Loaded {len(transformed_data)} records into Neon.")
        except Exception as e:
            logger.error(f"Load failed: {e}")
            raise

    raw = extract(ticker=TICKER)
    clean = transform(raw)
    load(clean)