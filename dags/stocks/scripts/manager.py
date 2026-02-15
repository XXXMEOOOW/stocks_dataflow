"""Оркестрация: вызов API, подготовка данных, загрузка в ClickHouse."""

import logging
from datetime import datetime
from typing import Any

import clickhouse_connect
import pandas as pd
from airflow.hooks.base import BaseHook

from stocks.scripts.api import yf_download

logger = logging.getLogger(__name__)

CONN_ID = "dwh_clickhouse"
TABLE = "dwh.raw_stock_prices"

INSERT_COLS = [
    "ticker",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "source",
    "data_interval_start",
    "data_interval_end",
]


def _get_ch_client() -> Any:
    conn = BaseHook.get_connection(CONN_ID)
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login or "default",
        password=conn.password or "",
        database=conn.schema or "dwh",
    )


def _prepare_df(
    raw: pd.DataFrame,
    ticker: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> pd.DataFrame:
    """Преобразует сырые данные yfinance в формат для загрузки."""
    if raw.empty:
        return raw

    df = raw.reset_index()

    if "Date" in df.columns:
        df.rename(columns={"Date": "trade_date"}, inplace=True)
    elif "Datetime" in df.columns:
        df.rename(columns={"Datetime": "trade_date"}, inplace=True)

    df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
        },
        inplace=True,
    )

    df = df[["trade_date", "open", "high", "low", "close"]].copy()
    df["ticker"] = ticker
    df["source"] = "yfinance"
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["data_interval_start"] = data_interval_start.date()
    df["data_interval_end"] = data_interval_end.date()

    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)

    return df[INSERT_COLS]


def download_and_load(
    ticker: str,
    start: str,
    end: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> None:
    """
    Скачивает данные через API и загружает в ClickHouse.

    Args:
        ticker: Тикер
        start: Начало периода (YYYY-MM-DD)
        end: Конец периода (YYYY-MM-DD)
        data_interval_start: Начало data interval Airflow
        data_interval_end: Конец data interval Airflow
    """
    logger.info("Download %s [%s..%s]", ticker, start, end)

    raw = yf_download(ticker, start, end)
    df = _prepare_df(raw, ticker, data_interval_start, data_interval_end)

    if df.empty:
        logger.warning("No data for %s", ticker)
        return

    client = _get_ch_client()

    delete_sql = """
    ALTER TABLE dwh.raw_stock_prices
    DELETE WHERE ticker = %(ticker)s
      AND trade_date >= toDate(%(start)s)
      AND trade_date <  toDate(%(end)s)
    """
    client.command(delete_sql, parameters={"ticker": ticker, "start": start, "end": end})
    logger.info("Deleted existing rows for %s in %s..%s", ticker, start, end)

    records = df[INSERT_COLS].to_records(index=False).tolist()
    client.insert(TABLE, records, column_names=INSERT_COLS)
    logger.info("Inserted %d rows into %s", len(records), TABLE)
