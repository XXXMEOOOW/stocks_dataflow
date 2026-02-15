"""API для загрузки данных о котировках акций."""

import logging
from datetime import datetime

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

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


def download_stock_data(
    ticker: str,
    start: str,
    end: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> pd.DataFrame | None:
    """
    Скачивает OHLCV-данные по тикеру через yfinance.

    Args:
        ticker: Тикер (AAPL, MSFT, MOEX.SBER и т.д.)
        start: Начало периода (YYYY-MM-DD)
        end: Конец периода (YYYY-MM-DD, exclusive)
        data_interval_start: Начало data interval Airflow
        data_interval_end: Конец data interval Airflow

    Returns:
        DataFrame с колонками insert_cols или None, если данных нет
    """
    logger.info("Download %s [%s..%s]", ticker, start, end)

    df = yf.download(ticker, start=start, end=end)

    if df.empty:
        logger.warning("No data from yfinance for %s", ticker)
        return None

    df = df.reset_index()

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
            "Adj Close": "adj_close",
        },
        inplace=True,
    )

    keep_cols = ["trade_date", "open", "high", "low", "close"]
    df = df[keep_cols].copy()

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
