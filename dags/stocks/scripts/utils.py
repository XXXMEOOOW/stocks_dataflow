import pandas as pd
from datetime import timedelta
from typing import Any, Tuple

import clickhouse_connect
from airflow.hooks.base import BaseHook


def _get_ch_client() -> Any:
    conn = BaseHook.get_connection("dwh_clickhouse")
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login or "default",
        password=conn.password or "",
        database=conn.schema or "dwh",
    )


def _compute_window(context: dict, lookback_days: int = 1) -> Tuple[str, str, Any, Any]:
    """
    Возвращает:
      start_str, end_str (YYYY-MM-DD)
      data_interval_start, data_interval_end (как datetime из Airflow)

    Правило:
      start = data_interval_start - lookback_days
      end   = data_interval_end
    """
    di_start = context["data_interval_start"]
    di_end = context["data_interval_end"]

    start_dt = di_start - timedelta(days=lookback_days)
    end_dt = di_end

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")
    return start_str, end_str, di_start, di_end


def _prepare_df(
    raw: pd.DataFrame,
    ticker: str,
    data_interval_start,   # datetime from Airflow
    data_interval_end,     # datetime from Airflow
    insert_cols: list[str],
) -> pd.DataFrame:
    """Преобразует сырые данные yfinance в формат для загрузки в ClickHouse (raw layer)."""
    if raw is None or raw.empty:
        return raw

    df = raw.reset_index()

    # yfinance может вернуть индекс Date или Datetime
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

    # Приводим к float
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)

    return df[insert_cols]
