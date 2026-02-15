"""Оркестрация: вызов API, подготовка данных, загрузка в ClickHouse."""

import logging

from stocks.scripts.api import yf_download
from stocks.scripts.utils import _get_ch_client, _prepare_df, _compute_window
import stocks.config as cfg

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


def download_and_load(ticker: str, **context) -> None:
    """
    RAW: скачиваем котировки и грузим в dwh.raw_stock_prices.
    Все вычисления интервалов — в utils._compute_window()
    """
    start, end, di_start, di_end = _compute_window(context, lookback_days=1)
    logger.info("Download %s [%s..%s]", ticker, start, end)

    raw = yf_download(ticker, start, end)
    df = _prepare_df(raw, ticker, di_start, di_end, insert_cols=INSERT_COLS)

    if df is None or df.empty:
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
    logger.info("Deleted existing RAW rows for %s in %s..%s", ticker, start, end)

    records = df[INSERT_COLS].to_records(index=False).tolist()
    client.insert(cfg.TABLE_RAW, records, column_names=INSERT_COLS)
    logger.info("Inserted %d rows into %s", len(records), cfg.TABLE_RAW)


def load_raw_to_ods(ticker: str, **context) -> None:
    """
    ODS: копируем из raw в ods (фильтром по тикеру и окну).
    Если тикер не в ODS_TICKERS — просто пропускаем.
    """
    if ticker not in cfg.ODS_TICKERS:
        logger.info("Ticker %s is not in ODS_TICKERS, skipping ODS load.", ticker)
        return

    start, end, _di_start, _di_end = _compute_window(context, lookback_days=1)

    client = _get_ch_client()

    delete_sql = """
    ALTER TABLE dwh.ods_stock_daily
    DELETE WHERE ticker = %(ticker)s
      AND trade_date >= toDate(%(start)s)
      AND trade_date <  toDate(%(end)s)
    """
    client.command(delete_sql, parameters={"ticker": ticker, "start": start, "end": end})
    logger.info("Deleted existing ODS rows for %s in %s..%s", ticker, start, end)

    insert_sql = """
    INSERT INTO dwh.ods_stock_daily
    SELECT
        ticker, trade_date, open, high, low, close, source,
        data_interval_start, data_interval_end, ingestion_ts
    FROM dwh.raw_stock_prices
    WHERE ticker = %(ticker)s
      AND trade_date >= toDate(%(start)s)
      AND trade_date <  toDate(%(end)s)
    """
    client.command(insert_sql, parameters={"ticker": ticker, "start": start, "end": end})
    logger.info("Loaded %s from raw to ods_stock_daily [%s..%s]", ticker, start, end)
