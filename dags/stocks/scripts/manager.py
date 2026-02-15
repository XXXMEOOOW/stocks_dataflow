"""Управление загрузкой данных в ClickHouse."""

import logging
from typing import Any

import clickhouse_connect
import pandas as pd
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

CONN_ID = "dwh_clickhouse"
TABLE = "dwh.raw_stock_prices"


def get_ch_client() -> Any:
    """Возвращает клиент clickhouse-connect."""
    conn = BaseHook.get_connection(CONN_ID)
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login or "default",
        password=conn.password or "",
        database=conn.schema or "dwh",
    )


def delete_range(client: Any, ticker: str, start: str, end: str) -> None:
    """Удаляет строки в диапазоне дат для идемпотентности."""
    delete_sql = """
    ALTER TABLE dwh.raw_stock_prices
    DELETE WHERE ticker = %(ticker)s
      AND trade_date >= toDate(%(start)s)
      AND trade_date <  toDate(%(end)s)
    """
    client.command(delete_sql, parameters={"ticker": ticker, "start": start, "end": end})
    logger.info("Deleted existing rows for %s in %s..%s", ticker, start, end)


def load_to_clickhouse(
    df: pd.DataFrame,
    ticker: str,
    start: str,
    end: str,
    insert_cols: list[str],
) -> None:
    """
    Удаляет старые данные и вставляет новые в dwh.raw_stock_prices.

    Args:
        df: DataFrame с данными
        ticker: Тикер
        start: Начало периода
        end: Конец периода
        insert_cols: Список колонок для вставки
    """
    client = get_ch_client()
    delete_range(client, ticker, start, end)

    records = df[insert_cols].to_records(index=False).tolist()
    client.insert(TABLE, records, column_names=insert_cols)
    logger.info("Inserted %d rows into %s", len(records), TABLE)
