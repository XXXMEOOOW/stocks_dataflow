"""DAG для загрузки котировок акций в raw- и ODS-слои DWH."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from stocks.config import get_interval, get_ods_tickers, get_tickers
from stocks.scripts.manager import download_and_load, load_raw_to_ods

logger = logging.getLogger(__name__)


def task_download_and_load_raw(ticker: str, interval: str, **context) -> None:
    """Задача: вызывает manager с датами и тикером из контекста Airflow."""
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    start = (data_interval_start - timedelta(days=1)).strftime("%Y-%m-%d")
    end = data_interval_end.strftime("%Y-%m-%d")

    download_and_load(ticker, start, end, data_interval_start, data_interval_end)


def task_load_to_ods(ticker: str, **context) -> None:
    """Задача: копирует данные из raw в ods_stock_daily."""
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    start = (data_interval_start - timedelta(days=1)).strftime("%Y-%m-%d")
    end = data_interval_end.strftime("%Y-%m-%d")

    load_raw_to_ods(ticker, start, end)


tickers = get_tickers()
ods_tickers = get_ods_tickers()
interval = get_interval()

for ticker in tickers:
    safe_ticker = ticker.replace(".", "_").lower()

    dag = DAG(
        dag_id=f"stock_raw_{safe_ticker}",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=True,
        max_active_runs=1,
        tags=["stocks", "raw", ticker],
    )

    t1 = PythonOperator(
        task_id="download_and_load_raw",
        python_callable=task_download_and_load_raw,
        op_kwargs={"ticker": ticker, "interval": interval},
        dag=dag,
    )

    if ticker in ods_tickers:
        t2 = PythonOperator(
            task_id="load_to_ods",
            python_callable=task_load_to_ods,
            op_kwargs={"ticker": ticker},
            dag=dag,
        )
        t1 >> t2

    globals()[dag.dag_id] = dag
