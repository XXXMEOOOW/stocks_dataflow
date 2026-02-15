"""DAG для загрузки котировок акций в raw-слой DWH."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from stocks.config import get_interval, get_tickers
from stocks.scripts.manager import download_and_load

logger = logging.getLogger(__name__)


def task_download_and_load_raw(ticker: str, interval: str, **context) -> None:
    """Задача: вызывает manager с датами и тикером из контекста Airflow."""
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    start = (data_interval_start - timedelta(days=1)).strftime("%Y-%m-%d")
    end = data_interval_end.strftime("%Y-%m-%d")

    download_and_load(ticker, start, end, data_interval_start, data_interval_end)


tickers = get_tickers()
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

    PythonOperator(
        task_id="download_and_load_raw",
        python_callable=task_download_and_load_raw,
        op_kwargs={"ticker": ticker, "interval": interval},
        dag=dag,
    )

    globals()[dag.dag_id] = dag
