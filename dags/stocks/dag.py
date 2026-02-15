"""DAG для загрузки котировок акций в raw- и ODS-слои DWH."""

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from stocks.config import TICKERS
from stocks.scripts.manager import download_and_load, load_raw_to_ods

logger = logging.getLogger(__name__)

for ticker in TICKERS:
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
        python_callable=download_and_load,
        op_kwargs={"ticker": ticker},
        dag=dag,
    )

    t2 = PythonOperator(
        task_id="load_to_ods",
        python_callable=load_raw_to_ods,
        op_kwargs={"ticker": ticker},
        dag=dag,
    )

    t1 >> t2
    globals()[dag.dag_id] = dag
