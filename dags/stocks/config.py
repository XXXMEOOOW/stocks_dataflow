"""Конфигурация DAG stocks."""

TABLE_RAW = "dwh.raw_stock_prices"
TABLE_ODS = "dwh.ods_stock_daily"

TICKERS = [
    "AAPL",
    "TSLA",
    "NVDA",
    "MSFT",
    "MOEX.SBER",
]

ODS_TICKERS = ["AAPL"]  # Тикеры, данные которых загружаются в ODS

