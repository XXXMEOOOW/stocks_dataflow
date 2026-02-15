"""Конфигурация DAG stocks."""

TICKERS = [
    "AAPL",
    "TSLA",
    "NVDA",
    "MSFT",
    "MOEX.SBER",
]

ODS_TICKERS = ["AAPL"]  # Тикеры, данные которых загружаются в ODS

INTERVAL = "1d"


def get_tickers() -> list[str]:
    """Возвращает список тикеров."""
    return TICKERS


def get_interval() -> str:
    """Возвращает интервал (например, 1d)."""
    return INTERVAL


def get_ods_tickers() -> list[str]:
    """Возвращает тикеры для загрузки в ODS-слой."""
    return ODS_TICKERS
