"""Конфигурация DAG stocks."""

TICKERS = [
    "AAPL",
    "TSLA",
    "NVDA",
    "MSFT",
    "MOEX.SBER",
]

INTERVAL = "1d"


def get_tickers() -> list[str]:
    """Возвращает список тикеров."""
    return TICKERS


def get_interval() -> str:
    """Возвращает интервал (например, 1d)."""
    return INTERVAL
