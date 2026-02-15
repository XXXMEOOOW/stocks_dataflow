"""API для работы с внешними источниками данных."""

import yfinance as yf


def yf_download(ticker: str, start: str, end: str):
    """
    Скачивает OHLCV-данные через yfinance.

    Args:
        ticker: Тикер (AAPL, MSFT, MOEX.SBER и т.д.)
        start: Начало периода (YYYY-MM-DD)
        end: Конец периода (YYYY-MM-DD, exclusive)

    Returns:
        DataFrame с данными yfinance (пустой, если данных нет)
    """
    data = yf.download(ticker, start=start, end=end)
    return data
