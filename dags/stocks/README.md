# Stocks DAG

DAG Airflow для загрузки котировок акций в raw-слой DWH (ClickHouse).

## Архитектура решения

### Разделение ответственности

| Модуль | Назначение |
|--------|------------|
| **api.py** | Функции работы с внешними API. Только загрузка сырых данных, без бизнес-логики. |
| **manager.py** | Оркестрация: вызывает API, подготавливает данные, загружает в ClickHouse. Прокидывает даты и тикер. |
| **dag.py** | Определение Airflow DAG и задач. Получает `data_interval_start/end` из контекста и передаёт в manager. |
| **config.py** | Список тикеров и интервал. |

### Поток данных

```
dag.py (контекст Airflow)
    ↓ ticker, start, end, data_interval_start, data_interval_end
manager.py
    ↓ yf_download(ticker, start, end)
api.py  →  сырой DataFrame
    ↓ _prepare_df()
manager.py  →  подготовленный DataFrame
    ↓ load to ClickHouse
dwh.raw_stock_prices
```

### api.py

Содержит только функции вызова внешних API:

```python
def yf_download(ticker, start, end):
    data = yf.download(ticker, start=start, end=end)
    return data
```

Минимальная логика — возвращает сырые данные yfinance.

### manager.py

- Импортирует `api.yf_download`
- Принимает тикер и даты
- Вызывает API
- Нормализует колонки, добавляет `ticker`, `source`, `data_interval_start`, `data_interval_end`
- Удаляет старые строки в диапазоне (идемпотентность)
- Вставляет данные в `dwh.raw_stock_prices`

### dag.py

- Создаёт DAG для каждого тикера из `config.py`
- Задача вызывает `manager.download_and_load` с датами из Airflow context

## Структура проекта

```
stocks/
├── config.py              # Тикеры, интервал
├── dag.py                 # Airflow DAG
├── README.md
└── scripts/
    ├── api.py             # Функции работы с API (yfinance)
    ├── init_clickhouse.sql # Схема таблицы (выполняется при старте Docker)
    └── manager.py         # Оркестрация и загрузка в ClickHouse
```

## Конфигурация

Тикеры задаются в `config.py`:

```python
TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "MOEX.SBER"]
INTERVAL = "1d"
```

## Таблицы ClickHouse

- **raw**: `dwh.raw_stock_prices` — все тикеры
- **ods**: `dwh.ods_stock_daily` — фильтрованные тикеры (по умолчанию AAPL)

Обе: ReplacingMergeTree(ingestion_ts). Колонки: ticker, trade_date, open, high, low, close, source, data_interval_start, data_interval_end, ingestion_ts.

Тикеры для ODS задаются в `config.ODS_TICKERS`.

## Запуск

1. `docker compose up -d`
2. Таблица создаётся автоматически из `scripts/init_clickhouse.sql`
3. Airflow Connection `dwh_clickhouse` создаётся в airflow-init
4. DAG: `stock_raw_<ticker>` (например, `stock_raw_aapl`), расписание `@daily`

## Если таблица не создалась при старте

Перезапустить только init и посмотреть логи:

```bash
docker compose up -d clickhouse-init
docker compose logs clickhouse-init
```

Создать таблицу вручную: выполнить SQL из `scripts/init_clickhouse.sql` в DBeaver или через:

```bash
Get-Content dags\stocks\scripts\init_clickhouse.sql | docker exec -i dwh_clickhouse clickhouse-client --user dwh_user --password dwh_pass --multiquery
```

## Зависимости

- yfinance
- clickhouse-connect
- pandas
