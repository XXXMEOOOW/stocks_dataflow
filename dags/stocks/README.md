# Stocks DAG

DAG для загрузки котировок акций в raw-слой DWH (ClickHouse).

## Структура

```
stocks/
├── config.py       # Конфигурация (тикеры, интервал)
├── dag.py          # Определения Airflow DAG
├── README.md
└── scripts/
    ├── api.py              # Загрузка данных через yfinance
    ├── init_clickhouse.sql # Схема таблицы (выполняется при старте)
    └── manager.py          # Работа с ClickHouse
```

## Конфигурация

Тикеры и интервал задаются в `config.py`:

## Требования

- Airflow Connection `dwh_clickhouse` (создаётся в airflow-init)
- Таблица `dwh.raw_stock_prices` (создаётся автоматически при старте из `scripts/init_clickhouse.sql`)
- Пакеты: yfinance, clickhouse-connect, pyyaml

## Запуск

DAG создаётся динамически для каждого тикера (например, `stock_raw_aapl`, `stock_raw_moex_sber`).
Расписание: ежедневно (`@daily`).
