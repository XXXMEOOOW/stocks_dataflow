CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.raw_stock_prices
(
    ticker              String,
    trade_date          Date,
    open                Float64,
    high                Float64,
    low                 Float64,
    close               Float64,
    source              String,
    data_interval_start Date,
    data_interval_end   Date,
    ingestion_ts        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingestion_ts)
ORDER BY (ticker, trade_date);
