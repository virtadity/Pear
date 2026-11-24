CREATE DATABASE IF NOT EXISTS currency_dwh;


CREATE TABLE IF NOT EXISTS currency_dwh.raw_rates (
    currency_identifier String,
    symbol String,
    nominal Integer,
    currency_name String,
    currency_value Float64,
    currency_unit_rate Float64,
    load_date Date,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(load_date)
ORDER BY (currency_identifier, load_date);


CREATE TABLE IF NOT EXISTS currency_dwh.currency_metrics (
    currency_identifier String,
    symbol String,
    nominal Integer,
    currency_name String,
    currency_value Float64,
    previous_currency_value Float64,
    daily_change Float64,
    moving_avg_7d Float64,
    load_date Date
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(load_date)
ORDER BY (currency_identifier, load_date);