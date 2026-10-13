-- Runs once when the warehouse container is first created.
CREATE SCHEMA IF NOT EXISTS lake_gold;   -- loaded by Spark gold_batch.py (dbt sources)
CREATE SCHEMA IF NOT EXISTS analytics;   -- built by dbt (star schema + KPI marts)
CREATE SCHEMA IF NOT EXISTS realtime;    -- written by the Spark speed layer
CREATE SCHEMA IF NOT EXISTS monitoring;  -- alerts + pipeline metrics

CREATE TABLE IF NOT EXISTS realtime.orders_per_minute (
    window_start timestamptz PRIMARY KEY,
    orders       integer        NOT NULL,
    revenue      numeric(14, 2) NOT NULL,
    updated_at   timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS realtime.payments_per_minute (
    window_start timestamptz PRIMARY KEY,
    payments     integer          NOT NULL,
    failed       integer          NOT NULL,
    failure_rate double precision NOT NULL,
    updated_at   timestamptz      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monitoring.alerts (
    id           bigserial PRIMARY KEY,
    created_at   timestamptz NOT NULL DEFAULT now(),
    source       text        NOT NULL,
    severity     text        NOT NULL,
    window_start timestamptz NOT NULL DEFAULT now(),
    message      text        NOT NULL,
    UNIQUE (source, window_start)        -- one alert per source per minute
);
