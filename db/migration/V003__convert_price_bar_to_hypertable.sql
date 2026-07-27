-- TimescaleDB requires every unique index to contain all partition columns.
-- Keep the surrogate id for compatibility, but include asset_id and bar_time
-- in the primary key used by the hypertable.
ALTER TABLE price_bar
    DROP CONSTRAINT price_bar_pkey;

ALTER TABLE price_bar
    ADD CONSTRAINT price_bar_pkey
        PRIMARY KEY (id, asset_id, bar_time);

-- Daily price history performed best in the previous workload with a
-- 90-day range chunk and four symbol hash partitions. asset_id is the stable
-- replacement for the legacy symb partition key.
SELECT create_hypertable(
    'price_bar',
    'bar_time',
    partitioning_column => 'asset_id',
    number_partitions => 4,
    chunk_time_interval => INTERVAL '90 days',
    create_default_indexes => FALSE,
    migrate_data => TRUE,
    if_not_exists => TRUE
);

-- The V001 business key already includes both hypertable dimensions and
-- remains the idempotency boundary for provider price bars.
DROP INDEX IF EXISTS idx_price_bar_asset_time;

CREATE INDEX idx_price_bar_asset_interval_time
    ON price_bar (asset_id, "interval", bar_time DESC);
