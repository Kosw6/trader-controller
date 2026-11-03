-- 일봉의 날짜 라벨과 같은 UTC 달력. 주봉은 월요일 시작이다.
-- 월/연봉은 주 경계가 달력 경계를 넘으므로 이 뷰를 재집계하지 않는다.
CREATE MATERIALIZED VIEW price_bar_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT asset_id, source, currency,
       time_bucket(INTERVAL '1 week', bar_time) AS bar_time,
       first(open, bar_time) AS open, max(high) AS high, min(low) AS low,
       last(close, bar_time) AS close,
       CASE WHEN count(adjusted_close) = count(*) THEN last(adjusted_close, bar_time) END AS adjusted_close,
       CASE WHEN count(volume) = count(*) THEN sum(volume) END AS volume
FROM price_bar
WHERE "interval" = '1D' AND display_policy = 'FULL_OHLC'
GROUP BY asset_id, source, currency, time_bucket(INTERVAL '1 week', bar_time)
WITH NO DATA;

CREATE INDEX idx_price_bar_weekly_lookup
ON price_bar_weekly (asset_id, source, bar_time DESC);

-- 완전히 포함된 최근 주만 갱신한다. 진행 중인 주는 real-time aggregate로 보완.
-- 이 범위보다 오래된 backfill은 별도의 해당 범위 refresh가 필요하다.
SELECT add_continuous_aggregate_policy('price_bar_weekly',
    start_offset => INTERVAL '3 weeks', end_offset => INTERVAL '0 days',
    schedule_interval => INTERVAL '1 hour');
