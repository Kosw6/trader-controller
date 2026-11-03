\set ON_ERROR_STOP on
-- 실제 원본과 CAGG를 공급자·통화까지 동일 조건으로 대조한다. 읽기 전용.
WITH selected AS (
 SELECT DISTINCT asset_id FROM asset_provider_symbol
 WHERE provider='KIS' AND provider_symbol='NVDA' AND is_current=true
), expected AS (
 SELECT asset_id,source,currency,time_bucket('1 week',bar_time) AS bar_time,
 first(open,bar_time) AS open,max(high) AS high,min(low) AS low,last(close,bar_time) AS close,
 CASE WHEN count(volume)=count(*) THEN sum(volume) END AS volume
 FROM price_bar WHERE asset_id = (SELECT asset_id FROM selected)
 AND source='KIS' AND "interval"='1D' AND display_policy='FULL_OHLC'
 AND bar_time >= '2025-01-06T00:00:00Z' AND bar_time < '2025-07-28T00:00:00Z'
 GROUP BY asset_id,source,currency,time_bucket('1 week',bar_time)
), actual AS (
 SELECT asset_id,source,currency,bar_time,open,high,low,close,volume
 FROM price_bar_weekly WHERE asset_id = (SELECT asset_id FROM selected) AND source='KIS'
 AND bar_time >= '2025-01-06T00:00:00Z' AND bar_time < '2025-07-28T00:00:00Z'
), differences AS (
 (SELECT * FROM expected EXCEPT SELECT * FROM actual)
 UNION ALL
 (SELECT * FROM actual EXCEPT SELECT * FROM expected)
)
SELECT (SELECT count(*) FROM expected) AS expected_weeks,
       (SELECT count(*) FROM actual) AS actual_weeks,
       count(*) AS mismatched_rows FROM differences;

SELECT view_name,materialized_only FROM timescaledb_information.continuous_aggregates;
SELECT application_name,schedule_interval,scheduled FROM timescaledb_information.jobs
WHERE proc_name='policy_refresh_continuous_aggregate';
