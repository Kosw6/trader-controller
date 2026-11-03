package postgres

// A bounded date range plus seven days of predecessor context, not the full history.
const PriceMoveRangeSQL = `WITH targets AS MATERIALIZED (
 SELECT id,ticker,name,exchange FROM asset WHERE active AND asset_type='EQUITY'
 AND ($3='' OR strpos(lower(ticker),lower($3))>0 OR strpos(lower(name),lower($3))>0)
), observations AS MATERIALIZED (
 SELECT a.*,d.* FROM targets a CROSS JOIN LATERAL (
  SELECT bar_time,count(*) n,min(close) close,min(currency) currency,
   bool_and(quality_status IN ('VALID','PARTIAL') AND display_policy IN ('FULL_OHLC','CLOSE_ONLY')
    AND close>0 AND close::text NOT IN ('NaN','Infinity','-Infinity')) ok
  FROM admin_price_move_input p WHERE p.asset_id=a.id
   AND p.bar_time>=($1::date-7)::timestamp AT TIME ZONE 'UTC'
   AND p.bar_time<($2::date+1)::timestamp AT TIME ZONE 'UTC'
  GROUP BY bar_time
 )d
), series AS (
 SELECT *,lag(n) OVER w previous_n,lag(close) OVER w previous_close,
 lag(currency) OVER w previous_currency,lag(ok) OVER w previous_ok,lag(bar_time) OVER w previous_time
 FROM observations WHERE $8='EVENTS' WINDOW w AS (PARTITION BY id ORDER BY bar_time)
), first_in_range AS (
 SELECT DISTINCT ON(id) * FROM observations WHERE $8='PERIOD'
 AND bar_time >= $1::date::timestamp AT TIME ZONE 'UTC' ORDER BY id,bar_time
), last_in_range AS (
 SELECT DISTINCT ON(id) * FROM observations WHERE $8='PERIOD'
 AND bar_time >= $1::date::timestamp AT TIME ZONE 'UTC' ORDER BY id,bar_time DESC
), paired AS MATERIALIZED (
 SELECT id,ticker,name,exchange,bar_time,n,close,currency,ok,
 previous_n,previous_close,previous_currency,previous_ok,previous_time
 FROM series WHERE bar_time >= $1::date::timestamp AT TIME ZONE 'UTC'
 UNION ALL
 SELECT e.id,e.ticker,e.name,e.exchange,e.bar_time,e.n,e.close,e.currency,e.ok,
 b.n,b.close,b.currency,b.ok,b.bar_time FROM last_in_range e JOIN first_in_range b USING(id)
), valid AS MATERIALIZED (
 SELECT *,100*(close-previous_close)/nullif(previous_close,0) percent,
 (bar_time AT TIME ZONE 'UTC')::date-(previous_time AT TIME ZONE 'UTC')::date gap_days
 FROM paired WHERE n=1 AND previous_n=1 AND ok AND previous_ok
 AND currency IS NOT NULL AND currency=previous_currency AND previous_time<bar_time
 AND bar_time=(bar_time AT TIME ZONE 'UTC')::date::timestamp AT TIME ZONE 'UTC'
 AND previous_time=(previous_time AT TIME ZONE 'UTC')::date::timestamp AT TIME ZONE 'UTC'
 AND ($8='PERIOD' OR bar_time-previous_time<=interval '7 days')
), matched AS MATERIALIZED (
 SELECT * FROM valid WHERE
 (($4 IN ('ALL','UP') AND percent >= $5::numeric) OR ($4 IN ('ALL','DOWN') AND percent <= -$6::numeric))
), page AS (
 SELECT id AS "assetId",ticker,name,exchange,to_char(bar_time AT TIME ZONE 'UTC','YYYY-MM-DD') AS date,
 to_char(previous_time AT TIME ZONE 'UTC','YYYY-MM-DD') AS "previousDate",
 close::text,previous_close::text AS "previousClose",percent::text,currency,gap_days AS "gapDays"
 FROM matched ORDER BY abs(percent) DESC,bar_time DESC,ticker,exchange,id LIMIT 100 OFFSET $7
)
SELECT json_build_object('rows',coalesce((SELECT json_agg(page ORDER BY abs(percent::numeric) DESC,date DESC,ticker,exchange,"assetId") FROM page),'[]'::json),
 'total',(SELECT count(*) FROM matched),'assets',(SELECT count(*) FROM targets),
 'observations',(SELECT count(*) FROM observations WHERE bar_time >= $1::date::timestamp AT TIME ZONE 'UTC'),
 'missingCurrent',(SELECT count(*) FROM targets t WHERE NOT EXISTS(SELECT 1 FROM observations o WHERE o.id=t.id AND o.bar_time >= $1::date::timestamp AT TIME ZONE 'UTC')),
 'rejectedPairs',(SELECT count(*) FROM paired)-(SELECT count(*) FROM valid))`
