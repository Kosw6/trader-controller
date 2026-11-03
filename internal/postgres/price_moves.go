package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
	"time"
)

func (s *Store) GetPriceMoveRule(ctx context.Context) (domain.PriceMoveRule, error) {
	var r domain.PriceMoveRule
	err := s.pool.QueryRow(ctx, `SELECT rise_percent,fall_percent,version,updated_at FROM admin_price_move_rule WHERE id=1`).Scan(&r.RisePercent, &r.FallPercent, &r.Version, &r.UpdatedAt)
	return r, mapError(err)
}

func (s *Store) SavePriceMoveRule(ctx context.Context, r domain.PriceMoveRule, actor string) (domain.PriceMoveRule, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return r, err
	}
	defer tx.Rollback(ctx)
	before := r.Version
	err = tx.QueryRow(ctx, `UPDATE admin_price_move_rule SET rise_percent=$1,fall_percent=$2,version=version+1,updated_at=clock_timestamp() WHERE id=1 AND version=$3 RETURNING rise_percent,fall_percent,version,updated_at`, r.RisePercent, r.FallPercent, before).Scan(&r.RisePercent, &r.FallPercent, &r.Version, &r.UpdatedAt)
	if err != nil {
		if mapError(err) == store.ErrNotFound {
			return r, fmt.Errorf("%w: 다른 관리자가 설정을 변경했습니다. 새로고침해주세요", store.ErrConflict)
		}
		return r, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO admin_price_move_rule_history(version,rise_percent,fall_percent,updated_by) VALUES($1,$2,$3,$4)`, r.Version, r.RisePercent, r.FallPercent, actor)
	if err != nil {
		return r, err
	}
	return r, tx.Commit(ctx)
}

// Index probes are bounded per asset; never LAG across the full price history.
const PriceMoveLatestSQL = `SELECT to_char(max(p.bar_time) AT TIME ZONE 'UTC','YYYY-MM-DD') FROM asset a
 CROSS JOIN LATERAL (SELECT bar_time FROM admin_price_move_input p WHERE p.asset_id=a.id
 AND p.bar_time<(CURRENT_DATE+1)::timestamp AT TIME ZONE 'UTC'
 AND p.bar_time>=(CURRENT_DATE-45)::timestamp AT TIME ZONE 'UTC'
 AND p.quality_status IN ('VALID','PARTIAL') AND p.display_policy IN ('FULL_OHLC','CLOSE_ONLY')
 AND p.close>0 AND p.close::text NOT IN ('NaN','Infinity','-Infinity')
 ORDER BY bar_time DESC LIMIT 1)p
 WHERE a.active AND a.asset_type='EQUITY'`

const PriceMoveSQL = `WITH targets AS MATERIALIZED (
 SELECT id,ticker,name,exchange FROM asset WHERE active AND asset_type='EQUITY'
 AND ($2='' OR strpos(lower(ticker),lower($2))>0 OR strpos(lower(name),lower($2))>0)
), paired AS MATERIALIZED (
 SELECT a.*,c.n current_n,c.close,c.currency,c.ok current_ok,
 b.n previous_n,b.close previous_close,b.currency previous_currency,b.ok previous_ok,b.bar_time previous_time
 FROM targets a
 LEFT JOIN LATERAL (
  SELECT count(*) n,min(close) close,min(currency) currency,
   bool_and(quality_status IN ('VALID','PARTIAL') AND display_policy IN ('FULL_OHLC','CLOSE_ONLY')
    AND close>0 AND close::text NOT IN ('NaN','Infinity','-Infinity')) ok
  FROM admin_price_move_input p WHERE p.asset_id=a.id AND p.bar_time=$1::date::timestamp AT TIME ZONE 'UTC'
 )c ON TRUE
 LEFT JOIN LATERAL (
  SELECT bar_time,count(*) n,min(close) close,min(currency) currency,
   bool_and(quality_status IN ('VALID','PARTIAL') AND display_policy IN ('FULL_OHLC','CLOSE_ONLY')
    AND close>0 AND close::text NOT IN ('NaN','Infinity','-Infinity')) ok
  FROM admin_price_move_input p WHERE p.asset_id=a.id
   AND p.bar_time<$1::date::timestamp AT TIME ZONE 'UTC'
   AND p.bar_time>=($1::date-7)::timestamp AT TIME ZONE 'UTC'
  GROUP BY bar_time ORDER BY bar_time DESC LIMIT 1
 )b ON TRUE
), valid AS MATERIALIZED (
 SELECT *,100*(close-previous_close)/nullif(previous_close,0) percent,
 $1::date-(previous_time AT TIME ZONE 'UTC')::date gap_days
 FROM paired WHERE current_n=1 AND previous_n=1 AND current_ok AND previous_ok
 AND currency IS NOT NULL AND currency=previous_currency
 AND previous_time=(previous_time AT TIME ZONE 'UTC')::date::timestamp AT TIME ZONE 'UTC'
), matched AS MATERIALIZED (
 SELECT * FROM valid WHERE
 (($3 IN ('ALL','UP') AND percent >= $4::numeric) OR ($3 IN ('ALL','DOWN') AND percent <= -$5::numeric))
), page AS (
 SELECT id AS "assetId",ticker,name,exchange,$1::text AS date,
 to_char(previous_time AT TIME ZONE 'UTC','YYYY-MM-DD') AS "previousDate",
 close::text,previous_close::text AS "previousClose",percent::text,currency,gap_days AS "gapDays"
 FROM matched ORDER BY abs(percent) DESC,ticker,exchange,id LIMIT 100 OFFSET $6
)
SELECT json_build_object('rows',coalesce((SELECT json_agg(page ORDER BY abs(percent::numeric) DESC,ticker,exchange,"assetId") FROM page),'[]'::json),
 'total',(SELECT count(*) FROM matched),'assets',(SELECT count(*) FROM paired),
 'missingCurrent',(SELECT count(*) FROM paired WHERE current_n=0),
 'rejectedPairs',(SELECT count(*) FROM paired WHERE current_n>0)-(SELECT count(*) FROM valid))`

func (s *Store) ListPriceMoves(ctx context.Context, f domain.PriceMoveFilter) (domain.PriceMoveResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	rule, err := s.GetPriceMoveRule(ctx)
	if err != nil {
		return domain.PriceMoveResult{}, err
	}
	r := domain.PriceMoveResult{Rule: rule, Date: f.Date, Rows: []domain.PriceMove{}}
	r.Mode = f.Mode
	if f.Start != "" {
		r.Start = f.Start
		r.End = f.End
		r.Date = f.End
	}
	if r.Date == "" {
		var day *string
		err = s.pool.QueryRow(ctx, PriceMoveLatestSQL).Scan(&day)
		if err != nil {
			return r, err
		}
		if day == nil {
			r.CheckedAt = time.Now().UTC()
			return r, nil
		}
		r.Date = *day
	}
	var raw []byte
	if f.Mode == "SINGLE" || f.Mode == "" {
		r.Start = r.Date
		r.End = r.Date
		err = s.pool.QueryRow(ctx, PriceMoveSQL, r.Date, f.Query, f.Direction, rule.RisePercent, rule.FallPercent, f.Offset).Scan(&raw)
	} else {
		if r.End == "" {
			r.End = r.Date
			end, _ := time.Parse("2006-01-02", r.End)
			r.Start = end.AddDate(0, 0, -6).Format("2006-01-02")
		}
		err = s.pool.QueryRow(ctx, PriceMoveRangeSQL, r.Start, r.End, f.Query, f.Direction, rule.RisePercent, rule.FallPercent, f.Offset, f.Mode).Scan(&raw)
	}
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	r.HasMore = int64(f.Offset+len(r.Rows)) < r.Total
	r.CheckedAt = time.Now().UTC()
	return r, nil
}
