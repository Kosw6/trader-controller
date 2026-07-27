package postgres

import (
	"context"
	"strings"

	"github.com/kosw006/trader-controller/internal/domain"
)

// GetPriceQualitySummary aggregates chart-display health from price_bar and its open issue table.
func (s *Store) GetPriceQualitySummary(ctx context.Context) (domain.PriceQualitySummary, error) {
	result := domain.PriceQualitySummary{
		ByQualityStatus:  map[string]int64{},
		ByDisplayPolicy:  map[string]int64{},
		IssuesBySeverity: map[string]int64{},
		IssuesByCode:     map[string]int64{},
	}

	if err := s.pool.QueryRow(ctx, `
        SELECT GREATEST(COALESCE(c.reltuples, 0)::BIGINT, 0)
        FROM pg_class c
        WHERE c.oid = 'price_bar'::REGCLASS`).Scan(&result.TotalBars); err != nil {
		return domain.PriceQualitySummary{}, err
	}
	if err := s.pool.QueryRow(ctx, `
        SELECT COUNT(*)
        FROM price_bar_quality_issue
        WHERE issue_status = 'OPEN'`).Scan(&result.OpenIssues); err != nil {
		return domain.PriceQualitySummary{}, err
	}
	if err := scanCountMap(ctx, s, `
        SELECT severity, COUNT(*)
        FROM price_bar_quality_issue
        WHERE issue_status = 'OPEN'
        GROUP BY severity`, result.IssuesBySeverity); err != nil {
		return domain.PriceQualitySummary{}, err
	}
	if err := scanCountMap(ctx, s, `
        SELECT issue_code, COUNT(*)
        FROM price_bar_quality_issue
        WHERE issue_status = 'OPEN'
        GROUP BY issue_code`, result.IssuesByCode); err != nil {
		return domain.PriceQualitySummary{}, err
	}

	nonValidRows, err := s.pool.Query(ctx, `
        SELECT quality_status, display_policy, COUNT(*)
        FROM price_bar
        WHERE quality_status IN ('PARTIAL', 'INVALID', 'UNVERIFIED')
        GROUP BY quality_status, display_policy`)
	if err != nil {
		return domain.PriceQualitySummary{}, err
	}
	defer nonValidRows.Close()
	var nonValidTotal int64
	for nonValidRows.Next() {
		var qualityStatus, displayPolicy string
		var count int64
		if err := nonValidRows.Scan(&qualityStatus, &displayPolicy, &count); err != nil {
			return domain.PriceQualitySummary{}, err
		}
		result.ByQualityStatus[qualityStatus] += count
		result.ByDisplayPolicy[displayPolicy] += count
		nonValidTotal += count
		if displayPolicy == "HIDDEN" {
			result.HiddenBars += count
		}
	}
	if err := nonValidRows.Err(); err != nil {
		return domain.PriceQualitySummary{}, err
	}
	if result.TotalBars < nonValidTotal {
		result.TotalBars = nonValidTotal
	}
	result.ByQualityStatus["VALID"] = result.TotalBars - nonValidTotal
	result.ByDisplayPolicy["FULL_OHLC"] = result.ByQualityStatus["VALID"]
	result.ChartUsableBars = result.TotalBars - result.HiddenBars
	result.CandleUsableBars = result.ByQualityStatus["VALID"]
	return result, nil
}

// ListPriceQualityIssues returns recent price-quality rows with enough context for an operator.
func (s *Store) ListPriceQualityIssues(ctx context.Context, filter domain.PriceQualityIssueFilter) ([]domain.PriceQualityIssue, error) {
	filter = normalizePriceQualityIssueFilter(filter)
	rows, err := s.pool.Query(ctx, `
        SELECT
            pqi.id,
            pqi.price_bar_id,
            pqi.asset_id,
            a.ticker,
            COALESCE(a.name, ''),
            a.exchange,
            pqi.source,
            pqi."interval",
            pqi.bar_time,
            pb.quality_status,
            pb.display_policy,
            pqi.issue_code,
            pqi.field_name,
            pqi.severity,
            pqi.issue_status,
            pqi.observed_value::TEXT,
            pqi.detected_at,
            pqi.last_seen_at,
            pqi.resolved_at
        FROM price_bar_quality_issue pqi
        JOIN price_bar pb ON pb.id = pqi.price_bar_id
        JOIN asset a ON a.id = pqi.asset_id
        WHERE ($1 = '' OR pqi.issue_status = $1)
          AND ($2 = '' OR pqi.severity = $2)
          AND (
              $3 = ''
              OR a.ticker ILIKE '%' || $3 || '%'
              OR a.name ILIKE '%' || $3 || '%'
              OR pqi.issue_code ILIKE '%' || $3 || '%'
          )
        ORDER BY
            CASE pqi.severity
                WHEN 'ERROR' THEN 0
                WHEN 'WARNING' THEN 1
                ELSE 2
            END,
            pqi.detected_at DESC,
            pqi.id DESC
        LIMIT $4 OFFSET $5`,
		filter.Status,
		filter.Severity,
		filter.Query,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.PriceQualityIssue, 0)
	for rows.Next() {
		var value domain.PriceQualityIssue
		if err := rows.Scan(
			&value.ID,
			&value.PriceBarID,
			&value.AssetID,
			&value.Ticker,
			&value.AssetName,
			&value.Exchange,
			&value.Source,
			&value.Interval,
			&value.BarTime,
			&value.QualityStatus,
			&value.DisplayPolicy,
			&value.IssueCode,
			&value.FieldName,
			&value.Severity,
			&value.IssueStatus,
			&value.ObservedValue,
			&value.DetectedAt,
			&value.LastSeenAt,
			&value.ResolvedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// GetPriceBarInventory summarizes which assets have daily price bars and their covered range.
func (s *Store) GetPriceBarInventory(ctx context.Context, filter domain.AssetLinkFilter) (domain.PriceBarInventory, error) {
	filter = normalizeAssetLinkFilter(filter)
	result := domain.PriceBarInventory{
		Rows: []domain.PriceBarInventoryAsset{},
	}

	rows, err := s.pool.Query(ctx, `
        WITH target_assets AS (
            SELECT
                a.id,
                a.ticker,
                a.name,
                a.asset_type,
                a.exchange
            FROM asset a
            WHERE ($1 = '' OR a.asset_type = $1)
              AND ($2 = '' OR a.exchange = $2)
              AND (
                  $3 = ''
                  OR a.ticker ILIKE '%' || $3 || '%'
                  OR a.name ILIKE '%' || $3 || '%'
              )
            ORDER BY a.exchange, a.ticker
            LIMIT $4 OFFSET $5
        )
        SELECT
            ta.id,
            ta.ticker,
            ta.name,
            ta.asset_type,
            ta.exchange,
            coverage.source,
            coverage."interval",
            coverage.bar_count,
            coverage.min_bar_time,
            coverage.max_bar_time,
            coverage.last_collected_at
        FROM target_assets ta
        JOIN LATERAL (
            SELECT
                pb.source,
                pb."interval",
                COUNT(*)::BIGINT AS bar_count,
                MIN(pb.bar_time) AS min_bar_time,
                MAX(pb.bar_time) AS max_bar_time,
                MAX(pb.collected_at) AS last_collected_at
            FROM price_bar pb
            WHERE pb.asset_id = ta.id
            GROUP BY pb.source, pb."interval"
        ) coverage ON TRUE
        ORDER BY ta.exchange, ta.ticker, coverage.source, coverage."interval"`,
		filter.AssetType,
		filter.Exchange,
		filter.Query,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return domain.PriceBarInventory{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var value domain.PriceBarInventoryAsset
		if err := rows.Scan(
			&value.AssetID,
			&value.Ticker,
			&value.Name,
			&value.AssetType,
			&value.Exchange,
			&value.Source,
			&value.Interval,
			&value.BarCount,
			&value.MinBarTime,
			&value.MaxBarTime,
			&value.LastCollectedAt,
		); err != nil {
			return domain.PriceBarInventory{}, err
		}
		result.Rows = append(result.Rows, value)
		result.TotalBars += value.BarCount
		if value.MinBarTime != nil && (result.MinBarTime == nil || value.MinBarTime.Before(*result.MinBarTime)) {
			result.MinBarTime = value.MinBarTime
		}
		if value.MaxBarTime != nil && (result.MaxBarTime == nil || value.MaxBarTime.After(*result.MaxBarTime)) {
			result.MaxBarTime = value.MaxBarTime
		}
		if value.LastCollectedAt != nil && (result.LastCollectedAt == nil || value.LastCollectedAt.After(*result.LastCollectedAt)) {
			result.LastCollectedAt = value.LastCollectedAt
		}
	}
	assetsWithBars := make(map[int64]struct{}, len(result.Rows))
	for _, row := range result.Rows {
		assetsWithBars[row.AssetID] = struct{}{}
	}
	result.AssetsWithBars = int64(len(assetsWithBars))
	return result, rows.Err()
}

// scanCountMap is shared by summary cards that are represented as key-count maps.
func scanCountMap(ctx context.Context, s *Store, query string, target map[string]int64) error {
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return err
		}
		target[key] = count
	}
	return rows.Err()
}

// normalizePriceQualityIssueFilter keeps the database query bounded and compares enum filters consistently.
func normalizePriceQualityIssueFilter(filter domain.PriceQualityIssueFilter) domain.PriceQualityIssueFilter {
	filter.Status = strings.TrimSpace(strings.ToUpper(filter.Status))
	filter.Severity = strings.TrimSpace(strings.ToUpper(filter.Severity))
	filter.Query = strings.TrimSpace(filter.Query)
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 200 {
		filter.Limit = 200
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	return filter
}
