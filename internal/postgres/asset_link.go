package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

const assetCompanyMatchSelect = `
    SELECT
        m.id, m.asset_id, a.ticker, a.name, a.asset_type, a.exchange,
        m.company_id, c.cik, c.company_name,
        m.match_method, m.match_confidence, m.status,
        m.sec_source_object_id, m.market_source_object_id, m.evidence,
        m.matched_at, m.reviewed_at, m.reviewed_by, m.created_at, m.updated_at
    FROM asset_company_match m
    JOIN asset a ON a.id = m.asset_id
    JOIN company c ON c.id = m.company_id`

// GetAssetCompanyLinkSummary는 Asset 연결과 Match 검토 상태를 관리자 요약으로 집계한다.
func (s *Store) GetAssetCompanyLinkSummary(ctx context.Context) (domain.AssetCompanyLinkSummary, error) {
	result := domain.AssetCompanyLinkSummary{MatchesByStatus: map[string]int64{}}
	err := s.pool.QueryRow(ctx, `
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE asset_type = 'EQUITY'),
            COUNT(*) FILTER (WHERE company_id IS NOT NULL),
            COUNT(*) FILTER (WHERE asset_type = 'EQUITY' AND company_id IS NULL)
        FROM asset`).Scan(
		&result.TotalAssets,
		&result.EquityAssets,
		&result.LinkedAssets,
		&result.UnlinkedEquities,
	)
	if err != nil {
		return domain.AssetCompanyLinkSummary{}, err
	}

	rows, err := s.pool.Query(ctx, `
        SELECT status, COUNT(*)
        FROM asset_company_match
        GROUP BY status`)
	if err != nil {
		return domain.AssetCompanyLinkSummary{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return domain.AssetCompanyLinkSummary{}, err
		}
		result.MatchesByStatus[status] = count
	}
	return result, rows.Err()
}

// ListAssetCompanyMatches는 상태·Asset 유형·거래소·검색어로 연결 후보를 조회한다.
func (s *Store) ListAssets(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.AssetOption, error) {
	filter = normalizeAssetLinkFilter(filter)
	rows, err := s.pool.Query(ctx, `
        SELECT
            a.id, a.ticker, a.name, a.asset_type, a.exchange, a.currency,
            COALESCE(aps.provider, ''),
            COALESCE(aps.provider_exchange_code, ''),
            COALESCE(aps.provider_symbol, '')
        FROM asset a
        LEFT JOIN LATERAL (
            SELECT provider, provider_exchange_code, provider_symbol
            FROM asset_provider_symbol
            WHERE asset_id = a.id
              AND provider = 'KIS'
              AND is_current = TRUE
            ORDER BY id DESC
            LIMIT 1
        ) aps ON TRUE
        WHERE a.active = TRUE
          AND ($1 = '' OR a.asset_type = $1)
          AND ($2 = '' OR a.exchange = $2)
          AND (
              $3 = ''
              OR a.ticker ILIKE '%' || $3 || '%'
              OR a.name ILIKE '%' || $3 || '%'
              OR aps.provider_symbol ILIKE '%' || $3 || '%'
          )
        ORDER BY a.exchange, a.ticker, a.id
        LIMIT $4 OFFSET $5`,
		filter.AssetType,
		filter.Exchange,
		filter.Query,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.AssetOption, 0)
	for rows.Next() {
		var value domain.AssetOption
		if err := rows.Scan(
			&value.ID,
			&value.Ticker,
			&value.Name,
			&value.AssetType,
			&value.Exchange,
			&value.Currency,
			&value.Provider,
			&value.ProviderExchangeCode,
			&value.ProviderSymbol,
		); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (s *Store) ListAssetCompanyMatches(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.AssetCompanyMatch, error) {
	filter = normalizeAssetLinkFilter(filter)
	rows, err := s.pool.Query(ctx, assetCompanyMatchSelect+`
        WHERE ($1 = '' OR m.status = $1)
          AND ($2 = '' OR a.asset_type = $2)
          AND ($3 = '' OR a.exchange = $3)
          AND (
              $4 = ''
              OR a.ticker ILIKE '%' || $4 || '%'
              OR a.name ILIKE '%' || $4 || '%'
              OR c.cik ILIKE '%' || $4 || '%'
              OR c.company_name ILIKE '%' || $4 || '%'
          )
        ORDER BY
            CASE m.status
                WHEN 'REVIEW_REQUIRED' THEN 0
                WHEN 'CANDIDATE' THEN 1
                WHEN 'CONFIRMED' THEN 2
                ELSE 3
            END,
            m.match_confidence DESC,
            m.id
        LIMIT $5 OFFSET $6`,
		filter.Status,
		filter.AssetType,
		filter.Exchange,
		filter.Query,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.AssetCompanyMatch, 0)
	for rows.Next() {
		value, err := scanAssetCompanyMatch(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// ListUnmatchedAssets는 확정 Company가 없는 Asset과 현재 KIS 식별자를 조회한다.
func (s *Store) ListUnmatchedAssets(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.UnmatchedAsset, error) {
	filter = normalizeAssetLinkFilter(filter)
	rows, err := s.pool.Query(ctx, `
        SELECT
            a.id, a.ticker, a.name, a.asset_type, a.exchange,
            COALESCE(aps.provider, ''),
            COALESCE(aps.provider_exchange_code, ''),
            COALESCE(aps.provider_symbol, '')
        FROM asset a
        LEFT JOIN LATERAL (
            SELECT provider, provider_exchange_code, provider_symbol
            FROM asset_provider_symbol
            WHERE asset_id = a.id
              AND provider = 'KIS'
              AND is_current = TRUE
            ORDER BY id DESC
            LIMIT 1
        ) aps ON TRUE
        WHERE a.company_id IS NULL
          AND a.active = TRUE
          AND ($1 = '' OR a.asset_type = $1)
          AND ($2 = '' OR a.exchange = $2)
          AND (
              $3 = ''
              OR a.ticker ILIKE '%' || $3 || '%'
              OR a.name ILIKE '%' || $3 || '%'
              OR EXISTS (
                  SELECT 1
                  FROM asset_alias aa
                  WHERE aa.asset_id = a.id
                    AND aa.alias ILIKE '%' || $3 || '%'
              )
          )
        ORDER BY a.exchange, a.ticker, a.id
        LIMIT $4 OFFSET $5`,
		filter.AssetType,
		filter.Exchange,
		filter.Query,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.UnmatchedAsset, 0)
	for rows.Next() {
		var value domain.UnmatchedAsset
		if err := rows.Scan(
			&value.ID,
			&value.Ticker,
			&value.Name,
			&value.AssetType,
			&value.Exchange,
			&value.Provider,
			&value.ProviderExchangeCode,
			&value.ProviderSymbol,
		); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// ConfirmAssetCompanyMatch는 후보를 잠그고 Asset 현재 Company와 검토 이력을 함께 확정한다.
func (s *Store) ConfirmAssetCompanyMatch(ctx context.Context, id int64, reviewedBy string) (domain.AssetCompanyMatch, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.AssetCompanyMatch{}, err
	}
	defer tx.Rollback(ctx)

	var status string
	var assetID, companyID int64
	var currentCompanyID *int64
	err = tx.QueryRow(ctx, `
        SELECT m.status, m.asset_id, m.company_id, a.company_id
        FROM asset_company_match m
        JOIN asset a ON a.id = m.asset_id
        WHERE m.id = $1
        FOR UPDATE OF m, a`, id).Scan(
		&status, &assetID, &companyID, &currentCompanyID,
	)
	if err != nil {
		return domain.AssetCompanyMatch{}, mapError(err)
	}
	if status != "CANDIDATE" && status != "REVIEW_REQUIRED" {
		return domain.AssetCompanyMatch{}, store.ErrInvalidState
	}
	if currentCompanyID != nil && *currentCompanyID != companyID {
		return domain.AssetCompanyMatch{}, store.ErrConflict
	}

	_, err = tx.Exec(ctx, `
        UPDATE asset_company_match
        SET status = 'REJECTED',
            reviewed_at = CURRENT_TIMESTAMP,
            reviewed_by = $2,
            updated_at = CURRENT_TIMESTAMP
        WHERE asset_id = $1
          AND id <> $3
          AND status IN ('CANDIDATE', 'REVIEW_REQUIRED')`, assetID, reviewedBy, id)
	if err != nil {
		return domain.AssetCompanyMatch{}, err
	}
	_, err = tx.Exec(ctx, `
        UPDATE asset_company_match
        SET status = 'CONFIRMED',
            reviewed_at = CURRENT_TIMESTAMP,
            reviewed_by = $2,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1`, id, reviewedBy)
	if err != nil {
		return domain.AssetCompanyMatch{}, mapError(err)
	}
	_, err = tx.Exec(ctx, `
        UPDATE asset
        SET company_id = $2
        WHERE id = $1`, assetID, companyID)
	if err != nil {
		return domain.AssetCompanyMatch{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.AssetCompanyMatch{}, mapError(err)
	}
	return s.getAssetCompanyMatch(ctx, id)
}

// RejectAssetCompanyMatch는 미확정 후보를 거절하고 검토 주체와 시각을 기록한다.
func (s *Store) RejectAssetCompanyMatch(ctx context.Context, id int64, reviewedBy string) (domain.AssetCompanyMatch, error) {
	command, err := s.pool.Exec(ctx, `
        UPDATE asset_company_match
        SET status = 'REJECTED',
            reviewed_at = CURRENT_TIMESTAMP,
            reviewed_by = $2,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
          AND status IN ('CANDIDATE', 'REVIEW_REQUIRED')`, id, reviewedBy)
	if err != nil {
		return domain.AssetCompanyMatch{}, err
	}
	if command.RowsAffected() == 0 {
		var exists bool
		if err := s.pool.QueryRow(ctx, `
            SELECT EXISTS(SELECT 1 FROM asset_company_match WHERE id = $1)`, id).Scan(&exists); err != nil {
			return domain.AssetCompanyMatch{}, err
		}
		if !exists {
			return domain.AssetCompanyMatch{}, store.ErrNotFound
		}
		return domain.AssetCompanyMatch{}, store.ErrInvalidState
	}
	return s.getAssetCompanyMatch(ctx, id)
}

// getAssetCompanyMatch는 상태 변경 후 관리자 응답에 사용할 연결 상세를 조회한다.
func (s *Store) getAssetCompanyMatch(ctx context.Context, id int64) (domain.AssetCompanyMatch, error) {
	value, err := scanAssetCompanyMatch(s.pool.QueryRow(ctx,
		assetCompanyMatchSelect+` WHERE m.id = $1`, id))
	if err != nil {
		return domain.AssetCompanyMatch{}, mapError(err)
	}
	return value, nil
}

// scanAssetCompanyMatch는 Company·Asset 조인 행을 관리자 API 모델로 변환한다.
func scanAssetCompanyMatch(row rowScanner) (domain.AssetCompanyMatch, error) {
	var value domain.AssetCompanyMatch
	err := row.Scan(
		&value.ID,
		&value.AssetID,
		&value.AssetTicker,
		&value.AssetName,
		&value.AssetType,
		&value.Exchange,
		&value.CompanyID,
		&value.CIK,
		&value.CompanyName,
		&value.MatchMethod,
		&value.MatchConfidence,
		&value.Status,
		&value.SECSourceObjectID,
		&value.MarketSourceObjectID,
		&value.Evidence,
		&value.MatchedAt,
		&value.ReviewedAt,
		&value.ReviewedBy,
		&value.CreatedAt,
		&value.UpdatedAt,
	)
	return value, err
}

// normalizeAssetLinkFilter는 저장소 쿼리에 들어갈 문자열 공백을 제거한다.
func normalizeAssetLinkFilter(filter domain.AssetLinkFilter) domain.AssetLinkFilter {
	filter.Status = strings.TrimSpace(strings.ToUpper(filter.Status))
	filter.AssetType = strings.TrimSpace(strings.ToUpper(filter.AssetType))
	filter.Exchange = strings.TrimSpace(strings.ToUpper(filter.Exchange))
	filter.Query = strings.TrimSpace(filter.Query)
	return filter
}
