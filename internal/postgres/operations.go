package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

const outboxColumns = `
    id, source_object_id, job_item_id, event_type, topic, message_key,
    idempotency_key, payload, status, attempt_count, next_attempt_at,
    published_at, last_error, kafka_partition, kafka_offset, created_at, updated_at`

const processedEventColumns = `
    consumer_name, idempotency_key, topic, partition_no, offset_no,
    status, error_message, processed_at`

const coverageColumns = `
    id, dataset_type, entity_type, entity_key, period_type, period_start,
    period_end, status, expected_count, actual_count, missing_items,
    latest_source_at, last_checked_at, resolution_job_id, reason_code`

const gapColumns = `
    id, coverage_id, gap_key, status, detected_at, resolved_at,
    resolution_job_id, details`

const macroSeriesColumns = `
    id, provider, external_series_id, name, category, frequency,
    unit, seasonal_adjustment, country_code`

// ListOutbox는 Kafka 발행 상태와 페이지 조건으로 Outbox 이벤트를 조회한다.
func (s *Store) ListOutbox(ctx context.Context, filter domain.ListFilter) ([]domain.OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+outboxColumns+`
        FROM pipeline_outbox
        WHERE ($1 = '' OR status = $1)
        ORDER BY id DESC
        LIMIT $2 OFFSET $3`, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.OutboxEvent, 0)
	for rows.Next() {
		value, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// RetryOutbox는 재시도 가능한 실패 이벤트만 PENDING으로 되돌린다.
func (s *Store) RetryOutbox(ctx context.Context, id int64) (domain.OutboxEvent, error) {
	value, err := scanOutbox(s.pool.QueryRow(ctx, `
        UPDATE pipeline_outbox
        SET status = 'PENDING',
            next_attempt_at = NULL,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1 AND status IN ('DEAD', 'RETRY_WAIT')
        RETURNING `+outboxColumns, id))
	if err != nil {
		if mapError(err) == store.ErrNotFound {
			return domain.OutboxEvent{}, store.ErrInvalidState
		}
		return domain.OutboxEvent{}, err
	}
	return value, nil
}

// ListProcessedEvents는 Python Worker가 Kafka 메시지를 처리한 결과를 최신순으로 조회한다.
func (s *Store) ListProcessedEvents(ctx context.Context, filter domain.ListFilter) ([]domain.ProcessedEvent, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+processedEventColumns+`
        FROM processed_event
        WHERE ($1 = '' OR status = $1)
        ORDER BY processed_at DESC
        LIMIT $2 OFFSET $3`, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.ProcessedEvent, 0)
	for rows.Next() {
		value, err := scanProcessedEvent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// ListCoverage는 최신 검사순으로 데이터 완전성 결과를 조회한다.
func (s *Store) ListCoverage(ctx context.Context, filter domain.ListFilter) ([]domain.Coverage, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+coverageColumns+`
        FROM data_coverage
        WHERE ($1 = '' OR status = $1)
        ORDER BY last_checked_at DESC, id DESC
        LIMIT $2 OFFSET $3`, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.Coverage, 0)
	for rows.Next() {
		value, err := scanCoverage(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// ListGaps는 탐지시각 역순으로 누락 데이터와 복구 상태를 조회한다.
func (s *Store) ListGaps(ctx context.Context, filter domain.ListFilter) ([]domain.Gap, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+gapColumns+`
        FROM data_gap
        WHERE ($1 = '' OR status = $1)
        ORDER BY detected_at DESC, id DESC
        LIMIT $2 OFFSET $3`, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.Gap, 0)
	for rows.Next() {
		value, err := scanGap(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// BackfillGap은 OPEN Gap을 잠그고 백필 Job 생성과 상태 연결을 한 트랜잭션으로 수행한다.
func (s *Store) BackfillGap(
	ctx context.Context,
	id int64,
	req domain.BackfillGapRequest,
	idempotencyKey string,
) (domain.JobDetail, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.JobDetail{}, err
	}
	defer tx.Rollback(ctx)

	var status string
	err = tx.QueryRow(ctx, `
        SELECT status
        FROM data_gap
        WHERE id = $1
        FOR UPDATE`, id).Scan(&status)
	if err != nil {
		return domain.JobDetail{}, mapError(err)
	}
	if status != "OPEN" {
		return domain.JobDetail{}, store.ErrInvalidState
	}

	now := time.Now().UTC()
	job, err := insertJob(ctx, tx, domain.CreateJobRequest{
		JobType:        req.JobType,
		Source:         req.Source,
		Priority:       req.Priority,
		ScheduledAt:    &now,
		RequestedBy:    req.RequestedBy,
		IdempotencyKey: idempotencyKey,
		Parameters:     req.Parameters,
		ItemKeys:       req.ItemKeys,
	}, nil, 0)
	if err != nil {
		return domain.JobDetail{}, mapError(err)
	}
	_, err = tx.Exec(ctx, `
        UPDATE data_gap
        SET status = 'BACKFILL_SCHEDULED', resolution_job_id = $2
        WHERE id = $1`, id, job.ID)
	if err != nil {
		return domain.JobDetail{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.JobDetail{}, err
	}
	return s.GetJob(ctx, job.ID)
}

// GetSummary는 관리자 Overview에 필요한 테이블별 상태 개수를 집계한다.
func (s *Store) GetSummary(ctx context.Context) (domain.Summary, error) {
	result := domain.Summary{
		JobsByStatus:     map[string]int64{},
		OutboxByStatus:   map[string]int64{},
		CoverageByStatus: map[string]int64{},
	}
	if err := s.pool.QueryRow(ctx, `
        SELECT COUNT(*) FROM collection_schedule WHERE enabled = TRUE`).Scan(&result.SchedulesEnabled); err != nil {
		return domain.Summary{}, err
	}
	var err error
	if result.JobsByStatus, err = countByStatus(ctx, s.pool, "pipeline_job"); err != nil {
		return domain.Summary{}, err
	}
	if result.OutboxByStatus, err = countByStatus(ctx, s.pool, "pipeline_outbox"); err != nil {
		return domain.Summary{}, err
	}
	if result.CoverageByStatus, err = countByStatus(ctx, s.pool, "data_coverage"); err != nil {
		return domain.Summary{}, err
	}
	if err := s.pool.QueryRow(ctx, `
        SELECT COUNT(*) FROM data_gap WHERE status = 'OPEN'`).Scan(&result.OpenGaps); err != nil {
		return domain.Summary{}, err
	}
	return result, nil
}

// ListMacroSeries는 관리자가 수집할 BLS 지표를 선택할 때 사용할 매크로 지표 카탈로그를 조회한다.
func (s *Store) ListMacroSeries(ctx context.Context, provider string, filter domain.ListFilter) ([]domain.MacroSeries, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+macroSeriesColumns+`
        FROM macro_series
        WHERE ($1 = '' OR provider = $1)
          AND ($2 = '' OR category = $2)
        ORDER BY category, external_series_id
        LIMIT $3 OFFSET $4`, provider, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.MacroSeries, 0)
	for rows.Next() {
		value, err := scanMacroSeries(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// GetBLSInventory는 BLS 매크로 데이터가 category, series, year 단위로 얼마나 적재됐는지 집계한다.
func (s *Store) GetBLSInventory(ctx context.Context) (domain.BLSInventory, error) {
	result := domain.BLSInventory{
		Categories: []domain.BLSInventoryCategory{},
		Series:     []domain.BLSInventorySeries{},
	}
	if err := s.pool.QueryRow(ctx, `
        SELECT
            COUNT(DISTINCT ms.id),
            COUNT(mo.id),
            MIN(mo.reference_period),
            MAX(mo.reference_period),
            MAX(mo.collected_at)
        FROM macro_series ms
        LEFT JOIN macro_observation mo ON mo.series_id = ms.id
        WHERE ms.provider = 'BLS'`).Scan(
		&result.TotalSeries,
		&result.TotalObservations,
		&result.MinReferencePeriod,
		&result.MaxReferencePeriod,
		&result.LastCollectedAt,
	); err != nil {
		return domain.BLSInventory{}, err
	}

	categoryRows, err := s.pool.Query(ctx, `
        SELECT
            ms.category,
            COUNT(DISTINCT ms.id),
            COUNT(mo.id),
            MIN(mo.reference_period),
            MAX(mo.reference_period),
            MAX(mo.collected_at)
        FROM macro_series ms
        LEFT JOIN macro_observation mo ON mo.series_id = ms.id
        WHERE ms.provider = 'BLS'
        GROUP BY ms.category
        ORDER BY ms.category`)
	if err != nil {
		return domain.BLSInventory{}, err
	}
	for categoryRows.Next() {
		var value domain.BLSInventoryCategory
		if err := categoryRows.Scan(
			&value.Category,
			&value.SeriesCount,
			&value.ObservationCount,
			&value.MinReferencePeriod,
			&value.MaxReferencePeriod,
			&value.LastCollectedAt,
		); err != nil {
			categoryRows.Close()
			return domain.BLSInventory{}, err
		}
		result.Categories = append(result.Categories, value)
	}
	categoryRows.Close()
	if err := categoryRows.Err(); err != nil {
		return domain.BLSInventory{}, err
	}

	seriesRows, err := s.pool.Query(ctx, `
        SELECT
            ms.id,
            ms.external_series_id,
            ms.name,
            ms.category,
            ms.frequency,
            ms.unit,
            COUNT(mo.id),
            MIN(mo.reference_period),
            MAX(mo.reference_period),
            MAX(mo.collected_at)
        FROM macro_series ms
        LEFT JOIN macro_observation mo ON mo.series_id = ms.id
        WHERE ms.provider = 'BLS'
        GROUP BY ms.id, ms.external_series_id, ms.name, ms.category, ms.frequency, ms.unit
        ORDER BY ms.category, ms.external_series_id`)
	if err != nil {
		return domain.BLSInventory{}, err
	}
	seriesIndexByID := map[int64]int{}
	for seriesRows.Next() {
		var value domain.BLSInventorySeries
		if err := seriesRows.Scan(
			&value.ID,
			&value.ExternalSeriesID,
			&value.Name,
			&value.Category,
			&value.Frequency,
			&value.Unit,
			&value.ObservationCount,
			&value.MinReferencePeriod,
			&value.MaxReferencePeriod,
			&value.LastCollectedAt,
		); err != nil {
			seriesRows.Close()
			return domain.BLSInventory{}, err
		}
		value.Yearly = []domain.BLSInventoryYear{}
		seriesIndexByID[value.ID] = len(result.Series)
		result.Series = append(result.Series, value)
	}
	seriesRows.Close()
	if err := seriesRows.Err(); err != nil {
		return domain.BLSInventory{}, err
	}

	yearRows, err := s.pool.Query(ctx, `
        SELECT
            ms.id,
            LEFT(mo.reference_period, 4) AS year,
            COUNT(mo.id),
            ARRAY_AGG(mo.reference_period ORDER BY mo.reference_period)
        FROM macro_series ms
        JOIN macro_observation mo ON mo.series_id = ms.id
        WHERE ms.provider = 'BLS'
        GROUP BY ms.id, LEFT(mo.reference_period, 4)
        ORDER BY ms.id, year`)
	if err != nil {
		return domain.BLSInventory{}, err
	}
	for yearRows.Next() {
		var seriesID int64
		var value domain.BLSInventoryYear
		if err := yearRows.Scan(&seriesID, &value.Year, &value.ObservationCount, &value.Periods); err != nil {
			yearRows.Close()
			return domain.BLSInventory{}, err
		}
		value.ExpectedCount = 12
		if index, ok := seriesIndexByID[seriesID]; ok {
			result.Series[index].Yearly = append(result.Series[index].Yearly, value)
		}
	}
	yearRows.Close()
	if err := yearRows.Err(); err != nil {
		return domain.BLSInventory{}, err
	}

	statusRows, err := s.pool.Query(ctx, `
        SELECT
            ms.id,
            LEFT(mos.reference_period, 4) AS year,
            mos.reference_period,
            mos.status,
            mos.reason_code,
            mos.reason_message,
            mos.raw_value
        FROM macro_series ms
        JOIN macro_observation_status mos ON mos.series_id = ms.id
        WHERE ms.provider = 'BLS'
        ORDER BY ms.id, year, mos.reference_period`)
	if err != nil {
		return domain.BLSInventory{}, err
	}
	for statusRows.Next() {
		var seriesID int64
		var year string
		var unavailable domain.BLSInventoryUnavailablePeriod
		if err := statusRows.Scan(
			&seriesID,
			&year,
			&unavailable.ReferencePeriod,
			&unavailable.Status,
			&unavailable.ReasonCode,
			&unavailable.ReasonMessage,
			&unavailable.RawValue,
		); err != nil {
			statusRows.Close()
			return domain.BLSInventory{}, err
		}
		if index, ok := seriesIndexByID[seriesID]; ok {
			result.Series[index].Yearly = appendBLSUnavailablePeriod(
				result.Series[index].Yearly,
				year,
				unavailable,
			)
		}
	}
	statusRows.Close()
	if err := statusRows.Err(); err != nil {
		return domain.BLSInventory{}, err
	}
	return result, nil
}

func appendBLSUnavailablePeriod(
	years []domain.BLSInventoryYear,
	year string,
	unavailable domain.BLSInventoryUnavailablePeriod,
) []domain.BLSInventoryYear {
	for index := range years {
		if years[index].Year == year {
			years[index].UnavailablePeriods = append(years[index].UnavailablePeriods, unavailable)
			years[index].UnavailableCount++
			return years
		}
	}
	return append(years, domain.BLSInventoryYear{
		Year:               year,
		ExpectedCount:      12,
		Periods:            []string{},
		UnavailableCount:   1,
		UnavailablePeriods: []domain.BLSInventoryUnavailablePeriod{unavailable},
	})
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// countByStatus는 허용 목록에 포함된 테이블만 동적 SQL로 상태별 집계한다.
func countByStatus(ctx context.Context, db queryer, table string) (map[string]int64, error) {
	allowed := map[string]bool{
		"pipeline_job":    true,
		"pipeline_outbox": true,
		"data_coverage":   true,
	}
	if !allowed[table] {
		return nil, fmt.Errorf("unsupported status table: %s", table)
	}
	rows, err := db.Query(ctx, "SELECT status, COUNT(*) FROM "+table+" GROUP BY status")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		result[status] = count
	}
	return result, rows.Err()
}

// scanOutbox는 SQL 조회 결과를 OutboxEvent 구조체로 매핑한다.
func scanOutbox(row rowScanner) (domain.OutboxEvent, error) {
	var value domain.OutboxEvent
	err := row.Scan(
		&value.ID,
		&value.SourceObjectID,
		&value.JobItemID,
		&value.EventType,
		&value.Topic,
		&value.MessageKey,
		&value.IdempotencyKey,
		&value.Payload,
		&value.Status,
		&value.AttemptCount,
		&value.NextAttemptAt,
		&value.PublishedAt,
		&value.LastError,
		&value.KafkaPartition,
		&value.KafkaOffset,
		&value.CreatedAt,
		&value.UpdatedAt,
	)
	return value, err
}

// scanProcessedEvent는 SQL 조회 결과를 ProcessedEvent 구조체로 매핑한다.
func scanProcessedEvent(row rowScanner) (domain.ProcessedEvent, error) {
	var value domain.ProcessedEvent
	err := row.Scan(
		&value.ConsumerName,
		&value.IdempotencyKey,
		&value.Topic,
		&value.PartitionNo,
		&value.OffsetNo,
		&value.Status,
		&value.ErrorMessage,
		&value.ProcessedAt,
	)
	return value, err
}

// scanCoverage는 SQL 조회 결과를 Coverage 구조체로 매핑한다.
func scanCoverage(row rowScanner) (domain.Coverage, error) {
	var value domain.Coverage
	err := row.Scan(
		&value.ID,
		&value.DatasetType,
		&value.EntityType,
		&value.EntityKey,
		&value.PeriodType,
		&value.PeriodStart,
		&value.PeriodEnd,
		&value.Status,
		&value.ExpectedCount,
		&value.ActualCount,
		&value.MissingItems,
		&value.LatestSourceAt,
		&value.LastCheckedAt,
		&value.ResolutionJobID,
		&value.ReasonCode,
	)
	return value, err
}

// scanGap은 SQL 조회 결과를 Gap 구조체로 매핑한다.
func scanGap(row rowScanner) (domain.Gap, error) {
	var value domain.Gap
	err := row.Scan(
		&value.ID,
		&value.CoverageID,
		&value.GapKey,
		&value.Status,
		&value.DetectedAt,
		&value.ResolvedAt,
		&value.ResolutionJobID,
		&value.Details,
	)
	return value, err
}

// scanMacroSeries는 macro_series 조회 결과를 API 응답 모델로 변환한다.
func scanMacroSeries(row rowScanner) (domain.MacroSeries, error) {
	var value domain.MacroSeries
	err := row.Scan(
		&value.ID,
		&value.Provider,
		&value.ExternalSeriesID,
		&value.Name,
		&value.Category,
		&value.Frequency,
		&value.Unit,
		&value.SeasonalAdjustment,
		&value.CountryCode,
	)
	return value, err
}
