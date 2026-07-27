package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

const scheduleColumns = `
    id, job_type, cron_expression, timezone, enabled, parameters,
    last_run_at, next_run_at, created_at, updated_at`

// ListSchedules는 enabled·disabled 필터와 페이지 조건으로 Schedule을 조회한다.
func (s *Store) ListSchedules(ctx context.Context, filter domain.ListFilter) ([]domain.Schedule, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+scheduleColumns+`
        FROM collection_schedule
        WHERE ($1 = '')
           OR ($1 = 'enabled' AND enabled = TRUE)
           OR ($1 = 'disabled' AND enabled = FALSE)
        ORDER BY id DESC
        LIMIT $2 OFFSET $3`, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.Schedule, 0)
	for rows.Next() {
		value, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// GetSchedule은 PK로 Schedule 한 건을 조회하고 없는 행을 ErrNotFound로 변환한다.
func (s *Store) GetSchedule(ctx context.Context, id int64) (domain.Schedule, error) {
	value, err := scanSchedule(s.pool.QueryRow(ctx, `
        SELECT `+scheduleColumns+`
        FROM collection_schedule
        WHERE id = $1`, id))
	return value, mapError(err)
}

// CreateSchedule은 비활성 일정의 next_run_at을 비워둔 채 새 Schedule을 저장한다.
func (s *Store) CreateSchedule(ctx context.Context, req domain.CreateScheduleRequest, nextRun time.Time) (domain.Schedule, error) {
	parameters := normalizeJSON(req.Parameters)
	value, err := scanSchedule(s.pool.QueryRow(ctx, `
        INSERT INTO collection_schedule (
            job_type, cron_expression, timezone, enabled, parameters, next_run_at
        ) VALUES ($1, $2, $3, $4, $5, CASE WHEN $4 THEN $6::timestamptz ELSE NULL::timestamptz END)
        RETURNING `+scheduleColumns,
		req.JobType, req.CronExpression, req.Timezone, req.Enabled, parameters, nextRun))
	return value, mapError(err)
}

// UpdateSchedule은 설정과 다음 실행시각을 한 UPDATE로 변경한다.
func (s *Store) UpdateSchedule(ctx context.Context, id int64, req domain.UpdateScheduleRequest, nextRun time.Time) (domain.Schedule, error) {
	value, err := scanSchedule(s.pool.QueryRow(ctx, `
        UPDATE collection_schedule
        SET job_type = $2,
            cron_expression = $3,
            timezone = $4,
            parameters = $5,
            next_run_at = CASE WHEN enabled THEN $6::timestamptz ELSE NULL::timestamptz END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        RETURNING `+scheduleColumns,
		id, req.JobType, req.CronExpression, req.Timezone, normalizeJSON(req.Parameters), nextRun))
	return value, mapError(err)
}

// SetScheduleEnabled는 enabled와 next_run_at을 원자적으로 함께 변경한다.
func (s *Store) SetScheduleEnabled(ctx context.Context, id int64, enabled bool, nextRun *time.Time) (domain.Schedule, error) {
	value, err := scanSchedule(s.pool.QueryRow(ctx, `
        UPDATE collection_schedule
        SET enabled = $2,
            next_run_at = $3::timestamptz,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        RETURNING `+scheduleColumns, id, enabled, nextRun))
	return value, mapError(err)
}

// ListDueSchedules는 현재시각까지 실행 예정인 활성 Schedule 후보를 가져온다.
// DeleteSchedule은 실수 삭제를 막기 위해 비활성 schedule만 제거한다.
func (s *Store) DeleteSchedule(ctx context.Context, id int64) error {
	command, err := s.pool.Exec(ctx, `
        DELETE FROM collection_schedule
        WHERE id = $1
          AND enabled = FALSE`, id)
	if err != nil {
		return err
	}
	if command.RowsAffected() > 0 {
		return nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `
        SELECT EXISTS(SELECT 1 FROM collection_schedule WHERE id = $1)`, id).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return store.ErrNotFound
	}
	return store.ErrInvalidState
}

func (s *Store) ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]domain.Schedule, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+scheduleColumns+`
        FROM collection_schedule
        WHERE enabled = TRUE AND next_run_at <= $1
        ORDER BY next_run_at, id
        LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.Schedule, 0)
	for rows.Next() {
		value, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// TriggerSchedule은 Schedule 행을 잠근 뒤 Job·Item 생성과 다음 실행시각 갱신을 한 트랜잭션으로 처리한다.
func (s *Store) TriggerSchedule(
	ctx context.Context,
	schedule domain.Schedule,
	source string,
	itemKeys []string,
	scheduledFor time.Time,
	nextRun time.Time,
) (domain.Job, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx)

	var enabled bool
	var currentNext *time.Time
	err = tx.QueryRow(ctx, `
        SELECT enabled, next_run_at
        FROM collection_schedule
        WHERE id = $1
        FOR UPDATE`, schedule.ID).Scan(&enabled, &currentNext)
	if err != nil {
		return domain.Job{}, mapError(err)
	}
	if !enabled || currentNext == nil || !currentNext.Equal(scheduledFor) {
		return domain.Job{}, store.ErrConflict
	}

	idempotencyKey := fmt.Sprintf("schedule:%d:%s", schedule.ID, scheduledFor.UTC().Format(time.RFC3339Nano))
	job, err := insertJob(ctx, tx, domain.CreateJobRequest{
		JobType:        schedule.JobType,
		Source:         source,
		Priority:       0,
		ScheduledAt:    &scheduledFor,
		RequestedBy:    "SCHEDULER",
		IdempotencyKey: idempotencyKey,
		Parameters:     schedule.Parameters,
		ItemKeys:       itemKeys,
	}, &schedule.ID, 0)
	if err != nil {
		return domain.Job{}, mapError(err)
	}

	_, err = tx.Exec(ctx, `
        UPDATE collection_schedule
        SET last_run_at = $2::timestamptz, next_run_at = $3::timestamptz, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1`, schedule.ID, scheduledFor, nextRun)
	if err != nil {
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// scanSchedule은 pgx Row와 Rows가 공통으로 사용하는 Schedule 매핑 함수다.
func scanSchedule(row rowScanner) (domain.Schedule, error) {
	var value domain.Schedule
	err := row.Scan(
		&value.ID,
		&value.JobType,
		&value.CronExpression,
		&value.Timezone,
		&value.Enabled,
		&value.Parameters,
		&value.LastRunAt,
		&value.NextRunAt,
		&value.CreatedAt,
		&value.UpdatedAt,
	)
	return value, err
}

// normalizeJSON은 nil RawMessage를 PostgreSQL JSONB 빈 객체로 변환한다.
func normalizeJSON(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage(`{}`)
	}
	return value
}
