package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

const jobColumns = `
    id, schedule_id, job_type, source, status, priority, scheduled_at,
    started_at, completed_at, requested_by, idempotency_key, retry_count,
    parameters, error_code, error_message, created_at, updated_at`

const itemColumns = `
    id, job_id, item_key, status, attempt_count, checkpoint,
    error_code, error_message, started_at, completed_at`

// jobsEventsTopic must match the relay/consumer topic, including isolated queues.
func jobsEventsTopic() string {
	if topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC")); topic != "" {
		return topic
	}
	return "trader.jobs.events"
}

const rawKISReadyTopic = "trader.raw.kis.ready"
const rawBLSReadyTopic = "trader.raw.bls.ready"
const rawSECReadyTopic = "trader.raw.sec.ready"
const rawNewsReadyTopic = "trader.raw.news.ready"

const jobItemQueuedEventType = "JOB_ITEM_QUEUED"
const rawObjectReadyEventType = "RAW_OBJECT_READY"

// CreateJob은 Job과 모든 Item이 함께 성공하거나 롤백되도록 한 트랜잭션에서 생성한다.
func (s *Store) CreateJob(ctx context.Context, req domain.CreateJobRequest) (domain.JobDetail, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.JobDetail{}, err
	}
	defer tx.Rollback(ctx)

	job, err := insertJob(ctx, tx, req, nil, 0)
	if err != nil {
		return domain.JobDetail{}, mapError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.JobDetail{}, err
	}
	return s.GetJob(ctx, job.ID)
}

// ListJobs는 상태와 페이지 조건에 맞는 Job을 최신순으로 조회한다.
func (s *Store) ListJobs(ctx context.Context, filter domain.ListFilter) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+jobColumns+`
        FROM pipeline_job
        WHERE ($1 = '' OR status = $1)
        ORDER BY id DESC
        LIMIT $2 OFFSET $3`, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.Job, 0)
	for rows.Next() {
		value, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// GetJob은 Job 한 건과 관리자 상세에 표시할 하위 Item을 함께 조회한다.
func (s *Store) GetJob(ctx context.Context, id int64) (domain.JobDetail, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `
        SELECT `+jobColumns+`
        FROM pipeline_job
        WHERE id = $1`, id))
	if err != nil {
		return domain.JobDetail{}, mapError(err)
	}
	items, err := s.ListItems(ctx, id, domain.ListFilter{Limit: 200})
	if err != nil {
		return domain.JobDetail{}, err
	}
	return domain.JobDetail{Job: job, Items: items}, nil
}

// ActivateDueJobs는 SKIP LOCKED로 미래 예약 Job을 안전하게 선점해 QUEUED로 전환한다.
func (s *Store) ActivateDueJobs(ctx context.Context, now time.Time, limit int) (int64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
        SELECT id
        FROM pipeline_job
        WHERE status = 'SCHEDULED' AND scheduled_at <= $1
        ORDER BY priority DESC, scheduled_at, id
        LIMIT $2
        FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, tx.Commit(ctx)
	}
	command, err := tx.Exec(ctx, `
        UPDATE pipeline_job
        SET status = 'QUEUED', updated_at = CURRENT_TIMESTAMP
        WHERE id = ANY($1)`, ids)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `
        UPDATE pipeline_job_item
        SET status = 'QUEUED'
        WHERE job_id = ANY($1) AND status = 'SCHEDULED'`, ids)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `
        INSERT INTO pipeline_outbox (
            source_object_id, job_item_id, event_type, topic, message_key,
            idempotency_key, payload, status, attempt_count, next_attempt_at
        )
        SELECT
            NULL,
            i.id,
            'JOB_ITEM_QUEUED',
            $2,
            'job_item:' || i.id::TEXT,
            'JOB_ITEM_QUEUED:' || i.id::TEXT,
            jsonb_build_object(
                'eventId', 'job_item:' || i.id::TEXT,
                'eventType', 'JOB_ITEM_QUEUED',
                'eventVersion', 1,
                'idempotencyKey', 'JOB_ITEM_QUEUED:' || i.id::TEXT,
                'source', j.source,
                'jobType', j.job_type,
                'jobId', j.id,
                'itemId', i.id,
                'itemKey', i.item_key,
                'parameters', j.parameters,
                'createdAt', to_jsonb(CURRENT_TIMESTAMP)
            ),
            'PENDING',
            0,
            CURRENT_TIMESTAMP
        FROM pipeline_job_item i
        JOIN pipeline_job j ON j.id = i.job_id
        WHERE i.job_id = ANY($1)
          AND i.status = 'QUEUED'
        ON CONFLICT (idempotency_key) DO NOTHING`, ids, jobsEventsTopic())
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return command.RowsAffected(), nil
}

// AggregateJobStatuses는 Item 상태 집계로 Job의 RUNNING·SUCCESS·FAILED 상태를 결정한다.
func (s *Store) AggregateJobStatuses(ctx context.Context, limit int) (int64, error) {
	command, err := s.pool.Exec(ctx, `
        WITH candidates AS (
            SELECT id
            FROM pipeline_job
            WHERE status IN ('QUEUED', 'RUNNING', 'CANCEL_REQUESTED')
            ORDER BY updated_at, id
            LIMIT $1
        ), item_counts AS (
            SELECT
                j.id,
                j.status AS current_status,
                COUNT(i.id) AS total_count,
                COUNT(*) FILTER (WHERE i.status IN ('COLLECTED', 'SUCCESS', 'SKIPPED')) AS success_count,
                COUNT(*) FILTER (WHERE i.status IN ('FAILED', 'CANCELLED')) AS failure_count,
                COUNT(*) FILTER (WHERE i.status = 'RUNNING') AS running_count,
                COUNT(*) FILTER (WHERE i.status IN ('QUEUED', 'SCHEDULED', 'RETRY_WAIT')) AS pending_count
            FROM pipeline_job j
            JOIN candidates c ON c.id = j.id
            LEFT JOIN pipeline_job_item i ON i.job_id = j.id
            GROUP BY j.id, j.status
        ), resolved AS (
            SELECT
                id,
                CASE
                    WHEN current_status = 'CANCEL_REQUESTED' AND running_count = 0 THEN 'CANCELLED'
                    WHEN total_count > 0 AND success_count = total_count THEN 'SUCCESS'
                    WHEN pending_count = 0 AND running_count = 0 AND failure_count = total_count THEN 'FAILED'
                    WHEN pending_count = 0 AND running_count = 0 AND failure_count > 0 THEN 'PARTIAL_SUCCESS'
                    WHEN running_count > 0 THEN 'RUNNING'
                    ELSE current_status
                END AS next_status
            FROM item_counts
        )
        UPDATE pipeline_job j
        SET status = r.next_status,
            started_at = CASE WHEN r.next_status = 'RUNNING' THEN COALESCE(j.started_at, CURRENT_TIMESTAMP) ELSE j.started_at END,
            completed_at = CASE WHEN r.next_status IN ('SUCCESS', 'FAILED', 'PARTIAL_SUCCESS', 'CANCELLED') THEN COALESCE(j.completed_at, CURRENT_TIMESTAMP) ELSE NULL END,
            updated_at = CURRENT_TIMESTAMP
        FROM resolved r
        WHERE j.id = r.id AND j.status IS DISTINCT FROM r.next_status`, limit)
	if err != nil {
		return 0, err
	}
	return command.RowsAffected(), nil
}

// RetryJob은 원본 Job을 잠그고 실패·취소 Item만 복사한 새 Job을 만든다.
func (s *Store) RetryJob(ctx context.Context, id int64, requestedBy, idempotencyKey string) (domain.JobDetail, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.JobDetail{}, err
	}
	defer tx.Rollback(ctx)

	original, err := scanJob(tx.QueryRow(ctx, `
        SELECT `+jobColumns+`
        FROM pipeline_job
        WHERE id = $1
        FOR UPDATE`, id))
	if err != nil {
		return domain.JobDetail{}, mapError(err)
	}
	if original.Status != "FAILED" && original.Status != "PARTIAL_SUCCESS" && original.Status != "CANCELLED" {
		return domain.JobDetail{}, store.ErrInvalidState
	}

	rows, err := tx.Query(ctx, `
        SELECT item_key
        FROM pipeline_job_item
        WHERE job_id = $1 AND status IN ('FAILED', 'RETRY_WAIT', 'CANCELLED')
        ORDER BY id`, id)
	if err != nil {
		return domain.JobDetail{}, err
	}
	var itemKeys []string
	for rows.Next() {
		var itemKey string
		if err := rows.Scan(&itemKey); err != nil {
			rows.Close()
			return domain.JobDetail{}, err
		}
		itemKeys = append(itemKeys, itemKey)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return domain.JobDetail{}, err
	}
	if len(itemKeys) == 0 {
		return domain.JobDetail{}, store.ErrInvalidState
	}

	now := time.Now().UTC()
	job, err := insertJob(ctx, tx, domain.CreateJobRequest{
		JobType:        original.JobType,
		Source:         original.Source,
		Priority:       original.Priority,
		ScheduledAt:    &now,
		RequestedBy:    requestedBy,
		IdempotencyKey: idempotencyKey,
		Parameters:     original.Parameters,
		ItemKeys:       itemKeys,
	}, original.ScheduleID, original.RetryCount+1)
	if err != nil {
		return domain.JobDetail{}, mapError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.JobDetail{}, err
	}
	return s.GetJob(ctx, job.ID)
}

// CancelJob은 실행 전 Job은 즉시 취소하고 실행 중 Job은 CANCEL_REQUESTED로 전환한다.
func (s *Store) CancelJob(ctx context.Context, id int64) (domain.Job, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx)

	job, err := scanJob(tx.QueryRow(ctx, `
        UPDATE pipeline_job
        SET status = CASE WHEN status = 'RUNNING' THEN 'CANCEL_REQUESTED' ELSE 'CANCELLED' END,
            completed_at = CASE WHEN status = 'RUNNING' THEN completed_at ELSE CURRENT_TIMESTAMP END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
          AND status IN ('SCHEDULED', 'QUEUED', 'RETRY_WAIT', 'RUNNING')
        RETURNING `+jobColumns, id))
	if err != nil {
		if mapError(err) == store.ErrNotFound {
			return domain.Job{}, store.ErrInvalidState
		}
		return domain.Job{}, err
	}
	_, err = tx.Exec(ctx, `
        UPDATE pipeline_job_item
        SET status = 'CANCELLED', completed_at = CURRENT_TIMESTAMP
        WHERE job_id = $1 AND status IN ('SCHEDULED', 'QUEUED', 'RETRY_WAIT')`, id)
	if err != nil {
		return domain.Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

// ListItems는 선택한 Job 또는 전체 Item을 상태·페이지 조건으로 조회한다.
func (s *Store) ListItems(ctx context.Context, jobID int64, filter domain.ListFilter) ([]domain.JobItem, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+itemColumns+`
        FROM pipeline_job_item
        WHERE ($1 = 0 OR job_id = $1)
          AND ($2 = '' OR status = $2)
        ORDER BY id
        LIMIT $3 OFFSET $4`, jobID, filter.Status, filter.Limit, filter.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.JobItem, 0)
	for rows.Next() {
		value, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// RetryItem은 실패·취소 Item의 오류와 실행시각을 지우고 다시 QUEUED로 만든다.
func (s *Store) RetryItem(ctx context.Context, id int64) (domain.JobItem, error) {
	value, err := scanItem(s.pool.QueryRow(ctx, `
        UPDATE pipeline_job_item
        SET status = 'QUEUED',
            error_code = NULL,
            error_message = NULL,
            started_at = NULL,
            completed_at = NULL
        WHERE id = $1 AND status IN ('FAILED', 'RETRY_WAIT', 'CANCELLED')
        RETURNING `+itemColumns, id))
	if err != nil {
		if mapError(err) == store.ErrNotFound {
			return domain.JobItem{}, store.ErrInvalidState
		}
		return domain.JobItem{}, err
	}
	return value, nil
}

// insertJob은 이미 열린 트랜잭션 안에서 Job과 Item을 생성하는 공통 내부 함수다.
// InsertRawObjectReadyOutbox는 수집기가 raw 파일을 저장하고 source_object를 만든 뒤
// ETL worker에게 "이 원본 객체를 변환해도 된다"는 RAW_OBJECT_READY 이벤트를 예약한다.
// Kafka가 내려가 있어도 이 함수는 DB outbox까지만 기록하므로 raw 수집 결과가 유실되지 않는다.
func (s *Store) InsertRawObjectReadyOutbox(ctx context.Context, sourceObjectID int64) (domain.OutboxEvent, error) {
	var (
		provider       string
		dataType       string
		externalID     *string
		storageBackend string
		storageKey     string
		objectURI      string
		contentHash    string
		collectedAt    time.Time
		itemID         int64
		itemKey        string
		jobID          int64
		jobType        string
		jobSource      string
		parameters     json.RawMessage
	)

	err := s.pool.QueryRow(ctx, `
        SELECT
            so.provider,
            so.data_type,
            so.external_id,
            so.storage_backend,
            so.storage_key,
            so.object_uri,
            so.content_hash,
            so.collected_at,
            i.id,
            i.item_key,
            j.id,
            j.job_type,
            j.source,
            j.parameters
        FROM source_object so
        JOIN pipeline_job_item i ON i.id = so.pipeline_job_item_id
        JOIN pipeline_job j ON j.id = i.job_id
        WHERE so.id = $1`, sourceObjectID).Scan(
		&provider,
		&dataType,
		&externalID,
		&storageBackend,
		&storageKey,
		&objectURI,
		&contentHash,
		&collectedAt,
		&itemID,
		&itemKey,
		&jobID,
		&jobType,
		&jobSource,
		&parameters,
	)
	if err != nil {
		return domain.OutboxEvent{}, mapError(err)
	}

	topic, err := rawObjectReadyTopic(provider)
	if err != nil {
		return domain.OutboxEvent{}, err
	}

	idempotencyKey := fmt.Sprintf("%s:%d", rawObjectReadyEventType, sourceObjectID)
	messageKey := fmt.Sprintf("raw:%d", sourceObjectID)
	payload, err := json.Marshal(map[string]any{
		"eventId":        messageKey,
		"eventType":      rawObjectReadyEventType,
		"eventVersion":   1,
		"idempotencyKey": idempotencyKey,
		"source":         provider,
		"jobSource":      jobSource,
		"jobType":        jobType,
		"jobId":          jobID,
		"itemId":         itemID,
		"itemKey":        itemKey,
		"sourceObjectId": sourceObjectID,
		"dataType":       dataType,
		"externalId":     externalID,
		"storageBackend": storageBackend,
		"storageKey":     storageKey,
		"objectUri":      objectURI,
		"contentHash":    contentHash,
		"collectedAt":    collectedAt.UTC().Format(time.RFC3339Nano),
		"parameters":     json.RawMessage(parameters),
		"createdAt":      time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return domain.OutboxEvent{}, err
	}

	value, err := scanOutbox(s.pool.QueryRow(ctx, `
        INSERT INTO pipeline_outbox (
            source_object_id, job_item_id, event_type, topic, message_key,
            idempotency_key, payload, status, attempt_count, next_attempt_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'PENDING', 0, CURRENT_TIMESTAMP
        )
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING `+outboxColumns,
		sourceObjectID,
		itemID,
		rawObjectReadyEventType,
		topic,
		messageKey,
		idempotencyKey,
		payload,
	))
	if err == nil {
		return value, nil
	}
	if mapError(err) != store.ErrNotFound {
		return domain.OutboxEvent{}, err
	}
	return scanOutbox(s.pool.QueryRow(ctx, `
        SELECT `+outboxColumns+`
        FROM pipeline_outbox
        WHERE idempotency_key = $1`, idempotencyKey))
}

func rawObjectReadyTopic(provider string) (string, error) {
	switch strings.ToUpper(provider) {
	case "KIS":
		return rawKISReadyTopic, nil
	case "BLS":
		return rawBLSReadyTopic, nil
	case "SEC":
		return rawSECReadyTopic, nil
	case "NEWS":
		return rawNewsReadyTopic, nil
	default:
		return "", fmt.Errorf("unsupported raw object provider: %s", provider)
	}
}

func insertJob(
	ctx context.Context,
	tx pgx.Tx,
	req domain.CreateJobRequest,
	scheduleID *int64,
	retryCount int,
) (domain.Job, error) {
	status := "QUEUED"
	if req.ScheduledAt != nil && req.ScheduledAt.After(time.Now()) {
		status = "SCHEDULED"
	}
	job, err := scanJob(tx.QueryRow(ctx, `
        INSERT INTO pipeline_job (
            schedule_id, job_type, source, status, priority, scheduled_at,
            requested_by, idempotency_key, retry_count, parameters
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING `+jobColumns,
		scheduleID, req.JobType, req.Source, status, req.Priority, req.ScheduledAt,
		req.RequestedBy, req.IdempotencyKey, retryCount, normalizeJSON(req.Parameters)))
	if err != nil {
		return domain.Job{}, err
	}

	for _, itemKey := range req.ItemKeys {
		var itemID int64
		err := tx.QueryRow(ctx, `
            INSERT INTO pipeline_job_item (job_id, item_key, status)
            VALUES ($1, $2, $3)
            RETURNING id`, job.ID, itemKey, status).Scan(&itemID)
		if err != nil {
			return domain.Job{}, fmt.Errorf("insert job item %s: %w", itemKey, err)
		}
		if status == "QUEUED" {
			if err := insertJobItemQueuedOutbox(ctx, tx, job, itemID, itemKey); err != nil {
				return domain.Job{}, fmt.Errorf("insert outbox for job item %s: %w", itemKey, err)
			}
		}
	}
	return job, nil
}

// insertJobItemQueuedOutbox는 Worker가 처리할 최소 단위인 Job Item을 Kafka로 전달하기 위해
// pipeline_outbox에 JOB_ITEM_QUEUED 이벤트를 같은 DB 트랜잭션 안에서 저장한다.
// Kafka가 죽어 있어도 이 insert만 성공하면 나중에 relay가 복구 후 재발행할 수 있다.
func insertJobItemQueuedOutbox(ctx context.Context, tx pgx.Tx, job domain.Job, itemID int64, itemKey string) error {
	idempotencyKey := fmt.Sprintf("%s:%d", jobItemQueuedEventType, itemID)
	payload, err := json.Marshal(map[string]any{
		"eventId":        fmt.Sprintf("job_item:%d", itemID),
		"eventType":      jobItemQueuedEventType,
		"eventVersion":   1,
		"idempotencyKey": idempotencyKey,
		"source":         job.Source,
		"jobType":        job.JobType,
		"jobId":          job.ID,
		"itemId":         itemID,
		"itemKey":        itemKey,
		"parameters":     json.RawMessage(job.Parameters),
		"createdAt":      time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
        INSERT INTO pipeline_outbox (
            source_object_id, job_item_id, event_type, topic, message_key,
            idempotency_key, payload, status, attempt_count, next_attempt_at
        ) VALUES (
            NULL, $1, $2, $3, $4, $5, $6, 'PENDING', 0, CURRENT_TIMESTAMP
        )`,
		itemID,
		jobItemQueuedEventType,
		jobsEventsTopic(),
		fmt.Sprintf("job_item:%d", itemID),
		idempotencyKey,
		payload,
	)
	return err
}

// scanJob은 SQL 조회 결과를 Job 도메인 구조체로 매핑한다.
func scanJob(row rowScanner) (domain.Job, error) {
	var value domain.Job
	err := row.Scan(
		&value.ID,
		&value.ScheduleID,
		&value.JobType,
		&value.Source,
		&value.Status,
		&value.Priority,
		&value.ScheduledAt,
		&value.StartedAt,
		&value.CompletedAt,
		&value.RequestedBy,
		&value.IdempotencyKey,
		&value.RetryCount,
		&value.Parameters,
		&value.ErrorCode,
		&value.ErrorMessage,
		&value.CreatedAt,
		&value.UpdatedAt,
	)
	return value, err
}

// scanItem은 SQL 조회 결과를 JobItem 도메인 구조체로 매핑한다.
func scanItem(row rowScanner) (domain.JobItem, error) {
	var value domain.JobItem
	err := row.Scan(
		&value.ID,
		&value.JobID,
		&value.ItemKey,
		&value.Status,
		&value.AttemptCount,
		&value.Checkpoint,
		&value.ErrorCode,
		&value.ErrorMessage,
		&value.StartedAt,
		&value.CompletedAt,
	)
	return value, err
}
