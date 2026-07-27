package outbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kosw006/trader-controller/internal/domain"
)

const outboxColumns = `
    id, source_object_id, job_item_id, event_type, topic, message_key,
    idempotency_key, payload, status, attempt_count, next_attempt_at,
    published_at, last_error, kafka_partition, kafka_offset, created_at, updated_at`

const qualifiedOutboxColumns = `
    o.id, o.source_object_id, o.job_item_id, o.event_type, o.topic, o.message_key,
    o.idempotency_key, o.payload, o.status, o.attempt_count, o.next_attempt_at,
    o.published_at, o.last_error, o.kafka_partition, o.kafka_offset, o.created_at, o.updated_at`

// Repository는 pipeline_outbox 상태 전이를 담당한다.
// Kafka 전송 자체는 Publisher가 맡고, Repository는 DB 원장 상태만 바꾸도록 책임을 분리했다.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository는 pgxpool을 받아 outbox 전용 저장소를 만든다.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// RecoverStalePublishing은 relay가 PUBLISHING으로 잡은 뒤 죽은 row를 다시 재시도 가능 상태로 돌린다.
// 예: relay A가 row 10을 PUBLISHING으로 바꾼 직후 프로세스가 죽으면, row 10은 영원히 멈출 수 있다.
// 이 함수는 updated_at이 오래된 PUBLISHING row를 RETRY_WAIT으로 복구해서 다음 relay가 다시 잡게 한다.
func (r *Repository) RecoverStalePublishing(ctx context.Context, staleAfter time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-staleAfter)
	command, err := r.pool.Exec(ctx, `
        UPDATE pipeline_outbox
        SET status = 'RETRY_WAIT',
            next_attempt_at = CURRENT_TIMESTAMP,
            last_error = 'stale publishing recovered by outbox relay',
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'PUBLISHING'
          AND updated_at < $1`, cutoff)
	if err != nil {
		return 0, err
	}
	return command.RowsAffected(), nil
}

// ClaimPublishable은 발행 가능한 outbox row를 PUBLISHING으로 선점한 뒤 반환한다.
// FOR UPDATE SKIP LOCKED를 사용하므로 relay가 여러 개 떠도 같은 row를 동시에 가져가지 않는다.
func (r *Repository) ClaimPublishable(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	rows, err := r.pool.Query(ctx, `
        WITH candidates AS (
            SELECT id
            FROM pipeline_outbox
            WHERE status IN ('PENDING', 'RETRY_WAIT')
              AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
            ORDER BY created_at, id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pipeline_outbox o
        SET status = 'PUBLISHING',
            attempt_count = o.attempt_count + 1,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        FROM candidates c
        WHERE o.id = c.id
        RETURNING `+qualifiedOutboxColumns, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]domain.OutboxEvent, 0)
	for rows.Next() {
		value, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, value)
	}
	return events, rows.Err()
}

// MarkPublished는 Kafka가 메시지를 받아 offset까지 확정한 뒤 outbox row를 PUBLISHED로 바꾼다.
// partition과 offset을 저장해 두면 운영자가 "이 DB 이벤트가 Kafka 어디까지 갔는지" 추적할 수 있다.
func (r *Repository) MarkPublished(ctx context.Context, id int64, partition int, offset int64) error {
	_, err := r.pool.Exec(ctx, `
        UPDATE pipeline_outbox
        SET status = 'PUBLISHED',
            kafka_partition = $2,
            kafka_offset = $3,
            published_at = CURRENT_TIMESTAMP,
            next_attempt_at = NULL,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
          AND status = 'PUBLISHING'`, id, partition, offset)
	return err
}

// MarkPublishFailed는 Kafka 발행 실패 row를 RETRY_WAIT 또는 DEAD로 전환한다.
// attempt_count는 ClaimPublishable에서 이미 1 증가했으므로, 현재 attempt_count로 최대 시도 횟수를 판단한다.
func (r *Repository) MarkPublishFailed(
	ctx context.Context,
	event domain.OutboxEvent,
	errText string,
	nextAttemptAt time.Time,
	maxAttempts int,
) error {
	status := "RETRY_WAIT"
	var nextAttempt any = nextAttemptAt
	if event.AttemptCount >= maxAttempts {
		status = "DEAD"
		nextAttempt = nil
	}
	_, err := r.pool.Exec(ctx, `
        UPDATE pipeline_outbox
        SET status = $2,
            next_attempt_at = $3,
            last_error = $4,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
          AND status = 'PUBLISHING'`,
		event.ID,
		status,
		nextAttempt,
		truncateError(errText),
	)
	return err
}

// scanOutbox는 SQL 결과를 domain.OutboxEvent 구조체로 매핑한다.
func scanOutbox(row interface {
	Scan(dest ...any) error
}) (domain.OutboxEvent, error) {
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

// truncateError는 DB에 너무 긴 오류 메시지가 반복 저장되는 것을 막는다.
func truncateError(value string) string {
	const maxLen = 2000
	if len(value) <= maxLen {
		return value
	}
	return value[:maxLen]
}
