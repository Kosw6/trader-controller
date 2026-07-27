package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
)

// Relay는 DB outbox에서 발행 대상을 선점하고 Kafka로 전달한 뒤 상태를 갱신한다.
// controller API가 job을 만들고, relay가 Kafka로 밀어내는 구조라 Kafka 장애에도 job 생성은 보존된다.
type Relay struct {
	repository *Repository
	publisher  Publisher
	cfg        Config
	logger     *slog.Logger
}

// NewRelay는 Repository와 Publisher를 조립해 실행 가능한 relay를 만든다.
func NewRelay(repository *Repository, publisher Publisher, cfg Config, logger *slog.Logger) *Relay {
	return &Relay{
		repository: repository,
		publisher:  publisher,
		cfg:        cfg,
		logger:     logger,
	}
}

// Run은 주기적으로 Kafka health를 확인하고, 발행 가능한 outbox row를 처리한다.
// OUTBOX_RUN_ONCE=true이면 테스트나 수동 점검용으로 한 번만 처리하고 종료한다.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	defer r.publisher.Close()

	for {
		if err := r.runOnce(ctx); err != nil {
			r.logger.Error("outbox relay iteration failed", "error", err)
		}
		if r.cfg.RunOnce {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// runOnce는 stale row 복구, Kafka health 확인, claim, publish를 한 사이클 수행한다.
func (r *Relay) runOnce(ctx context.Context) error {
	recovered, err := r.repository.RecoverStalePublishing(ctx, r.cfg.StaleAfter)
	if err != nil {
		return err
	}
	if recovered > 0 {
		r.logger.Warn("stale publishing outbox events recovered", "count", recovered)
	}

	if err := r.publisher.Health(ctx); err != nil {
		r.logger.Warn("kafka is not healthy; outbox rows remain pending", "error", err)
		return nil
	}

	events, err := r.repository.ClaimPublishable(ctx, r.cfg.BatchSize)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		r.logger.Debug("no publishable outbox events")
		return nil
	}

	for _, event := range events {
		if err := r.publishOne(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// publishOne은 outbox event 하나를 Kafka에 보내고 성공/실패 상태를 DB에 반영한다.
// 실패해도 DB row는 사라지지 않고 RETRY_WAIT 또는 DEAD가 되므로 운영자가 관리자 페이지에서 추적할 수 있다.
func (r *Relay) publishOne(ctx context.Context, event domain.OutboxEvent) error {
	result, err := r.publisher.Publish(ctx, event)
	if err == nil {
		if markErr := r.repository.MarkPublished(ctx, event.ID, result.Partition, result.Offset); markErr != nil {
			return markErr
		}
		r.logger.Info(
			"outbox event published",
			"outboxId", event.ID,
			"jobItemId", event.JobItemID,
			"idempotencyKey", event.IdempotencyKey,
			"partition", result.Partition,
			"offset", result.Offset,
		)
		return nil
	}

	nextAttemptAt := r.nextRetryAt(event.AttemptCount)
	if markErr := r.repository.MarkPublishFailed(
		ctx,
		event,
		err.Error(),
		nextAttemptAt,
		r.cfg.MaxAttempts,
	); markErr != nil {
		return markErr
	}
	r.logger.Warn(
		"outbox event publish failed",
		"outboxId", event.ID,
		"jobItemId", event.JobItemID,
		"idempotencyKey", event.IdempotencyKey,
		"attempt", event.AttemptCount,
		"maxAttempts", r.cfg.MaxAttempts,
		"nextAttemptAt", nextAttemptAt,
		"error", err,
	)
	return nil
}

// nextRetryAt은 지수 백오프로 다음 재시도 시각을 계산한다.
// attempt_count가 늘수록 대기 시간이 길어져 Kafka 장애 중 DB와 Kafka를 과도하게 두드리지 않는다.
func (r *Relay) nextRetryAt(attempt int) time.Time {
	delay := r.cfg.RetryBaseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= r.cfg.RetryMaxDelay {
			delay = r.cfg.RetryMaxDelay
			break
		}
	}
	return time.Now().UTC().Add(delay)
}
