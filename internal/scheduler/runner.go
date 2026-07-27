package scheduler

import (
	"context"
	"log/slog"
	"time"
)

type DueScheduleRunner interface {
	RunDueSchedules(ctx context.Context, limit int) (int, error)
}

type Runner struct {
	service  DueScheduleRunner
	interval time.Duration
	limit    int
	logger   *slog.Logger
}

// New는 일정한 주기로 Control Plane tick을 실행할 Scheduler Runner를 만든다.
func New(service DueScheduleRunner, interval time.Duration, limit int, logger *slog.Logger) *Runner {
	return &Runner{service: service, interval: interval, limit: limit, logger: logger}
}

// Run은 즉시 한 번 실행한 뒤 context가 취소될 때까지 주기적으로 poll한다.
func (r *Runner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("scheduler stopped")
			return
		case <-ticker.C:
			r.poll(ctx)
		}
	}
}

// poll은 예약 Job 활성화, due Schedule 실행, Job 상태 집계를 한 번 수행한다.
func (r *Runner) poll(ctx context.Context) {
	triggered, err := r.service.RunDueSchedules(ctx, r.limit)
	if err != nil {
		r.logger.Error("schedule polling completed with errors", "error", err, "triggered", triggered)
		return
	}
	if triggered > 0 {
		r.logger.Info("controller tick applied", "activatedOrTriggered", triggered)
	}
}
