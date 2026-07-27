package worker

import (
	"context"
	"log/slog"
	"time"
)

// NewLagScalePlannerTask compares Kafka lag and worker heartbeat snapshots with
// the DB policy, then creates scale-out or scale-in commands for an actuator.
func NewLagScalePlannerTask(
	store LagScalePlanner,
	interval time.Duration,
	batchSize int,
	logger *slog.Logger,
) func(context.Context) error {
	return func(ctx context.Context) error {
		return repeat(ctx, interval, func(iterationCtx context.Context) error {
			scaleOutCreated, err := store.CreateLagTriggeredScaleCommands(iterationCtx, batchSize)
			if err != nil {
				return err
			}
			if scaleOutCreated > 0 {
				logger.Info("lag-triggered scale commands created", "created", scaleOutCreated)
			}

			scaleInCreated, err := store.CreateIdleScaleInCommands(iterationCtx, batchSize)
			if err != nil {
				return err
			}
			if scaleInCreated > 0 {
				logger.Info("idle-triggered scale-in commands created", "created", scaleInCreated)
			}
			return nil
		})
	}
}

// NewLagScalePlannerRunOnceTask runs the same scale decision once from the
// operator-facing control page.
func NewLagScalePlannerRunOnceTask(
	store LagScalePlanner,
	batchSize int,
	logger *slog.Logger,
) func(context.Context) error {
	return func(ctx context.Context) error {
		scaleOutCreated, err := store.CreateLagTriggeredScaleCommands(ctx, batchSize)
		if err != nil {
			return err
		}
		logger.Info("lag-triggered scale commands created once", "created", scaleOutCreated)

		scaleInCreated, err := store.CreateIdleScaleInCommands(ctx, batchSize)
		if err != nil {
			return err
		}
		logger.Info("idle-triggered scale-in commands created once", "created", scaleInCreated)
		return nil
	}
}
