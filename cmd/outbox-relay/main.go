package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kosw006/trader-controller/internal/outbox"
)

// main은 pipeline_outbox에 쌓인 이벤트를 Kafka로 전달하는 별도 프로세스를 실행한다.
// controller API와 분리되어 있으므로 Kafka 장애가 API 서버의 job 생성까지 막지 않는다.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := outbox.LoadConfig()
	if err != nil {
		logger.Error("outbox relay configuration failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := newPool(ctx, cfg)
	if err != nil {
		logger.Error("database pool creation failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repository := outbox.NewRepository(pool)
	publisher := outbox.NewKafkaPublisher(
		cfg.KafkaBrokers,
		cfg.Topic,
		cfg.PublishTimeout,
		cfg.HealthTimeout,
		cfg.AllowAutoTopic,
	)
	relay := outbox.NewRelay(repository, publisher, cfg, logger)

	logger.Info(
		"outbox relay started",
		"topic", cfg.Topic,
		"brokers", cfg.KafkaBrokers,
		"batchSize", cfg.BatchSize,
		"runOnce", cfg.RunOnce,
	)
	if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("outbox relay stopped with error", "error", err)
		os.Exit(1)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	select {
	case <-shutdownCtx.Done():
	default:
	}
	logger.Info("outbox relay stopped")
}

// newPool은 DATABASE_URL로 PostgreSQL connection pool을 만들고 ping까지 확인한다.
// relay는 짧은 쿼리를 반복하므로 API 서버보다 작은 pool 기본값을 사용한다.
func newPool(ctx context.Context, cfg outbox.Config) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = cfg.DBMaxConns
	poolConfig.MinConns = cfg.DBMinConns
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
