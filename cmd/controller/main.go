package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kosw006/trader-controller/internal/app"
	"github.com/kosw006/trader-controller/internal/config"
	"github.com/kosw006/trader-controller/internal/financialmapping"
	"github.com/kosw006/trader-controller/internal/httpapi"
	"github.com/kosw006/trader-controller/internal/outbox"
	"github.com/kosw006/trader-controller/internal/postgres"
	"github.com/kosw006/trader-controller/internal/recoverycoord"
	"github.com/kosw006/trader-controller/internal/scheduler"
	"github.com/kosw006/trader-controller/internal/worker"
)

// main은 설정·DB·API·Scheduler를 조립하고 종료 신호에 맞춰 안전하게 정리한다.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Error("database configuration failed", "error", err)
		os.Exit(1)
	}
	poolConfig.MaxConns = cfg.DBMaxConns
	poolConfig.MinConns = cfg.DBMinConns
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Error("database pool creation failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		logger.Error("database ping failed", "error", err)
		os.Exit(1)
	}

	repository := postgres.New(pool)
	service := app.New(repository)
	var apiOptions []httpapi.Option
	var mappingService *financialmapping.Service
	if cfg.FinancialMappingEnabled {
		mappingService = financialmapping.New(pool)
		apiOptions = append(apiOptions, httpapi.WithFinancialMapping(mappingService))
	}
	if cfg.RecoveryCoordinatorEnabled {
		coordinator, recoveryErr := recoverycoord.NewWithRawTopic(pool, cfg.RecoveryRawTopic)
		if recoveryErr != nil {
			logger.Error("invalid recovery topic configuration")
			os.Exit(1)
		}
		apiOptions = append(apiOptions, httpapi.WithRecoveryCoordinator(coordinator))
		if os.Getenv("NEWS_RECOVERY_ENABLED") == "true" {
			apiOptions = append(apiOptions, httpapi.WithNewsRecovery())
		}
	}
	api := httpapi.New(service, httpapi.AuthConfig{
		AdminAPIKey:       cfg.AdminAPIKey,
		AdminUsername:     cfg.AdminUsername,
		AdminPasswordHash: cfg.AdminPasswordHash,
		SessionTTL:        cfg.AdminSessionTTL,
		CookieSecure:      cfg.AdminCookieSecure,
	}, logger, apiOptions...)
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	if cfg.SchedulerEnabled {
		runner := scheduler.New(service, cfg.SchedulerPollInterval, cfg.SchedulerBatchSize, logger)
		go runner.Run(ctx)
		if mappingService != nil {
			go scheduler.New(mappingService, cfg.SchedulerPollInterval, cfg.SchedulerBatchSize, logger).Run(ctx)
		}
	}

	if cfg.WorkerManagerEnabled {
		outboxCfg, err := outbox.LoadConfig()
		if err != nil {
			logger.Error("managed worker configuration failed", "error", err)
			os.Exit(1)
		}
		kafkaMonitorTargets := kafkaMonitorConfigs(outboxCfg, cfg, repository)
		manager := worker.NewManager(
			repository,
			worker.ForHostingMode([]worker.Definition{
				{
					Name:         "outbox-relay",
					DisplayName:  "Kafka Outbox Relay",
					WorkerType:   "OUTBOX_RELAY",
					DefaultState: "DISABLED",
					Run: func(workerCtx context.Context) error {
						return runOutboxRelay(workerCtx, pool, outboxCfg, false, logger)
					},
					RunOnce: func(workerCtx context.Context) error {
						return runOutboxRelay(workerCtx, pool, outboxCfg, true, logger)
					},
				},
				{
					Name:         "kafka-health",
					DisplayName:  "Kafka Health Checker",
					WorkerType:   "KAFKA_HEALTH",
					DefaultState: "DISABLED",
					Run:          worker.NewKafkaHealthTaskForTargets(kafkaMonitorTargets, logger),
					RunOnce:      worker.NewKafkaHealthRunOnceTaskForTargets(kafkaMonitorTargets, logger),
				},
				{
					Name:         "job-status-aggregator",
					DisplayName:  "Job Status Aggregator",
					WorkerType:   "JOB_STATUS_AGGREGATOR",
					DefaultState: "DISABLED",
					Run:          worker.NewJobStatusAggregatorTask(repository, cfg.WorkerPollInterval, cfg.WorkerJobBatchSize, logger),
					RunOnce:      worker.NewJobStatusAggregatorRunOnceTask(repository, cfg.WorkerJobBatchSize, logger),
				},
				{
					Name:         "consumer-lag-monitor",
					DisplayName:  "Consumer Lag Monitor",
					WorkerType:   "CONSUMER_LAG_MONITOR",
					DefaultState: "DISABLED",
					Run:          worker.NewConsumerLagTaskForTargets(kafkaMonitorTargets, logger),
					RunOnce:      worker.NewConsumerLagRunOnceTaskForTargets(kafkaMonitorTargets, logger),
				},
				{
					Name:         "worker-scale-planner",
					DisplayName:  "Worker Scale Planner",
					WorkerType:   "WORKER_SCALE_PLANNER",
					DefaultState: "DISABLED",
					Run:          worker.NewLagScalePlannerTask(repository, cfg.WorkerPollInterval, cfg.WorkerJobBatchSize, logger),
					RunOnce:      worker.NewLagScalePlannerRunOnceTask(repository, cfg.WorkerJobBatchSize, logger),
				},
				{
					Name:         "local-docker-actuator",
					DisplayName:  "Local Docker Worker Actuator",
					WorkerType:   "LOCAL_DOCKER_ACTUATOR",
					DefaultState: "DISABLED",
					Run:          worker.NewLocalDockerActuatorTask(repository, cfg.WorkerPollInterval, logger),
					RunOnce:      worker.NewLocalDockerActuatorRunOnceTask(repository, logger),
				},
				{
					Name:         "aws-asg-actuator",
					DisplayName:  "AWS ASG Worker Actuator",
					WorkerType:   "AWS_ASG_ACTUATOR",
					DefaultState: "DISABLED",
					Run:          worker.NewAWSASGActuatorTask(repository, cfg.WorkerPollInterval, logger),
					RunOnce:      worker.NewAWSASGActuatorRunOnceTask(repository, logger),
				},
			}, cfg.SharedWorkerHost),
			cfg.WorkerPollInterval,
			cfg.WorkerLeaseTTL,
			logger,
		)
		go manager.Run(ctx)
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("controller API started", "address", cfg.HTTPAddr, "schedulerEnabled", cfg.SchedulerEnabled)
		serverErrors <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
			stop()
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown failed", "error", err)
	}
	logger.Info("controller stopped")
}

func kafkaMonitorConfigs(outboxCfg outbox.Config, cfg config.Config, recorder worker.KafkaLagRecorder) []worker.KafkaMonitorConfig {
	base := worker.KafkaMonitorConfig{
		Brokers:       outboxCfg.KafkaBrokers,
		Interval:      cfg.WorkerPollInterval,
		HealthTimeout: outboxCfg.HealthTimeout,
		LagThreshold:  cfg.KafkaLagThreshold,
		Recorder:      recorder,
	}
	targets := []struct {
		topicEnv     string
		defaultTopic string
		groupEnv     string
		defaultGroup string
	}{
		{"KAFKA_TOPIC", outboxCfg.Topic, "KAFKA_CONSUMER_GROUP", cfg.KafkaConsumerGroup},
		{"KAFKA_RAW_KIS_TOPIC", "trader.raw.kis.ready", "KAFKA_RAW_KIS_GROUP_ID", "trader-data-kis-etl"},
		{"KAFKA_RAW_BLS_TOPIC", "trader.raw.bls.ready", "KAFKA_RAW_BLS_GROUP_ID", "trader-data-bls-etl"},
		{"KAFKA_RAW_SEC_TOPIC", "trader.raw.sec.ready", "KAFKA_RAW_SEC_GROUP_ID", "trader-data-sec-etl"},
		{"KAFKA_RAW_NEWS_TOPIC", "trader.raw.news.ready", "KAFKA_RAW_NEWS_GROUP_ID", "trader-data-news-etl"},
	}

	result := make([]worker.KafkaMonitorConfig, 0, len(targets))
	for _, target := range targets {
		next := base
		next.Topic = envOrDefault(target.topicEnv, target.defaultTopic)
		next.ConsumerGroup = envOrDefault(target.groupEnv, target.defaultGroup)
		result = append(result, next)
	}
	return result
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// runOutboxRelay는 controller 내부 managed worker에서 outbox relay를 실행한다.
// 별도 cmd/outbox-relay 프로세스와 같은 Repository/Publisher/Relay를 재사용해서 동작 차이를 줄인다.
func runOutboxRelay(ctx context.Context, pool *pgxpool.Pool, cfg outbox.Config, runOnce bool, logger *slog.Logger) error {
	workerCfg := cfg
	workerCfg.RunOnce = runOnce
	repository := outbox.NewRepository(pool)
	publisher := outbox.NewKafkaPublisher(
		workerCfg.KafkaBrokers,
		workerCfg.Topic,
		workerCfg.PublishTimeout,
		workerCfg.HealthTimeout,
		workerCfg.AllowAutoTopic,
	)
	relay := outbox.NewRelay(repository, publisher, workerCfg, logger)
	return relay.Run(ctx)
}
