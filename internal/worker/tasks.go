package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/segmentio/kafka-go"
)

type JobStatusAggregator interface {
	AggregateJobStatuses(context.Context, int) (int64, error)
}

type LagScalePlanner interface {
	CreateLagTriggeredScaleCommands(context.Context, int) (int64, error)
	CreateIdleScaleInCommands(context.Context, int) (int64, error)
}

type KafkaLagRecorder interface {
	RecordKafkaLagSnapshot(context.Context, domain.KafkaLagSnapshot) error
}

type KafkaMonitorConfig struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	Interval      time.Duration
	HealthTimeout time.Duration
	LagThreshold  int64
	Recorder      KafkaLagRecorder
}

// NewKafkaHealthTaskForTargets는 여러 Kafka topic을 같은 worker 주기로 점검한다.
// topic별 장애를 모두 로그로 남기되, 한 topic 실패가 나머지 topic 점검을 막지 않게 한다.
func NewKafkaHealthTaskForTargets(configs []KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		interval := monitorInterval(configs)
		return repeat(ctx, interval, func(iterationCtx context.Context) error {
			for _, cfg := range configs {
				partitionCount, err := checkKafkaHealth(iterationCtx, cfg)
				if err != nil {
					logger.Error("kafka health check failed", "topic", cfg.Topic, "error", err)
					continue
				}
				logger.Info("kafka health check succeeded", "topic", cfg.Topic, "partitions", partitionCount)
			}
			return nil
		})
	}
}

// NewKafkaHealthRunOnceTaskForTargets는 여러 topic health를 관리자 once 요청에서 한 번만 점검한다.
func NewKafkaHealthRunOnceTaskForTargets(configs []KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, cfg := range configs {
			partitionCount, err := checkKafkaHealth(ctx, cfg)
			if err != nil {
				logger.Error("kafka health check failed once", "topic", cfg.Topic, "error", err)
				continue
			}
			logger.Info("kafka health check succeeded once", "topic", cfg.Topic, "partitions", partitionCount)
		}
		return nil
	}
}

// NewJobStatusAggregatorTask는 pipeline_job_item 상태를 부모 pipeline_job에 반영하는 반복 worker를 만든다.
// Python worker가 item만 SUCCESS/FAILED로 바꿔도 이 worker가 부모 Job을 SUCCESS/PARTIAL_SUCCESS 등으로 정리한다.
func NewJobStatusAggregatorTask(
	store JobStatusAggregator,
	interval time.Duration,
	batchSize int,
	logger *slog.Logger,
) func(context.Context) error {
	return func(ctx context.Context) error {
		return repeat(ctx, interval, func(iterationCtx context.Context) error {
			updated, err := store.AggregateJobStatuses(iterationCtx, batchSize)
			if err != nil {
				return err
			}
			if updated > 0 {
				logger.Info("pipeline job statuses aggregated", "updated", updated)
			}
			return nil
		})
	}
}

// NewJobStatusAggregatorRunOnceTask는 관리자 once 버튼을 눌렀을 때 집계를 한 번만 수행한다.
func NewJobStatusAggregatorRunOnceTask(
	store JobStatusAggregator,
	batchSize int,
	logger *slog.Logger,
) func(context.Context) error {
	return func(ctx context.Context) error {
		updated, err := store.AggregateJobStatuses(ctx, batchSize)
		if err != nil {
			return err
		}
		logger.Info("pipeline job statuses aggregated once", "updated", updated)
		return nil
	}
}

// NewKafkaHealthTask는 broker와 topic metadata를 주기적으로 확인하는 worker를 만든다.
// 이 worker가 ERROR가 되면 outbox relay 실패 원인이 Kafka 계층인지 빠르게 구분할 수 있다.
func NewKafkaHealthTask(cfg KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		return repeat(ctx, cfg.Interval, func(iterationCtx context.Context) error {
			partitionCount, err := checkKafkaHealth(iterationCtx, cfg)
			if err != nil {
				return err
			}
			logger.Info("kafka health check succeeded", "topic", cfg.Topic, "partitions", partitionCount)
			return nil
		})
	}
}

// NewKafkaHealthRunOnceTask는 Kafka health를 한 번만 확인한다.
func NewKafkaHealthRunOnceTask(cfg KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		partitionCount, err := checkKafkaHealth(ctx, cfg)
		if err != nil {
			return err
		}
		logger.Info("kafka health check succeeded once", "topic", cfg.Topic, "partitions", partitionCount)
		return nil
	}
}

// NewConsumerLagTask는 consumer group committed offset과 topic latest offset 차이를 주기적으로 계산한다.
// lag가 임계값을 넘으면 ERROR로 종료되며, manager가 last_error에 원인을 남긴 뒤 desired_state에 따라 재시작한다.
func NewConsumerLagTask(cfg KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		return repeat(ctx, cfg.Interval, func(iterationCtx context.Context) error {
			measurement, err := measureConsumerLag(iterationCtx, cfg)
			if err != nil {
				return err
			}
			if err := recordLagSnapshot(iterationCtx, cfg, measurement); err != nil {
				return err
			}
			logger.Info("consumer lag measured", "topic", cfg.Topic, "group", cfg.ConsumerGroup, "lag", measurement.TotalLag)
			if measurement.TotalLag > cfg.LagThreshold {
				return fmt.Errorf("consumer lag %d exceeded threshold %d", measurement.TotalLag, cfg.LagThreshold)
			}
			return nil
		})
	}
}

// NewConsumerLagTaskForTargets는 topic/group별 lag을 각각 측정해 snapshot으로 저장한다.
// raw topic이 늘어나도 같은 worker 하나에서 topic별 지연 상태를 볼 수 있게 하기 위한 함수다.
func NewConsumerLagTaskForTargets(configs []KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		interval := monitorInterval(configs)
		return repeat(ctx, interval, func(iterationCtx context.Context) error {
			for _, cfg := range configs {
				measurement, err := measureConsumerLag(iterationCtx, cfg)
				if err != nil {
					logger.Error(
						"consumer lag measurement failed",
						"topic", cfg.Topic,
						"group", cfg.ConsumerGroup,
						"error", err,
					)
					continue
				}
				if err := recordLagSnapshot(iterationCtx, cfg, measurement); err != nil {
					logger.Error(
						"consumer lag snapshot record failed",
						"topic", cfg.Topic,
						"group", cfg.ConsumerGroup,
						"error", err,
					)
					continue
				}
				logger.Info("consumer lag measured", "topic", cfg.Topic, "group", cfg.ConsumerGroup, "lag", measurement.TotalLag)
			}
			return nil
		})
	}
}

// NewConsumerLagRunOnceTask는 lag를 한 번만 측정한다.
func NewConsumerLagRunOnceTask(cfg KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		measurement, err := measureConsumerLag(ctx, cfg)
		if err != nil {
			return err
		}
		if err := recordLagSnapshot(ctx, cfg, measurement); err != nil {
			return err
		}
		logger.Info("consumer lag measured once", "topic", cfg.Topic, "group", cfg.ConsumerGroup, "lag", measurement.TotalLag)
		if measurement.TotalLag > cfg.LagThreshold {
			return fmt.Errorf("consumer lag %d exceeded threshold %d", measurement.TotalLag, cfg.LagThreshold)
		}
		return nil
	}
}

// NewConsumerLagRunOnceTaskForTargets는 topic/group별 lag을 한 번씩 측정한다.
func NewConsumerLagRunOnceTaskForTargets(configs []KafkaMonitorConfig, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, cfg := range configs {
			measurement, err := measureConsumerLag(ctx, cfg)
			if err != nil {
				logger.Error(
					"consumer lag measurement failed once",
					"topic", cfg.Topic,
					"group", cfg.ConsumerGroup,
					"error", err,
				)
				continue
			}
			if err := recordLagSnapshot(ctx, cfg, measurement); err != nil {
				logger.Error(
					"consumer lag snapshot record failed once",
					"topic", cfg.Topic,
					"group", cfg.ConsumerGroup,
					"error", err,
				)
				continue
			}
			logger.Info("consumer lag measured once", "topic", cfg.Topic, "group", cfg.ConsumerGroup, "lag", measurement.TotalLag)
		}
		return nil
	}
}

func monitorInterval(configs []KafkaMonitorConfig) time.Duration {
	for _, cfg := range configs {
		if cfg.Interval > 0 {
			return cfg.Interval
		}
	}
	return 5 * time.Second
}

func repeat(ctx context.Context, interval time.Duration, fn func(context.Context) error) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := fn(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func checkKafkaHealth(ctx context.Context, cfg KafkaMonitorConfig) (int, error) {
	if len(cfg.Brokers) == 0 {
		return 0, fmt.Errorf("kafka brokers are empty")
	}
	healthCtx, cancel := context.WithTimeout(ctx, cfg.HealthTimeout)
	defer cancel()

	conn, err := kafka.DialContext(healthCtx, "tcp", cfg.Brokers[0])
	if err != nil {
		return 0, fmt.Errorf("dial kafka broker: %w", err)
	}
	defer conn.Close()

	if deadline, ok := healthCtx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	partitions, err := conn.ReadPartitions(cfg.Topic)
	if err != nil {
		return 0, fmt.Errorf("read kafka topic metadata: %w", err)
	}
	if len(partitions) == 0 {
		return 0, fmt.Errorf("kafka topic %s has no partitions", cfg.Topic)
	}
	return len(partitions), nil
}

type ConsumerLagMeasurement struct {
	TotalLag      int64         `json:"totalLag"`
	PartitionLags map[int]int64 `json:"partitionLags"`
	LatestOffsets map[int]int64 `json:"latestOffsets"`
	CommitOffsets map[int]int64 `json:"commitOffsets"`
}

func calculateConsumerLag(ctx context.Context, cfg KafkaMonitorConfig) (int64, error) {
	measurement, err := measureConsumerLag(ctx, cfg)
	if err != nil {
		return 0, err
	}
	return measurement.TotalLag, nil
}

func measureConsumerLag(ctx context.Context, cfg KafkaMonitorConfig) (ConsumerLagMeasurement, error) {
	if cfg.ConsumerGroup == "" {
		return ConsumerLagMeasurement{}, fmt.Errorf("kafka consumer group is empty")
	}
	partitionLatestOffsets, err := latestOffsetsByPartition(ctx, cfg)
	if err != nil {
		return ConsumerLagMeasurement{}, err
	}
	client := kafka.Client{
		Addr:    kafka.TCP(cfg.Brokers...),
		Timeout: cfg.HealthTimeout,
	}
	committedOffsets, err := client.ConsumerOffsets(ctx, kafka.TopicAndGroup{
		Topic:   cfg.Topic,
		GroupId: cfg.ConsumerGroup,
	})
	if err != nil {
		return ConsumerLagMeasurement{}, fmt.Errorf("fetch consumer offsets: %w", err)
	}

	var totalLag int64
	partitionLags := make(map[int]int64, len(partitionLatestOffsets))
	commitOffsets := make(map[int]int64, len(partitionLatestOffsets))
	for partition, latestOffset := range partitionLatestOffsets {
		committedOffset, ok := committedOffsets[partition]
		if !ok || committedOffset < 0 {
			committedOffset = 0
		}
		commitOffsets[partition] = committedOffset
		lag := latestOffset - committedOffset
		if lag > 0 {
			totalLag += lag
			partitionLags[partition] = lag
		} else {
			partitionLags[partition] = 0
		}
	}
	return ConsumerLagMeasurement{
		TotalLag:      totalLag,
		PartitionLags: partitionLags,
		LatestOffsets: partitionLatestOffsets,
		CommitOffsets: commitOffsets,
	}, nil
}

func recordLagSnapshot(ctx context.Context, cfg KafkaMonitorConfig, measurement ConsumerLagMeasurement) error {
	if cfg.Recorder == nil {
		return nil
	}
	status := "OK"
	if cfg.LagThreshold > 0 && measurement.TotalLag > cfg.LagThreshold {
		status = "CRITICAL"
	} else if cfg.LagThreshold > 0 && measurement.TotalLag >= cfg.LagThreshold*8/10 {
		status = "WARN"
	}
	payload, err := json.Marshal(measurement)
	if err != nil {
		return err
	}
	return cfg.Recorder.RecordKafkaLagSnapshot(ctx, domain.KafkaLagSnapshot{
		Topic:         cfg.Topic,
		ConsumerGroup: cfg.ConsumerGroup,
		TotalLag:      measurement.TotalLag,
		ThresholdLag:  cfg.LagThreshold,
		Status:        status,
		PartitionLags: payload,
		MeasuredAt:    time.Now().UTC(),
	})
}

func latestOffsetsByPartition(ctx context.Context, cfg KafkaMonitorConfig) (map[int]int64, error) {
	healthCtx, cancel := context.WithTimeout(ctx, cfg.HealthTimeout)
	defer cancel()

	conn, err := kafka.DialContext(healthCtx, "tcp", cfg.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial kafka broker: %w", err)
	}
	partitions, err := conn.ReadPartitions(cfg.Topic)
	_ = conn.Close()
	if err != nil {
		return nil, fmt.Errorf("read kafka topic metadata: %w", err)
	}

	result := make(map[int]int64, len(partitions))
	for _, partition := range partitions {
		partitionConn, err := kafka.DialLeader(healthCtx, "tcp", cfg.Brokers[0], cfg.Topic, partition.ID)
		if err != nil {
			return nil, fmt.Errorf("dial kafka leader topic=%s partition=%d: %w", cfg.Topic, partition.ID, err)
		}
		if deadline, ok := healthCtx.Deadline(); ok {
			_ = partitionConn.SetDeadline(deadline)
		}
		lastOffset, err := partitionConn.ReadLastOffset()
		closeErr := partitionConn.Close()
		if err != nil {
			return nil, fmt.Errorf("read last offset topic=%s partition=%d: %w", cfg.Topic, partition.ID, err)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		result[partition.ID] = lastOffset
	}
	return result, nil
}
