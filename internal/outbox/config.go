package outbox

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const DefaultTopic = "trader.jobs.events"

// Config는 outbox relay 실행에 필요한 환경 설정을 한 곳에 모아 둔다.
// controller API와 별도 프로세스로 띄우기 때문에 Kafka, polling, retry 설정을 분리해서 관리한다.
type Config struct {
	DatabaseURL     string
	KafkaBrokers    []string
	Topic           string
	BatchSize       int
	PollInterval    time.Duration
	PublishTimeout  time.Duration
	HealthTimeout   time.Duration
	StaleAfter      time.Duration
	MaxAttempts     int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
	DBMaxConns      int32
	DBMinConns      int32
	RunOnce         bool
	ShutdownTimeout time.Duration
	AllowAutoTopic  bool
}

// LoadConfig는 환경 변수에서 relay 설정을 읽고 기본값과 필수값을 검증한다.
// Kafka가 잠시 내려가도 DB outbox에는 계속 쌓을 수 있으므로, relay는 Kafka 접속 정보를 별도로 받는다.
func LoadConfig() (Config, error) {
	cfg := Config{
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		KafkaBrokers:    splitCSV(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		Topic:           envOrDefault("KAFKA_TOPIC", DefaultTopic),
		BatchSize:       intEnv("OUTBOX_BATCH_SIZE", 20),
		PollInterval:    durationEnv("OUTBOX_POLL_INTERVAL", 5*time.Second),
		PublishTimeout:  durationEnv("OUTBOX_PUBLISH_TIMEOUT", 10*time.Second),
		HealthTimeout:   durationEnv("OUTBOX_HEALTH_TIMEOUT", 3*time.Second),
		StaleAfter:      durationEnv("OUTBOX_STALE_AFTER", 5*time.Minute),
		MaxAttempts:     intEnv("OUTBOX_MAX_ATTEMPTS", 5),
		RetryBaseDelay:  durationEnv("OUTBOX_RETRY_BASE_DELAY", 10*time.Second),
		RetryMaxDelay:   durationEnv("OUTBOX_RETRY_MAX_DELAY", 5*time.Minute),
		DBMaxConns:      int32(intEnv("DB_MAX_CONNS", 5)),
		DBMinConns:      int32(intEnv("DB_MIN_CONNS", 1)),
		RunOnce:         boolEnv("OUTBOX_RUN_ONCE", false),
		ShutdownTimeout: durationEnv("SHUTDOWN_TIMEOUT", 10*time.Second),
		AllowAutoTopic:  boolEnv("KAFKA_ALLOW_AUTO_TOPIC_CREATION", false),
	}

	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if len(cfg.KafkaBrokers) == 0 {
		missing = append(missing, "KAFKA_BROKERS")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %v", missing)
	}
	if cfg.BatchSize <= 0 {
		return Config{}, errors.New("OUTBOX_BATCH_SIZE must be greater than zero")
	}
	if cfg.MaxAttempts <= 0 {
		return Config{}, errors.New("OUTBOX_MAX_ATTEMPTS must be greater than zero")
	}
	if cfg.RetryBaseDelay <= 0 || cfg.RetryMaxDelay < cfg.RetryBaseDelay {
		return Config{}, errors.New("retry delay settings are invalid")
	}
	if cfg.DBMinConns < 0 || cfg.DBMaxConns < cfg.DBMinConns {
		return Config{}, errors.New("DB connection limits are invalid")
	}
	return cfg, nil
}

// envOrDefault는 환경 변수가 비어 있을 때 지정한 기본값을 사용한다.
func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// boolEnv는 "true", "false" 형식의 환경 변수를 bool 값으로 바꾼다.
func boolEnv(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// intEnv는 환경 변수를 정수로 읽고, 비어 있거나 잘못된 값이면 기본값을 사용한다.
func intEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// durationEnv는 "10s", "1m" 같은 Go duration 문자열을 time.Duration으로 읽는다.
func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// splitCSV는 KAFKA_BROKERS처럼 쉼표로 들어온 주소 목록을 공백 제거 후 슬라이스로 만든다.
func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
