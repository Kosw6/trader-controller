package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr              string
	DatabaseURL           string
	AdminAPIKey           string
	AdminUsername         string
	AdminPasswordHash     string
	AdminSessionTTL       time.Duration
	AdminCookieSecure     bool
	SchedulerEnabled      bool
	SchedulerPollInterval time.Duration
	SchedulerBatchSize    int
	WorkerManagerEnabled  bool
	WorkerPollInterval    time.Duration
	WorkerLeaseTTL        time.Duration
	WorkerJobBatchSize    int
	KafkaConsumerGroup    string
	KafkaLagThreshold     int64
	DBMaxConns            int32
	DBMinConns            int32
	ShutdownTimeout       time.Duration
}

// Load는 환경변수를 읽고 실행에 필요한 필수값과 범위를 검증한다.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:              envOrDefault("HTTP_ADDR", ":8090"),
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		AdminAPIKey:           os.Getenv("ADMIN_API_KEY"),
		AdminUsername:         envOrDefault("ADMIN_USERNAME", "admin"),
		AdminPasswordHash:     os.Getenv("ADMIN_PASSWORD_HASH"),
		AdminSessionTTL:       durationEnv("ADMIN_SESSION_TTL", 8*time.Hour),
		AdminCookieSecure:     boolEnv("ADMIN_COOKIE_SECURE", false),
		SchedulerEnabled:      boolEnv("SCHEDULER_ENABLED", true),
		SchedulerPollInterval: durationEnv("SCHEDULER_POLL_INTERVAL", 10*time.Second),
		SchedulerBatchSize:    intEnv("SCHEDULER_BATCH_SIZE", 20),
		WorkerManagerEnabled:  boolEnv("WORKER_MANAGER_ENABLED", true),
		WorkerPollInterval:    durationEnv("WORKER_POLL_INTERVAL", 5*time.Second),
		WorkerLeaseTTL:        durationEnv("WORKER_LEASE_TTL", 30*time.Second),
		WorkerJobBatchSize:    intEnv("WORKER_JOB_BATCH_SIZE", 100),
		KafkaConsumerGroup:    envOrDefault("KAFKA_CONSUMER_GROUP", "trader-data-workers"),
		KafkaLagThreshold:     int64(intEnv("KAFKA_LAG_THRESHOLD", 1000)),
		DBMaxConns:            int32(intEnv("DB_MAX_CONNS", 10)),
		DBMinConns:            int32(intEnv("DB_MIN_CONNS", 2)),
		ShutdownTimeout:       durationEnv("SHUTDOWN_TIMEOUT", 10*time.Second),
	}

	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if cfg.AdminAPIKey == "" {
		missing = append(missing, "ADMIN_API_KEY")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %v", missing)
	}
	if cfg.SchedulerBatchSize <= 0 {
		return Config{}, errors.New("SCHEDULER_BATCH_SIZE must be greater than zero")
	}
	if cfg.WorkerPollInterval <= 0 || cfg.WorkerLeaseTTL <= cfg.WorkerPollInterval {
		return Config{}, errors.New("worker manager timing settings are invalid")
	}
	if cfg.WorkerJobBatchSize <= 0 || cfg.KafkaLagThreshold < 0 {
		return Config{}, errors.New("worker batch or lag threshold settings are invalid")
	}
	if cfg.AdminSessionTTL <= 0 {
		return Config{}, errors.New("ADMIN_SESSION_TTL must be positive")
	}
	if cfg.DBMinConns < 0 || cfg.DBMaxConns < cfg.DBMinConns {
		return Config{}, errors.New("DB connection limits are invalid")
	}
	return cfg, nil
}

// envOrDefault는 환경변수가 비어 있을 때 지정한 기본 문자열을 반환한다.
func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// boolEnv는 환경변수를 bool로 변환하며 형식이 잘못되면 기본값을 사용한다.
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

// intEnv는 환경변수를 정수로 변환하며 형식이 잘못되면 기본값을 사용한다.
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

// durationEnv는 10s, 1m 같은 문자열을 time.Duration으로 변환한다.
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
