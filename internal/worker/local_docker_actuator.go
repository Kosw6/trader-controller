package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
)

const localDockerDriver = "LOCAL_DOCKER"

type WorkerScaleCommandStore interface {
	ClaimWorkerScaleCommand(context.Context, string) (domain.WorkerScaleCommandExecution, bool, error)
	CompleteWorkerScaleCommand(context.Context, int64, string, *string) error
}

type localDockerTargetConfig struct {
	ComposeProject string `json:"composeProject"`
	ComposeFile    string `json:"composeFile"`
	ComposeDir     string `json:"composeDir"`
	ServiceName    string `json:"serviceName"`
}

type localDockerCommandDetails struct {
	WorkerName string `json:"workerName"`
	Source     string `json:"source"`
	Topic      string `json:"topic"`
}

// NewLocalDockerActuatorTask는 DB scale command를 실제 Docker Compose 명령으로 반영하는 반복 worker를 만든다.
// 관리자 페이지의 scale-out/in 버튼은 DB에 command만 기록하고, 이 worker가 command를 소비해 컨테이너를 조작한다.
func NewLocalDockerActuatorTask(
	store WorkerScaleCommandStore,
	interval time.Duration,
	logger *slog.Logger,
) func(context.Context) error {
	actuator := LocalDockerActuator{store: store, logger: logger}
	return func(ctx context.Context) error {
		return repeat(ctx, interval, func(iterationCtx context.Context) error {
			for {
				processed, err := actuator.ProcessOne(iterationCtx)
				if err != nil {
					logger.Error("local docker actuator iteration failed", "error", err)
				}
				if !processed || iterationCtx.Err() != nil {
					return nil
				}
			}
		})
	}
}

// NewLocalDockerActuatorRunOnceTask는 관리자가 "once"로 실행할 때 command 하나만 처리한다.
func NewLocalDockerActuatorRunOnceTask(
	store WorkerScaleCommandStore,
	logger *slog.Logger,
) func(context.Context) error {
	actuator := LocalDockerActuator{store: store, logger: logger}
	return func(ctx context.Context) error {
		_, err := actuator.ProcessOne(ctx)
		return err
	}
}

type LocalDockerActuator struct {
	store  WorkerScaleCommandStore
	logger *slog.Logger
}

// ProcessOne은 REQUESTED scale command 하나를 선점하고, Docker 실행 결과를 DB에 기록한다.
func (a LocalDockerActuator) ProcessOne(ctx context.Context) (bool, error) {
	execution, claimed, err := a.store.ClaimWorkerScaleCommand(ctx, localDockerDriver)
	if err != nil || !claimed {
		return claimed, err
	}

	err = a.runDockerCompose(ctx, execution)
	if err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		message := err.Error()
		if finishErr := a.store.CompleteWorkerScaleCommand(ctx, execution.Command.ID, "FAILED", &message); finishErr != nil {
			return true, fmt.Errorf("scale command failed and status update failed: %w", finishErr)
		}
		return true, err
	}

	if err := a.store.CompleteWorkerScaleCommand(ctx, execution.Command.ID, "SUCCEEDED", nil); err != nil {
		return true, err
	}
	a.logger.Info(
		"local docker scale command completed",
		"commandId", execution.Command.ID,
		"target", execution.Target.TargetKey,
		"requestedCapacity", execution.Command.RequestedCapacity,
	)
	return true, nil
}

func (a LocalDockerActuator) runDockerCompose(ctx context.Context, execution domain.WorkerScaleCommandExecution) error {
	if execution.Target.Driver != localDockerDriver {
		return fmt.Errorf("unsupported worker target driver: %s", execution.Target.Driver)
	}
	if execution.Command.RequestedCapacity < 0 {
		return fmt.Errorf("requested capacity must be zero or positive")
	}
	if execution.Command.RequestedCapacity > 1 {
		return fmt.Errorf("local docker actuator currently supports capacity 0 or 1")
	}

	cfg, err := parseLocalDockerTargetConfig(execution.Target)
	if err != nil {
		return err
	}
	composeDir, composeFile, err := resolveComposePath(cfg)
	if err != nil {
		return err
	}

	args := []string{"compose"}
	if cfg.ComposeProject != "" {
		args = append(args, "-p", cfg.ComposeProject)
	}
	args = append(args, "-f", composeFile)
	if execution.Command.RequestedCapacity == 0 {
		args = append(args, "stop", cfg.ServiceName)
	} else {
		args = append(args, "up", "-d", "--force-recreate", cfg.ServiceName)
	}

	a.logger.Info(
		"executing local docker command",
		"commandId", execution.Command.ID,
		"target", execution.Target.TargetKey,
		"composeDir", composeDir,
		"args", strings.Join(args, " "),
	)
	env := localDockerWorkerEnv(execution)
	output, err := runCommand(ctx, composeDir, env, "docker", args...)
	if output != "" {
		a.logger.Info("local docker command output", "commandId", execution.Command.ID, "output", output)
	}
	if err != nil {
		return fmt.Errorf("docker %s failed: %w", strings.Join(args, " "), err)
	}
	return nil
}

func localDockerWorkerEnv(execution domain.WorkerScaleCommandExecution) []string {
	if execution.Command.RequestedCapacity == 0 || len(execution.Command.Details) == 0 {
		return nil
	}
	var details localDockerCommandDetails
	if err := json.Unmarshal(execution.Command.Details, &details); err != nil {
		return nil
	}
	if details.WorkerName == "" {
		return nil
	}

	module := details.WorkerName
	if !strings.Contains(module, ".") {
		module = "workers." + module
	}
	env := []string{"WORKER_MODULE=" + module}

	// topic은 worker별 기본 환경 변수가 이미 compose에 있지만, command details에 있으면
	// 로컬 검증에서도 어떤 topic을 바라보는지 명시적으로 남겨 둔다.
	switch strings.ToUpper(details.Source) {
	case "BLS":
		env = append(env, "WORKER_KAFKA_RAW_BLS_TOPIC="+details.Topic)
	case "KIS":
		env = append(env, "WORKER_KAFKA_RAW_KIS_TOPIC="+details.Topic)
	case "SEC":
		env = append(env, "WORKER_KAFKA_RAW_SEC_TOPIC="+details.Topic)
	}
	return env
}

func parseLocalDockerTargetConfig(target domain.WorkerTarget) (localDockerTargetConfig, error) {
	var cfg localDockerTargetConfig
	if len(target.Config) > 0 {
		if err := json.Unmarshal(target.Config, &cfg); err != nil {
			return localDockerTargetConfig{}, fmt.Errorf("parse target config: %w", err)
		}
	}
	if cfg.ComposeFile == "" {
		cfg.ComposeFile = "docker-compose.local-worker.yml"
	}
	if cfg.ServiceName == "" {
		return localDockerTargetConfig{}, fmt.Errorf("target config serviceName is required")
	}
	return cfg, nil
}

func resolveComposePath(cfg localDockerTargetConfig) (string, string, error) {
	candidates := composeDirCandidates(cfg.ComposeDir)
	for _, dir := range candidates {
		composeFile := filepath.Join(dir, cfg.ComposeFile)
		if _, err := os.Stat(composeFile); err == nil {
			return dir, composeFile, nil
		}
	}
	return "", "", fmt.Errorf("compose file %s was not found in candidates: %s", cfg.ComposeFile, strings.Join(candidates, ", "))
}

func composeDirCandidates(configured string) []string {
	values := make([]string, 0, 4)
	appendIfNotEmpty := func(value string) {
		if value == "" {
			return
		}
		if abs, err := filepath.Abs(value); err == nil {
			values = append(values, abs)
		}
	}

	appendIfNotEmpty(configured)
	appendIfNotEmpty(os.Getenv("LOCAL_WORKER_COMPOSE_DIR"))
	if cwd, err := os.Getwd(); err == nil {
		appendIfNotEmpty(cwd)
		appendIfNotEmpty(filepath.Join(cwd, "trader-data"))
		appendIfNotEmpty(filepath.Join(cwd, "..", "trader-data"))
	}
	return values
}

func runCommand(ctx context.Context, dir string, extraEnv []string, name string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = dir
	command.Env = append(os.Environ(), extraEnv...)

	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	return strings.TrimSpace(output.String()), err
}
