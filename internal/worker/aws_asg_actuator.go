package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
)

const awsASGDriver = "AWS_ASG"

type awsASGTargetConfig struct {
	AutoScalingGroupName string `json:"autoScalingGroupName"`
	Region               string `json:"region"`
	Profile              string `json:"profile"`
	CLIPath              string `json:"cliPath"`
	HonorCooldown        bool   `json:"honorCooldown"`
}

// NewAWSASGActuatorTask는 AWS 환경에서 scale command를 Auto Scaling Group desired capacity 변경으로 반영한다.
//
// 이 worker는 직접 어떤 작업을 실행하지 않는다. DB에 기록된 "python-worker-node를 몇 대로 맞춰라"라는
// 명령만 소비하고, 실제 Python worker EC2/컨테이너 기동은 ASG launch template과 user_data가 담당한다.
func NewAWSASGActuatorTask(
	store WorkerScaleCommandStore,
	interval time.Duration,
	logger *slog.Logger,
) func(context.Context) error {
	actuator := AWSASGActuator{store: store, logger: logger}
	return func(ctx context.Context) error {
		return repeat(ctx, interval, func(iterationCtx context.Context) error {
			for {
				processed, err := actuator.ProcessOne(iterationCtx)
				if err != nil {
					logger.Error("aws asg actuator iteration failed", "error", err)
				}
				if !processed || iterationCtx.Err() != nil {
					return nil
				}
			}
		})
	}
}

// NewAWSASGActuatorRunOnceTask는 관리자 페이지의 once 실행으로 AWS scale command 하나만 처리한다.
func NewAWSASGActuatorRunOnceTask(
	store WorkerScaleCommandStore,
	logger *slog.Logger,
) func(context.Context) error {
	actuator := AWSASGActuator{store: store, logger: logger}
	return func(ctx context.Context) error {
		_, err := actuator.ProcessOne(ctx)
		return err
	}
}

type AWSASGActuator struct {
	store  WorkerScaleCommandStore
	logger *slog.Logger
}

// ProcessOne은 AWS_ASG driver를 가진 REQUESTED scale command 하나를 선점하고 ASG desired capacity를 변경한다.
func (a AWSASGActuator) ProcessOne(ctx context.Context) (bool, error) {
	execution, claimed, err := a.store.ClaimWorkerScaleCommand(ctx, awsASGDriver)
	if err != nil || !claimed {
		return claimed, err
	}

	err = a.setDesiredCapacity(ctx, execution)
	if err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		message := err.Error()
		if finishErr := a.store.CompleteWorkerScaleCommand(ctx, execution.Command.ID, "FAILED", &message); finishErr != nil {
			return true, fmt.Errorf("aws scale command failed and status update failed: %w", finishErr)
		}
		return true, err
	}

	if err := a.store.CompleteWorkerScaleCommand(ctx, execution.Command.ID, "SUCCEEDED", nil); err != nil {
		return true, err
	}
	a.logger.Info(
		"aws asg scale command completed",
		"commandId", execution.Command.ID,
		"target", execution.Target.TargetKey,
		"requestedCapacity", execution.Command.RequestedCapacity,
	)
	return true, nil
}

func (a AWSASGActuator) setDesiredCapacity(ctx context.Context, execution domain.WorkerScaleCommandExecution) error {
	if execution.Target.Driver != awsASGDriver {
		return fmt.Errorf("unsupported worker target driver: %s", execution.Target.Driver)
	}
	if execution.Command.RequestedCapacity < 0 {
		return fmt.Errorf("requested capacity must be zero or positive")
	}
	if execution.Command.RequestedCapacity < execution.Target.MinCapacity ||
		execution.Command.RequestedCapacity > execution.Target.MaxCapacity {
		return fmt.Errorf(
			"requested capacity %d is outside target range %d-%d",
			execution.Command.RequestedCapacity,
			execution.Target.MinCapacity,
			execution.Target.MaxCapacity,
		)
	}

	cfg, err := parseAWSASGTargetConfig(execution.Target)
	if err != nil {
		return err
	}

	args := []string{
		"autoscaling",
		"set-desired-capacity",
		"--auto-scaling-group-name",
		cfg.AutoScalingGroupName,
		"--desired-capacity",
		fmt.Sprintf("%d", execution.Command.RequestedCapacity),
	}
	if cfg.HonorCooldown {
		args = append(args, "--honor-cooldown")
	}
	if cfg.Region != "" {
		args = append(args, "--region", cfg.Region)
	}
	if cfg.Profile != "" {
		args = append(args, "--profile", cfg.Profile)
	}

	cliPath := cfg.CLIPath
	if cliPath == "" {
		cliPath = os.Getenv("AWS_CLI_PATH")
	}
	if cliPath == "" {
		cliPath = "aws"
	}

	a.logger.Info(
		"executing aws autoscaling command",
		"commandId", execution.Command.ID,
		"target", execution.Target.TargetKey,
		"asg", cfg.AutoScalingGroupName,
		"requestedCapacity", execution.Command.RequestedCapacity,
		"args", strings.Join(args, " "),
	)

	output, err := runCommand(ctx, "", nil, cliPath, args...)
	if output != "" {
		a.logger.Info("aws autoscaling command output", "commandId", execution.Command.ID, "output", output)
	}
	if err != nil {
		return fmt.Errorf("%s %s failed: %w", cliPath, strings.Join(args, " "), err)
	}
	return nil
}

func parseAWSASGTargetConfig(target domain.WorkerTarget) (awsASGTargetConfig, error) {
	var cfg awsASGTargetConfig
	if len(target.Config) > 0 {
		if err := json.Unmarshal(target.Config, &cfg); err != nil {
			return awsASGTargetConfig{}, fmt.Errorf("parse target config: %w", err)
		}
	}
	if cfg.AutoScalingGroupName == "" {
		return awsASGTargetConfig{}, fmt.Errorf("target config autoScalingGroupName is required")
	}
	return cfg, nil
}
