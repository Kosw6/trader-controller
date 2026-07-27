package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/kosw006/trader-controller/internal/postgres"
)

type Definition struct {
	Name         string
	DisplayName  string
	WorkerType   string
	DefaultState string
	Run          func(context.Context) error
	RunOnce      func(context.Context) error
}

type Store interface {
	RegisterWorkerDefinition(context.Context, postgres.WorkerDefinition) error
	ListWorkerControlStates(context.Context) ([]postgres.WorkerControlState, error)
	TryAcquireWorkerLease(context.Context, string, string, time.Duration, string) (bool, error)
	ClaimWorkerRunOnce(context.Context, string) (bool, error)
	HeartbeatWorker(context.Context, string, string, string) error
	FinishWorkerRun(context.Context, string, string, string, *string) error
	MarkWorkerStopped(context.Context, string) error
}

type Manager struct {
	store        Store
	definitions  map[string]Definition
	pollInterval time.Duration
	leaseTTL     time.Duration
	ownerID      string
	logger       *slog.Logger

	mu      sync.Mutex
	running map[string]*runningWorker
}

type runningWorker struct {
	cancel context.CancelFunc
	mode   string
}

func NewManager(
	store Store,
	definitions []Definition,
	pollInterval time.Duration,
	leaseTTL time.Duration,
	logger *slog.Logger,
) *Manager {
	index := make(map[string]Definition, len(definitions))
	for _, definition := range definitions {
		index[definition.Name] = definition
	}
	return &Manager{
		store:        store,
		definitions:  index,
		pollInterval: pollInterval,
		leaseTTL:     leaseTTL,
		ownerID:      newOwnerID(),
		logger:       logger,
		running:      map[string]*runningWorker{},
	}
}

// Run은 controller 프로세스와 함께 살아 있는 worker control loop다.
// DB의 desired_state를 주기적으로 읽고, ENABLED면 goroutine을 띄우고 DISABLED면 context cancel로 멈춘다.
func (m *Manager) Run(ctx context.Context) {
	if err := m.registerDefinitions(ctx); err != nil {
		m.logger.Error("worker definition registration failed", "error", err)
		return
	}

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	m.sync(ctx)
	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return
		case <-ticker.C:
			m.sync(ctx)
		}
	}
}

// registerDefinitions는 코드에 포함된 worker 목록을 DB에 보장한다.
// migration에 누락된 worker가 있어도 서버 기동 시 관리 페이지에 나타나게 하기 위한 단계다.
func (m *Manager) registerDefinitions(ctx context.Context) error {
	for _, definition := range m.definitions {
		if err := m.store.RegisterWorkerDefinition(ctx, postgres.WorkerDefinition{
			Name:         definition.Name,
			DisplayName:  definition.DisplayName,
			WorkerType:   definition.WorkerType,
			DefaultState: definition.DefaultState,
		}); err != nil {
			return err
		}
	}
	return nil
}

// sync는 DB control 상태와 현재 goroutine 상태를 맞춘다.
// 이 함수가 control-plane의 핵심이며, HTTP API는 DB desired_state만 바꾸고 실제 실행은 여기서 처리한다.
func (m *Manager) sync(ctx context.Context) {
	states, err := m.store.ListWorkerControlStates(ctx)
	if err != nil {
		m.logger.Error("worker control state query failed", "error", err)
		return
	}

	for _, state := range states {
		definition, ok := m.definitions[state.Name]
		if !ok {
			continue
		}
		mode, running := m.runningMode(state.Name)
		if running {
			m.heartbeat(ctx, state.Name)
		}

		if state.DesiredState == "ENABLED" && !running {
			m.start(ctx, definition, "RUNNING")
			continue
		}
		if state.DesiredState == "DISABLED" && running && mode == "RUNNING" {
			m.stop(state.Name)
			continue
		}
		if state.DesiredState == "DISABLED" && !running {
			if err := m.store.MarkWorkerStopped(ctx, state.Name); err != nil {
				m.logger.Warn("worker stopped state update failed", "worker", state.Name, "error", err)
			}
		}
		if m.hasPendingRunOnce(state) && !running {
			claimed, err := m.store.ClaimWorkerRunOnce(ctx, state.Name)
			if err != nil {
				m.logger.Error("worker run-once claim failed", "worker", state.Name, "error", err)
				continue
			}
			if claimed {
				m.start(ctx, definition, "RUN_ONCE")
			}
		}
	}
}

func (m *Manager) hasPendingRunOnce(state postgres.WorkerControlState) bool {
	return state.RunOnceRequestedAt != nil &&
		(state.RunOnceClaimedAt == nil || state.RunOnceClaimedAt.Before(*state.RunOnceRequestedAt))
}

// start는 worker goroutine을 띄우기 전에 DB lease를 먼저 잡는다.
// 이 lease 덕분에 나중에 controller가 여러 대 떠도 같은 worker가 동시에 두 번 실행되는 위험을 줄일 수 있다.
func (m *Manager) start(parent context.Context, definition Definition, mode string) {
	state := mode
	acquired, err := m.store.TryAcquireWorkerLease(parent, definition.Name, m.ownerID, m.leaseTTL, state)
	if err != nil {
		m.logger.Error("worker lease acquisition failed", "worker", definition.Name, "error", err)
		return
	}
	if !acquired {
		m.logger.Debug("worker lease is held by another controller", "worker", definition.Name)
		return
	}

	ctx, cancel := context.WithCancel(parent)
	m.mu.Lock()
	if _, exists := m.running[definition.Name]; exists {
		m.mu.Unlock()
		cancel()
		return
	}
	m.running[definition.Name] = &runningWorker{cancel: cancel, mode: mode}
	m.mu.Unlock()

	m.logger.Info("managed worker started", "worker", definition.Name, "mode", mode, "owner", m.ownerID)
	go m.runWorker(ctx, definition, mode)
}

// runWorker는 panic과 error를 runtime table에 남긴다.
// worker 내부 장애가 controller API 서버 전체를 죽이지 않게 분리하는 것이 목적이다.
func (m *Manager) runWorker(ctx context.Context, definition Definition, mode string) {
	finalState := "STOPPED"
	var finalError *string
	defer func() {
		if recovered := recover(); recovered != nil {
			finalState = "ERROR"
			message := fmt.Sprintf("panic: %v", recovered)
			finalError = &message
			m.logger.Error("managed worker panic recovered", "worker", definition.Name, "panic", recovered)
		}
		if err := m.store.FinishWorkerRun(context.Background(), definition.Name, m.ownerID, finalState, finalError); err != nil {
			m.logger.Error("worker finish update failed", "worker", definition.Name, "error", err)
		}
		m.mu.Lock()
		delete(m.running, definition.Name)
		m.mu.Unlock()
	}()

	run := definition.Run
	if mode == "RUN_ONCE" && definition.RunOnce != nil {
		run = definition.RunOnce
	}
	if err := run(ctx); err != nil && ctx.Err() == nil {
		finalState = "ERROR"
		message := err.Error()
		finalError = &message
		m.logger.Error("managed worker stopped with error", "worker", definition.Name, "error", err)
		return
	}
	m.logger.Info("managed worker stopped", "worker", definition.Name, "mode", mode)
}

func (m *Manager) heartbeat(ctx context.Context, name string) {
	m.mu.Lock()
	running := m.running[name]
	m.mu.Unlock()
	if running == nil {
		return
	}
	if err := m.store.HeartbeatWorker(ctx, name, m.ownerID, running.mode); err != nil {
		m.logger.Warn("worker heartbeat failed", "worker", name, "error", err)
	}
}

func (m *Manager) runningMode(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	running, ok := m.running[name]
	if !ok {
		return "", false
	}
	return running.mode, true
}

func (m *Manager) stop(name string) {
	m.mu.Lock()
	running := m.running[name]
	m.mu.Unlock()
	if running == nil {
		return
	}
	m.logger.Info("managed worker stop requested", "worker", name)
	running.cancel()
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, running := range m.running {
		m.logger.Info("managed worker shutdown requested", "worker", name)
		running.cancel()
	}
}

func newOwnerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}
