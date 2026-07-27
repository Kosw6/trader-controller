package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

const workerColumns = `
    c.name, c.display_name, c.worker_type, c.desired_state,
    r.runtime_state, r.owner_instance_id, r.last_heartbeat_at,
    r.last_started_at, r.last_stopped_at, r.last_run_at, r.last_error,
    c.run_once_requested_at, c.run_once_claimed_at, c.config,
    GREATEST(c.updated_at, r.updated_at) AS updated_at`

type WorkerDefinition struct {
	Name         string
	DisplayName  string
	WorkerType   string
	DefaultState string
	Config       json.RawMessage
}

type WorkerControlState struct {
	Name               string
	DesiredState       string
	RunOnceRequestedAt *time.Time
	RunOnceClaimedAt   *time.Time
	RuntimeState       string
	OwnerInstanceID    *string
	LastHeartbeatAt    *time.Time
}

// RegisterWorkerDefinition은 코드에 존재하는 worker를 DB control-plane에 보장한다.
// migration에 기본 row가 있어도, 새 worker가 추가될 때 서버 기동만으로 목록에 나타나게 하려는 목적이다.
func (s *Store) RegisterWorkerDefinition(ctx context.Context, def WorkerDefinition) error {
	if def.DefaultState == "" {
		def.DefaultState = "DISABLED"
	}
	if len(def.Config) == 0 {
		def.Config = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
        INSERT INTO pipeline_worker_control (
            name, display_name, worker_type, desired_state, config
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (name) DO UPDATE
        SET display_name = EXCLUDED.display_name,
            worker_type = EXCLUDED.worker_type,
            updated_at = CURRENT_TIMESTAMP`,
		def.Name,
		def.DisplayName,
		def.WorkerType,
		def.DefaultState,
		def.Config,
	)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
        INSERT INTO pipeline_worker_runtime (name, runtime_state)
        VALUES ($1, 'STOPPED')
        ON CONFLICT (name) DO NOTHING`, def.Name)
	return err
}

// ListWorkers는 관리자 페이지가 worker desired/runtime 상태를 한 번에 볼 수 있게 join 결과를 반환한다.
func (s *Store) ListWorkers(ctx context.Context) ([]domain.ManagedWorker, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+workerColumns+`
        FROM pipeline_worker_control c
        JOIN pipeline_worker_runtime r ON r.name = c.name
        ORDER BY c.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.ManagedWorker, 0)
	for rows.Next() {
		value, err := scanManagedWorker(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// SetWorkerDesiredState는 관리자의 start/stop 요청을 DB에 남긴다.
// 실제 goroutine 시작/종료는 WorkerManager가 이 값을 주기적으로 읽고 수행한다.
func (s *Store) SetWorkerDesiredState(ctx context.Context, name, desiredState string) (domain.ManagedWorker, error) {
	command, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_control
        SET desired_state = $2,
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $1`,
		name,
		desiredState,
	)
	if err != nil {
		return domain.ManagedWorker{}, err
	}
	if command.RowsAffected() == 0 {
		return domain.ManagedWorker{}, store.ErrNotFound
	}
	return s.GetWorker(ctx, name)
}

// RequestWorkerRunOnce는 DISABLED worker라도 1회만 실행해 볼 수 있도록 요청 시각을 갱신한다.
func (s *Store) RequestWorkerRunOnce(ctx context.Context, name string) (domain.ManagedWorker, error) {
	command, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_control
        SET run_once_requested_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $1`, name)
	if err != nil {
		return domain.ManagedWorker{}, err
	}
	if command.RowsAffected() == 0 {
		return domain.ManagedWorker{}, store.ErrNotFound
	}
	return s.GetWorker(ctx, name)
}

// GetWorker는 단일 worker 상태를 조회한다.
// RecordKafkaLagSnapshot은 lag monitor가 측정한 consumer lag을 운영 조회용으로 저장한다.
func (s *Store) RecordKafkaLagSnapshot(ctx context.Context, snapshot domain.KafkaLagSnapshot) error {
	partitionLags := snapshot.PartitionLags
	if len(partitionLags) == 0 {
		partitionLags = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `
        WITH effective_policy AS (
            SELECT COALESCE((
                SELECT p.scale_out_lag_threshold
                FROM pipeline_worker_policy p
                WHERE p.enabled = TRUE
                  AND p.topic = $1
                  AND p.consumer_group = $2
                  AND p.scale_out_lag_threshold > 0
                ORDER BY p.priority DESC, p.id
                LIMIT 1
            ), $4::bigint) AS threshold_lag
        )
        INSERT INTO kafka_consumer_lag_snapshot (
            topic, consumer_group, total_lag, threshold_lag, status, partition_lags, measured_at
        )
        SELECT
            $1,
            $2,
            $3,
            ep.threshold_lag,
            CASE
                WHEN ep.threshold_lag > 0 AND $3::bigint > ep.threshold_lag THEN 'CRITICAL'
                WHEN ep.threshold_lag > 0 AND $3::bigint >= ((ep.threshold_lag * 8 + 9) / 10) THEN 'WARN'
                ELSE 'OK'
            END,
            $5,
            COALESCE($6::timestamptz, CURRENT_TIMESTAMP)
        FROM effective_policy ep`,
		snapshot.Topic,
		snapshot.ConsumerGroup,
		snapshot.TotalLag,
		snapshot.ThresholdLag,
		partitionLags,
		snapshot.MeasuredAt,
	)
	return err
}

// GetKafkaLagSummary는 최신 lag과 최근 히스토리를 관리자 페이지에 제공한다.
func (s *Store) GetKafkaLagSummary(ctx context.Context, limit int) (domain.KafkaLagSummary, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
        SELECT id, topic, consumer_group, total_lag, threshold_lag, status, partition_lags, measured_at
        FROM kafka_consumer_lag_snapshot
        ORDER BY measured_at DESC, id DESC
        LIMIT $1`, limit)
	if err != nil {
		return domain.KafkaLagSummary{}, err
	}
	defer rows.Close()

	result := domain.KafkaLagSummary{History: []domain.KafkaLagSnapshot{}}
	for rows.Next() {
		var value domain.KafkaLagSnapshot
		if err := rows.Scan(
			&value.ID,
			&value.Topic,
			&value.ConsumerGroup,
			&value.TotalLag,
			&value.ThresholdLag,
			&value.Status,
			&value.PartitionLags,
			&value.MeasuredAt,
		); err != nil {
			return domain.KafkaLagSummary{}, err
		}
		if result.Latest == nil {
			latest := value
			result.Latest = &latest
		}
		result.History = append(result.History, value)
	}
	return result, rows.Err()
}

func (s *Store) GetWorker(ctx context.Context, name string) (domain.ManagedWorker, error) {
	value, err := scanManagedWorker(s.pool.QueryRow(ctx, `
        SELECT `+workerColumns+`
        FROM pipeline_worker_control c
        JOIN pipeline_worker_runtime r ON r.name = c.name
        WHERE c.name = $1`, name))
	if err != nil {
		return domain.ManagedWorker{}, mapError(err)
	}
	return value, nil
}

// ListWorkerControlStates는 WorkerManager가 desired_state와 run-once 요청만 가볍게 읽는 내부용 조회다.
func (s *Store) ListWorkerControlStates(ctx context.Context) ([]WorkerControlState, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT
            c.name,
            c.desired_state,
            c.run_once_requested_at,
            c.run_once_claimed_at,
            r.runtime_state,
            r.owner_instance_id,
            r.last_heartbeat_at
        FROM pipeline_worker_control c
        JOIN pipeline_worker_runtime r ON r.name = c.name
        ORDER BY c.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]WorkerControlState, 0)
	for rows.Next() {
		var value WorkerControlState
		if err := rows.Scan(
			&value.Name,
			&value.DesiredState,
			&value.RunOnceRequestedAt,
			&value.RunOnceClaimedAt,
			&value.RuntimeState,
			&value.OwnerInstanceID,
			&value.LastHeartbeatAt,
		); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// TryAcquireWorkerLease는 같은 worker를 여러 controller가 동시에 실행하지 않도록 owner를 선점한다.
// heartbeat가 leaseTTL보다 오래 끊긴 owner는 죽은 것으로 보고 새 인스턴스가 takeover할 수 있다.
func (s *Store) TryAcquireWorkerLease(ctx context.Context, name, owner string, leaseTTL time.Duration, state string) (bool, error) {
	cutoff := time.Now().UTC().Add(-leaseTTL)
	command, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_runtime
        SET owner_instance_id = $2,
            runtime_state = $4,
            last_started_at = CURRENT_TIMESTAMP,
            last_heartbeat_at = CURRENT_TIMESTAMP,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $1
          AND (
              owner_instance_id IS NULL
              OR owner_instance_id = $2
              OR last_heartbeat_at IS NULL
              OR last_heartbeat_at < $3
          )`,
		name,
		owner,
		cutoff,
		state,
	)
	if err != nil {
		return false, err
	}
	return command.RowsAffected() > 0, nil
}

// ClaimWorkerRunOnce는 아직 처리하지 않은 run-once 요청을 현재 manager가 가져간다.
func (s *Store) ClaimWorkerRunOnce(ctx context.Context, name string) (bool, error) {
	command, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_control
        SET run_once_claimed_at = run_once_requested_at,
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $1
          AND run_once_requested_at IS NOT NULL
          AND (
              run_once_claimed_at IS NULL
              OR run_once_claimed_at < run_once_requested_at
          )`, name)
	if err != nil {
		return false, err
	}
	return command.RowsAffected() > 0, nil
}

// HeartbeatWorker는 manager가 살아 있고 해당 worker를 계속 소유 중임을 DB에 기록한다.
func (s *Store) HeartbeatWorker(ctx context.Context, name, owner, state string) error {
	_, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_runtime
        SET runtime_state = $3,
            last_heartbeat_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $1
          AND owner_instance_id = $2`, name, owner, state)
	return err
}

// FinishWorkerRun은 worker 실행이 끝났을 때 owner와 최종 상태를 정리한다.
func (s *Store) FinishWorkerRun(ctx context.Context, name, owner, state string, errText *string) error {
	_, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_runtime
        SET runtime_state = $3,
            owner_instance_id = NULL,
            last_stopped_at = CURRENT_TIMESTAMP,
            last_run_at = CURRENT_TIMESTAMP,
            last_error = $4,
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $1
          AND owner_instance_id = $2`, name, owner, state, errText)
	return err
}

// MarkWorkerStopped는 desired_state=DISABLED인데 실행 중이 아닌 worker를 STOPPED로 정리한다.
func (s *Store) MarkWorkerStopped(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_runtime
        SET runtime_state = 'STOPPED',
            owner_instance_id = NULL,
            last_stopped_at = COALESCE(last_stopped_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $1
          AND runtime_state <> 'STOPPED'`, name)
	return err
}

func scanManagedWorker(row rowScanner) (domain.ManagedWorker, error) {
	var value domain.ManagedWorker
	err := row.Scan(
		&value.Name,
		&value.DisplayName,
		&value.WorkerType,
		&value.DesiredState,
		&value.RuntimeState,
		&value.OwnerInstanceID,
		&value.LastHeartbeatAt,
		&value.LastStartedAt,
		&value.LastStoppedAt,
		&value.LastRunAt,
		&value.LastError,
		&value.RunOnceRequestedAt,
		&value.RunOnceClaimedAt,
		&value.Config,
		&value.UpdatedAt,
	)
	return value, err
}
