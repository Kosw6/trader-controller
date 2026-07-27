package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

const workerTargetColumns = `
    target_key, display_name, environment, driver, enabled,
    min_capacity, max_capacity, desired_capacity, scale_in_idle_seconds,
    config, created_at, updated_at`

const workerPolicyColumns = `
    id, policy_key, target_key, source, worker_name, worker_role, enabled,
    mode, topic, consumer_group, scale_out_lag_threshold, scale_out_queue_threshold,
    batch_size, max_concurrency, scale_in_idle_seconds,
    to_char(allowed_start_time, 'HH24:MI') AS allowed_start_time,
    to_char(allowed_end_time, 'HH24:MI') AS allowed_end_time,
    check_interval_hours, priority, config, created_at, updated_at`

const workerHeartbeatColumns = `
    node_name, target_key, status, idle, accepting_new_work, running_workers,
    running_job_items, processing_source_objects, last_heartbeat_at,
    metadata, created_at, updated_at`

const plannerRunColumns = `
    id, source, planner_type, status, detected_count, planned_count,
    skipped_count, reason, policy_snapshot, details, pipeline_job_id,
    started_at, completed_at, error_message, created_at`

const scaleCommandColumns = `
    id, target_key, command_type, requested_capacity, previous_capacity,
    reason, triggered_by, status, planner_run_id, details, requested_at,
    started_at, completed_at, error_message`

// ListWorkerTargets는 Python worker node처럼 controller가 켜고 끌 대상 목록을 조회한다.
func (s *Store) ListWorkerTargets(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerTarget, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+workerTargetColumns+`
        FROM pipeline_worker_target
        WHERE ($1 = '' OR target_key = $1)
        ORDER BY target_key
        LIMIT $2 OFFSET $3`,
		filter.TargetKey,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.WorkerTarget, 0)
	for rows.Next() {
		value, err := scanWorkerTarget(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// ListWorkerPolicies는 source별 ETL/collector 실행 정책을 관리자와 worker-agent에 제공한다.
// UpdateWorkerTarget은 local docker와 AWS ASG 중 어느 actuator가 target을 제어할지 수정한다.
// 운영자는 이 값을 통해 같은 scale command 흐름을 로컬 Docker 또는 AWS ASG에 연결할 수 있다.
func (s *Store) UpdateWorkerTarget(ctx context.Context, targetKey string, req domain.UpdateWorkerTargetRequest) (domain.WorkerTarget, error) {
	if len(req.Config) == 0 {
		req.Config = json.RawMessage(`{}`)
	}
	value, err := scanWorkerTarget(s.pool.QueryRow(ctx, `
        UPDATE pipeline_worker_target
        SET display_name = $2,
            environment = $3,
            driver = $4,
            enabled = $5,
            min_capacity = $6,
            max_capacity = $7,
            desired_capacity = $8,
            scale_in_idle_seconds = $9,
            config = $10,
            updated_at = CURRENT_TIMESTAMP
        WHERE target_key = $1
        RETURNING `+workerTargetColumns,
		targetKey,
		req.DisplayName,
		req.Environment,
		req.Driver,
		req.Enabled,
		req.MinCapacity,
		req.MaxCapacity,
		req.DesiredCapacity,
		req.ScaleInIdleSeconds,
		req.Config,
	))
	if err != nil {
		return domain.WorkerTarget{}, mapError(err)
	}
	return value, nil
}

func (s *Store) ListWorkerPolicies(ctx context.Context, filter domain.WorkerPolicyFilter) ([]domain.WorkerPolicy, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+workerPolicyColumns+`
        FROM pipeline_worker_policy
        WHERE ($1 = '' OR target_key = $1)
          AND ($2 = '' OR source = $2)
        ORDER BY priority DESC, source, worker_name
        LIMIT $3 OFFSET $4`,
		filter.TargetKey,
		filter.Source,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.WorkerPolicy, 0)
	for rows.Next() {
		value, err := scanWorkerPolicy(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// GetWorkerAgentPolicies는 Python worker-agent가 부팅 직후 읽는 정책 묶음이다.
// agent는 이 응답만 보고 어떤 worker process를 띄울지, 몇 분마다 갱신할지 판단한다.
// UpdateWorkerPolicy는 관리자 페이지에서 source별 worker 실행 기준을 조정할 때 사용한다.
// lag threshold, queue threshold, batch size, 허용 시간대 같은 운영 정책은 DB에 두고
// Go controller와 Python worker-agent가 같은 값을 읽도록 만든다.
func (s *Store) UpdateWorkerPolicy(ctx context.Context, id int64, req domain.UpdateWorkerPolicyRequest) (domain.WorkerPolicy, error) {
	if len(req.Config) == 0 {
		req.Config = json.RawMessage(`{}`)
	}
	allowedStartTime := stringFromPointer(req.AllowedStartTime)
	allowedEndTime := stringFromPointer(req.AllowedEndTime)
	value, err := scanWorkerPolicy(s.pool.QueryRow(ctx, `
        UPDATE pipeline_worker_policy
        SET enabled = $2,
            scale_out_lag_threshold = $3,
            scale_out_queue_threshold = $4,
            batch_size = $5,
            max_concurrency = $6,
            scale_in_idle_seconds = $7,
            allowed_start_time = NULLIF($8, '')::time,
            allowed_end_time = NULLIF($9, '')::time,
            check_interval_hours = $10,
            priority = $11,
            config = $12,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        RETURNING `+workerPolicyColumns,
		id,
		req.Enabled,
		req.ScaleOutLagThreshold,
		req.ScaleOutQueueThreshold,
		req.BatchSize,
		req.MaxConcurrency,
		req.ScaleInIdleSeconds,
		allowedStartTime,
		allowedEndTime,
		req.CheckIntervalHours,
		req.Priority,
		req.Config,
	))
	if err != nil {
		return domain.WorkerPolicy{}, mapError(err)
	}
	return value, nil
}

func (s *Store) GetWorkerAgentPolicies(ctx context.Context, targetKey string) (domain.WorkerAgentPolicies, error) {
	target, err := scanWorkerTarget(s.pool.QueryRow(ctx, `
        SELECT `+workerTargetColumns+`
        FROM pipeline_worker_target
        WHERE target_key = $1`, targetKey))
	if err != nil {
		return domain.WorkerAgentPolicies{}, mapError(err)
	}
	policies, err := s.ListWorkerPolicies(ctx, domain.WorkerPolicyFilter{
		TargetKey: targetKey,
		Limit:     200,
	})
	if err != nil {
		return domain.WorkerAgentPolicies{}, err
	}
	return domain.WorkerAgentPolicies{
		Target:                   target,
		Policies:                 policies,
		RefreshIntervalSeconds:   300,
		HeartbeatIntervalSeconds: 30,
	}, nil
}

// UpsertWorkerHeartbeat는 Python node가 현재 처리 상태를 주기적으로 보고하는 입구다.
// scale-in 판단은 반드시 이 heartbeat의 idle/running count를 보고 결정한다.
func (s *Store) UpsertWorkerHeartbeat(ctx context.Context, req domain.WorkerHeartbeatRequest) (domain.WorkerNodeHeartbeat, error) {
	if len(req.RunningWorkers) == 0 {
		req.RunningWorkers = json.RawMessage(`[]`)
	}
	if len(req.Metadata) == 0 {
		req.Metadata = json.RawMessage(`{}`)
	}
	value, err := scanWorkerHeartbeat(s.pool.QueryRow(ctx, `
        INSERT INTO pipeline_worker_node_heartbeat (
            node_name, target_key, status, idle, accepting_new_work,
            running_workers, running_job_items, processing_source_objects,
            last_heartbeat_at, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, $9
        )
        ON CONFLICT (node_name) DO UPDATE
        SET target_key = EXCLUDED.target_key,
            status = EXCLUDED.status,
            idle = EXCLUDED.idle,
            accepting_new_work = EXCLUDED.accepting_new_work,
            running_workers = EXCLUDED.running_workers,
            running_job_items = EXCLUDED.running_job_items,
            processing_source_objects = EXCLUDED.processing_source_objects,
            last_heartbeat_at = CURRENT_TIMESTAMP,
            metadata = EXCLUDED.metadata,
            updated_at = CURRENT_TIMESTAMP
        RETURNING `+workerHeartbeatColumns,
		req.NodeName,
		req.TargetKey,
		req.Status,
		req.Idle,
		req.AcceptingNewWork,
		req.RunningWorkers,
		req.RunningJobItems,
		req.ProcessingSourceObjects,
		req.Metadata,
	))
	return value, err
}

// ListWorkerHeartbeats는 관리자 화면에서 외부 Python node가 살아 있는지 확인하는 목록이다.
func (s *Store) ListWorkerHeartbeats(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerNodeHeartbeat, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+workerHeartbeatColumns+`
        FROM pipeline_worker_node_heartbeat
        WHERE ($1 = '' OR target_key = $1)
          AND ($2 = '' OR status = $2)
        ORDER BY last_heartbeat_at DESC
        LIMIT $3 OFFSET $4`,
		filter.TargetKey,
		filter.Status,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.WorkerNodeHeartbeat, 0)
	for rows.Next() {
		value, err := scanWorkerHeartbeat(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// CreateWorkerScaleCommand는 수동 또는 자동 판단 결과를 scale command로 기록한다.
// 1차 구현에서는 desired_capacity를 함께 갱신하고, 실제 Docker/ASG 조작은 이후 actuator loop가 이 command를 소비한다.
func (s *Store) CreateWorkerScaleCommand(
	ctx context.Context,
	targetKey string,
	req domain.CreateWorkerScaleCommandRequest,
) (domain.WorkerScaleCommand, error) {
	if len(req.Details) == 0 {
		req.Details = json.RawMessage(`{}`)
	}
	if req.Reason == "" {
		req.Reason = "MANUAL_REQUEST"
	}
	if req.TriggeredBy == "" {
		req.TriggeredBy = "MANUAL"
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkerScaleCommand{}, err
	}
	defer tx.Rollback(ctx)

	var minCapacity, maxCapacity, previousCapacity int
	err = tx.QueryRow(ctx, `
        SELECT min_capacity, max_capacity, desired_capacity
        FROM pipeline_worker_target
        WHERE target_key = $1
        FOR UPDATE`, targetKey).Scan(&minCapacity, &maxCapacity, &previousCapacity)
	if err != nil {
		return domain.WorkerScaleCommand{}, mapError(err)
	}
	if req.RequestedCapacity < minCapacity || req.RequestedCapacity > maxCapacity {
		return domain.WorkerScaleCommand{}, fmt.Errorf(
			"requestedCapacity must be between %d and %d",
			minCapacity,
			maxCapacity,
		)
	}

	commandType := "SET_CAPACITY"
	if req.RequestedCapacity > previousCapacity {
		commandType = "SCALE_OUT"
	} else if req.RequestedCapacity < previousCapacity {
		commandType = "SCALE_IN"
	}

	value, err := scanWorkerScaleCommand(tx.QueryRow(ctx, `
        INSERT INTO pipeline_worker_scale_command (
            target_key, command_type, requested_capacity, previous_capacity,
            reason, triggered_by, status, details
        ) VALUES ($1, $2, $3, $4, $5, $6, 'REQUESTED', $7)
        RETURNING `+scaleCommandColumns,
		targetKey,
		commandType,
		req.RequestedCapacity,
		previousCapacity,
		req.Reason,
		req.TriggeredBy,
		req.Details,
	))
	if err != nil {
		return domain.WorkerScaleCommand{}, err
	}
	_, err = tx.Exec(ctx, `
        UPDATE pipeline_worker_target
        SET desired_capacity = $2,
            updated_at = CURRENT_TIMESTAMP
        WHERE target_key = $1`,
		targetKey,
		req.RequestedCapacity,
	)
	if err != nil {
		return domain.WorkerScaleCommand{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkerScaleCommand{}, err
	}
	return value, nil
}

// ListWorkerScaleCommands는 왜 worker node를 켜거나 끄려고 했는지 운영 이력을 보여준다.
func (s *Store) ListWorkerScaleCommands(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerScaleCommand, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+scaleCommandColumns+`
        FROM pipeline_worker_scale_command
        WHERE ($1 = '' OR target_key = $1)
          AND ($2 = '' OR status = $2)
        ORDER BY requested_at DESC, id DESC
        LIMIT $3 OFFSET $4`,
		filter.TargetKey,
		filter.Status,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.WorkerScaleCommand, 0)
	for rows.Next() {
		value, err := scanWorkerScaleCommand(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// CreateLagTriggeredScaleCommands는 최신 Kafka lag와 worker policy를 비교해 scale-out 명령을 만든다.
// consumer-lag-monitor는 측정만 담당하고, 이 함수는 "정책 임계치 이상이면 worker node를 켠다"는 판단을 담당한다.
// 이미 target이 켜져 있거나 처리 대기/RUNNING command가 있으면 중복 scale-out을 만들지 않는다.
func (s *Store) CreateLagTriggeredScaleCommands(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 20
	}
	commandTag, err := s.pool.Exec(ctx, `
        WITH latest_lag AS (
            SELECT DISTINCT ON (topic, consumer_group)
                topic,
                consumer_group,
                total_lag,
                measured_at
            FROM kafka_consumer_lag_snapshot
            ORDER BY topic, consumer_group, measured_at DESC, id DESC
        ), candidates AS (
            SELECT
                p.id AS policy_id,
                p.policy_key,
                p.target_key,
                p.source,
                p.worker_name,
                p.topic,
                p.consumer_group,
                p.scale_out_lag_threshold,
                p.batch_size,
                p.max_concurrency,
                l.total_lag,
                l.measured_at,
                t.desired_capacity,
                t.max_capacity
            FROM pipeline_worker_policy p
            JOIN pipeline_worker_target t ON t.target_key = p.target_key
            JOIN latest_lag l
              ON l.topic = p.topic
             AND l.consumer_group = p.consumer_group
            WHERE p.enabled = TRUE
              AND t.enabled = TRUE
              AND p.worker_role IN ('ETL', 'COMBINED')
              AND p.topic IS NOT NULL
              AND p.consumer_group IS NOT NULL
              AND p.scale_out_lag_threshold > 0
              AND l.total_lag >= p.scale_out_lag_threshold
              AND t.desired_capacity < t.max_capacity
              AND (
                    p.allowed_start_time IS NULL
                    OR p.allowed_end_time IS NULL
                    OR (
                        p.allowed_start_time <= p.allowed_end_time
                        AND CURRENT_TIME BETWEEN p.allowed_start_time AND p.allowed_end_time
                    )
                    OR (
                        p.allowed_start_time > p.allowed_end_time
                        AND (
                            CURRENT_TIME >= p.allowed_start_time
                            OR CURRENT_TIME <= p.allowed_end_time
                        )
                    )
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM pipeline_worker_scale_command c
                    WHERE c.target_key = p.target_key
                      AND c.status IN ('REQUESTED', 'RUNNING')
              )
            ORDER BY p.priority DESC, l.total_lag DESC, p.id
            LIMIT $1
        ), inserted AS (
            INSERT INTO pipeline_worker_scale_command (
                target_key,
                command_type,
                requested_capacity,
                previous_capacity,
                reason,
                triggered_by,
                status,
                details
            )
            SELECT
                target_key,
                'SCALE_OUT',
                LEAST(max_capacity, GREATEST(desired_capacity + 1, 1)),
                desired_capacity,
                'KAFKA_LAG_THRESHOLD_EXCEEDED',
                'LAG_MONITOR',
                'REQUESTED',
                jsonb_build_object(
                    'policyId', policy_id,
                    'policyKey', policy_key,
                    'source', source,
                    'workerName', worker_name,
                    'topic', topic,
                    'consumerGroup', consumer_group,
                    'totalLag', total_lag,
                    'scaleOutLagThreshold', scale_out_lag_threshold,
                    'batchSize', batch_size,
                    'maxConcurrency', max_concurrency,
                    'measuredAt', measured_at
                )
            FROM candidates
            RETURNING target_key, requested_capacity
        )
        UPDATE pipeline_worker_target t
        SET desired_capacity = i.requested_capacity,
            updated_at = CURRENT_TIMESTAMP
        FROM inserted i
        WHERE t.target_key = i.target_key`,
		limit,
	)
	if err != nil {
		return 0, err
	}
	return commandTag.RowsAffected(), nil
}

// ClaimWorkerScaleCommand는 actuator가 처리할 scale command 하나를 선점한다.
// SKIP LOCKED를 사용해 controller가 여러 대 떠도 같은 command를 동시에 실행하지 않게 한다.
// CreateIdleScaleInCommands creates scale-in commands when a target has no lag
// and its recent worker heartbeats have stayed idle longer than the target
// policy allows. The actuator still performs the actual Docker or ASG change.
func (s *Store) CreateIdleScaleInCommands(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 20
	}
	commandTag, err := s.pool.Exec(ctx, `
        WITH latest_lag AS (
            SELECT DISTINCT ON (topic, consumer_group)
                topic,
                consumer_group,
                total_lag,
                measured_at
            FROM kafka_consumer_lag_snapshot
            ORDER BY topic, consumer_group, measured_at DESC, id DESC
        ), policy_lag AS (
            SELECT
                p.target_key,
                COUNT(*) AS policy_count,
                COUNT(l.topic) AS snapshot_count,
                MAX(COALESCE(l.total_lag, 0)) AS max_total_lag,
                jsonb_agg(
                    jsonb_build_object(
                        'policyId', p.id,
                        'policyKey', p.policy_key,
                        'source', p.source,
                        'topic', p.topic,
                        'consumerGroup', p.consumer_group,
                        'totalLag', l.total_lag,
                        'measuredAt', l.measured_at
                    )
                    ORDER BY p.priority DESC, p.id
                ) AS lag_snapshot
            FROM pipeline_worker_policy p
            LEFT JOIN latest_lag l
              ON l.topic = p.topic
             AND l.consumer_group = p.consumer_group
             AND l.measured_at >= CURRENT_TIMESTAMP - interval '3 minutes'
            WHERE p.enabled = TRUE
              AND p.worker_role IN ('ETL', 'COMBINED')
              AND p.topic IS NOT NULL
              AND p.consumer_group IS NOT NULL
            GROUP BY p.target_key
        ), recent_heartbeat AS (
            SELECT
                h.*,
                CASE
                    WHEN h.metadata ? 'idleSince'
                     AND (h.metadata->>'idleSince') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
                    THEN (h.metadata->>'idleSince')::timestamptz
                    ELSE h.created_at
                END AS idle_since
            FROM pipeline_worker_node_heartbeat h
            WHERE h.last_heartbeat_at >= CURRENT_TIMESTAMP - interval '3 minutes'
        ), heartbeat_summary AS (
            SELECT
                target_key,
                COUNT(*) AS recent_nodes,
                BOOL_AND(
                    idle = TRUE
                    AND status = 'IDLE'
                    AND running_job_items = 0
                    AND processing_source_objects = 0
                ) AS all_idle,
                MIN(idle_since) AS oldest_idle_since,
                jsonb_agg(
                    jsonb_build_object(
                        'nodeName', node_name,
                        'status', status,
                        'idle', idle,
                        'runningJobItems', running_job_items,
                        'processingSourceObjects', processing_source_objects,
                        'lastHeartbeatAt', last_heartbeat_at,
                        'idleSince', idle_since
                    )
                    ORDER BY last_heartbeat_at DESC
                ) AS heartbeat_snapshot
            FROM recent_heartbeat
            GROUP BY target_key
        ), candidates AS (
            SELECT
                t.target_key,
                t.min_capacity,
                t.desired_capacity,
                t.scale_in_idle_seconds,
                pl.lag_snapshot,
                hs.recent_nodes,
                hs.oldest_idle_since,
                hs.heartbeat_snapshot
            FROM pipeline_worker_target t
            JOIN policy_lag pl ON pl.target_key = t.target_key
            JOIN heartbeat_summary hs ON hs.target_key = t.target_key
            WHERE t.enabled = TRUE
              AND t.desired_capacity > t.min_capacity
              AND t.scale_in_idle_seconds > 0
              AND pl.policy_count = pl.snapshot_count
              AND pl.max_total_lag = 0
              AND hs.recent_nodes > 0
              AND hs.all_idle = TRUE
              AND hs.oldest_idle_since <= CURRENT_TIMESTAMP - (t.scale_in_idle_seconds || ' seconds')::interval
              AND NOT EXISTS (
                    SELECT 1
                    FROM pipeline_worker_scale_command c
                    WHERE c.target_key = t.target_key
                      AND c.status IN ('REQUESTED', 'RUNNING')
              )
            ORDER BY hs.oldest_idle_since, t.target_key
            LIMIT $1
        ), inserted AS (
            INSERT INTO pipeline_worker_scale_command (
                target_key,
                command_type,
                requested_capacity,
                previous_capacity,
                reason,
                triggered_by,
                status,
                details
            )
            SELECT
                target_key,
                'SCALE_IN',
                GREATEST(min_capacity, desired_capacity - 1),
                desired_capacity,
                'IDLE_TIMEOUT',
                'SYSTEM',
                'REQUESTED',
                jsonb_build_object(
                    'scaleInIdleSeconds', scale_in_idle_seconds,
                    'recentNodes', recent_nodes,
                    'oldestIdleSince', oldest_idle_since,
                    'lagSnapshot', lag_snapshot,
                    'heartbeatSnapshot', heartbeat_snapshot
                )
            FROM candidates
            RETURNING target_key, requested_capacity
        )
        UPDATE pipeline_worker_target t
        SET desired_capacity = i.requested_capacity,
            updated_at = CURRENT_TIMESTAMP
        FROM inserted i
        WHERE t.target_key = i.target_key`,
		limit,
	)
	if err != nil {
		return 0, err
	}
	return commandTag.RowsAffected(), nil
}

func (s *Store) ClaimWorkerScaleCommand(
	ctx context.Context,
	driver string,
) (domain.WorkerScaleCommandExecution, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkerScaleCommandExecution{}, false, err
	}
	defer tx.Rollback(ctx)

	value, err := scanWorkerScaleCommandExecution(tx.QueryRow(ctx, `
        WITH selected AS (
            SELECT c.id
            FROM pipeline_worker_scale_command c
            JOIN pipeline_worker_target t ON t.target_key = c.target_key
            WHERE t.enabled = TRUE
              AND t.driver = $1
              AND (
                    c.status = 'REQUESTED'
                    OR (
                        c.status = 'RUNNING'
                        AND c.started_at < CURRENT_TIMESTAMP - interval '10 minutes'
                    )
              )
            ORDER BY c.requested_at, c.id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        ), updated AS (
            UPDATE pipeline_worker_scale_command c
            SET status = 'RUNNING',
                started_at = CURRENT_TIMESTAMP,
                error_message = NULL
            FROM selected s
            WHERE c.id = s.id
            RETURNING c.id, c.target_key, c.command_type, c.requested_capacity,
                c.previous_capacity, c.reason, c.triggered_by, c.status,
                c.planner_run_id, c.details, c.requested_at, c.started_at,
                c.completed_at, c.error_message
        )
        SELECT
            c.id, c.target_key, c.command_type, c.requested_capacity,
            c.previous_capacity, c.reason, c.triggered_by, c.status,
            c.planner_run_id, c.details, c.requested_at, c.started_at,
            c.completed_at, c.error_message,
            t.target_key, t.display_name, t.environment, t.driver, t.enabled,
            t.min_capacity, t.max_capacity, t.desired_capacity,
            t.scale_in_idle_seconds, t.config, t.created_at, t.updated_at
        FROM updated c
        JOIN pipeline_worker_target t ON t.target_key = c.target_key`,
		driver,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WorkerScaleCommandExecution{}, false, nil
	}
	if err != nil {
		return domain.WorkerScaleCommandExecution{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkerScaleCommandExecution{}, false, err
	}
	return value, true, nil
}

// CompleteWorkerScaleCommand는 actuator가 Docker/ASG 조작을 끝낸 뒤 결과를 기록한다.
func (s *Store) CompleteWorkerScaleCommand(ctx context.Context, id int64, status string, errorMessage *string) error {
	tag, err := s.pool.Exec(ctx, `
        UPDATE pipeline_worker_scale_command
        SET status = $2,
            completed_at = CURRENT_TIMESTAMP,
            error_message = $3
        WHERE id = $1
          AND status = 'RUNNING'`,
		id,
		status,
		errorMessage,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

// RunCollectionPlanner는 외부 API를 호출하지 않고 DB 상태만 보고 수집 필요성을 탐지한다.
// 실제 pipeline_job 생성은 다음 단계에서 연결하고, 지금은 판단 근거를 collection_planner_run에 남긴다.
func (s *Store) RunCollectionPlanner(ctx context.Context, req domain.RunCollectionPlannerRequest) (domain.CollectionPlannerRun, error) {
	source := strings.ToUpper(strings.TrimSpace(req.Source))
	if source == "" {
		return domain.CollectionPlannerRun{}, fmt.Errorf("source is required")
	}
	if len(req.Details) == 0 {
		req.Details = json.RawMessage(`{}`)
	}

	startedAt := time.Now().UTC()
	detectedCount, reason, details, err := s.detectCollectionCandidates(ctx, source)
	status := "NOOP"
	if detectedCount > 0 {
		status = "DETECTED"
	}
	if err != nil {
		status = "FAILED"
		reason = "planner query failed"
		details, _ = json.Marshal(map[string]any{"error": err.Error()})
	}

	policySnapshot, snapshotErr := s.policySnapshot(ctx, source)
	if snapshotErr != nil && err == nil {
		status = "FAILED"
		reason = "policy snapshot failed"
		details, _ = json.Marshal(map[string]any{"error": snapshotErr.Error()})
	}
	if len(policySnapshot) == 0 {
		policySnapshot = json.RawMessage(`{}`)
	}
	plannerType := strings.TrimSpace(req.PlannerType)
	if plannerType == "" {
		plannerType = defaultPlannerType(source)
	}
	var errorMessage *string
	if err != nil {
		message := err.Error()
		errorMessage = &message
	} else if snapshotErr != nil {
		message := snapshotErr.Error()
		errorMessage = &message
	}

	value, insertErr := scanPlannerRun(s.pool.QueryRow(ctx, `
        INSERT INTO collection_planner_run (
            source, planner_type, status, detected_count, planned_count,
            skipped_count, reason, policy_snapshot, details,
            started_at, completed_at, error_message
        ) VALUES (
            $1, $2, $3, $4, 0, 0, $5, $6, $7, $8, CURRENT_TIMESTAMP, $9
        )
        RETURNING `+plannerRunColumns,
		source,
		plannerType,
		status,
		detectedCount,
		reason,
		policySnapshot,
		details,
		startedAt,
		errorMessage,
	))
	if insertErr != nil {
		return domain.CollectionPlannerRun{}, insertErr
	}
	return value, nil
}

// ListCollectionPlannerRuns는 최근 detector 실행 결과를 source/status별로 조회한다.
func (s *Store) ListCollectionPlannerRuns(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.CollectionPlannerRun, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT `+plannerRunColumns+`
        FROM collection_planner_run
        WHERE ($1 = '' OR source = $1)
          AND ($2 = '' OR status = $2)
        ORDER BY started_at DESC, id DESC
        LIMIT $3 OFFSET $4`,
		filter.Source,
		filter.Status,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.CollectionPlannerRun, 0)
	for rows.Next() {
		value, err := scanPlannerRun(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (s *Store) detectCollectionCandidates(ctx context.Context, source string) (int, string, json.RawMessage, error) {
	switch source {
	case "KIS":
		return s.detectKISCandidates(ctx)
	case "BLS":
		return s.detectBLSCandidates(ctx)
	case "SEC":
		return s.detectSECCandidates(ctx)
	case "NEWS":
		details, _ := json.Marshal(map[string]any{"message": "news collector is not configured yet"})
		return 0, "NEWS planner is reserved for the next source", details, nil
	default:
		return 0, "", nil, fmt.Errorf("unsupported planner source: %s", source)
	}
}

func (s *Store) detectKISCandidates(ctx context.Context) (int, string, json.RawMessage, error) {
	var detectedCount, totalAssets int
	err := s.pool.QueryRow(ctx, `
        SELECT
            COUNT(*) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM price_bar pb
                    WHERE pb.asset_id = a.id
                      AND pb.source = 'KIS'
                      AND pb.interval = '1D'
                      AND pb.bar_time >= CURRENT_DATE::timestamptz
                      AND pb.bar_time < (CURRENT_DATE + 1)::timestamptz
                )
            ),
            COUNT(*)
        FROM asset a
        WHERE a.active = TRUE
          AND a.asset_type = 'EQUITY'`).Scan(&detectedCount, &totalAssets)
	if err != nil {
		return 0, "", nil, err
	}
	details, _ := json.Marshal(map[string]any{
		"totalAssets":    totalAssets,
		"targetDateRule": "no KIS 1D price_bar exists for CURRENT_DATE",
	})
	return detectedCount, "KIS asset has no current-date price_bar", details, nil
}

func (s *Store) detectBLSCandidates(ctx context.Context) (int, string, json.RawMessage, error) {
	var detectedCount, totalSeries int
	var targetPeriod string
	err := s.pool.QueryRow(ctx, `
        WITH series_latest AS (
            SELECT
                ms.id,
                MAX(mo.reference_period) AS latest_period,
                to_char(date_trunc('month', CURRENT_DATE) - interval '1 month', 'YYYY-MM') AS target_period
            FROM macro_series ms
            LEFT JOIN macro_observation mo ON mo.series_id = ms.id
            WHERE ms.provider = 'BLS'
            GROUP BY ms.id
        )
        SELECT
            COUNT(*) FILTER (
                WHERE latest_period IS NULL OR latest_period < target_period
            ),
            COUNT(*),
            MAX(target_period)
        FROM series_latest`).Scan(&detectedCount, &totalSeries, &targetPeriod)
	if err != nil {
		return 0, "", nil, err
	}
	details, _ := json.Marshal(map[string]any{
		"totalSeries":  totalSeries,
		"targetPeriod": targetPeriod,
		"targetRule":   "latest_reference_period < previous_month",
	})
	return detectedCount, "BLS series is missing the latest expected reference period", details, nil
}

func (s *Store) detectSECCandidates(ctx context.Context) (int, string, json.RawMessage, error) {
	var detectedCount, linkedCompanies int
	err := s.pool.QueryRow(ctx, `
        WITH linked_company AS (
            SELECT DISTINCT c.id
            FROM company c
            JOIN asset a ON a.company_id = c.id
            WHERE a.active = TRUE
        ), sec_state AS (
            SELECT company_id, next_check_at
            FROM company_collection_state
            WHERE provider = 'SEC'
              AND data_type = 'FINANCIAL'
        )
        SELECT
            COUNT(*) FILTER (
                WHERE ss.company_id IS NULL OR ss.next_check_at IS NULL OR ss.next_check_at <= CURRENT_TIMESTAMP
            ),
            COUNT(*)
        FROM linked_company lc
        LEFT JOIN sec_state ss ON ss.company_id = lc.id`).Scan(&detectedCount, &linkedCompanies)
	if err != nil {
		return 0, "", nil, err
	}
	details, _ := json.Marshal(map[string]any{
		"linkedCompanies": linkedCompanies,
		"targetRule":      "company_collection_state.next_check_at <= now or missing state",
	})
	return detectedCount, "SEC company interval check is due", details, nil
}

func (s *Store) policySnapshot(ctx context.Context, source string) (json.RawMessage, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT jsonb_build_object(
            'policyKey', policy_key,
            'targetKey', target_key,
            'source', source,
            'workerName', worker_name,
            'workerRole', worker_role,
            'enabled', enabled,
            'mode', mode,
            'topic', topic,
            'consumerGroup', consumer_group,
            'scaleOutLagThreshold', scale_out_lag_threshold,
            'scaleOutQueueThreshold', scale_out_queue_threshold,
            'batchSize', batch_size,
            'maxConcurrency', max_concurrency,
            'scaleInIdleSeconds', scale_in_idle_seconds,
            'allowedStartTime', to_char(allowed_start_time, 'HH24:MI'),
            'allowedEndTime', to_char(allowed_end_time, 'HH24:MI'),
            'checkIntervalHours', check_interval_hours,
            'priority', priority
        )
        FROM pipeline_worker_policy
        WHERE source = $1
        ORDER BY priority DESC`, source)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []json.RawMessage
	for rows.Next() {
		var value json.RawMessage
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return json.RawMessage(`[]`), nil
	}
	return json.Marshal(values)
}

func defaultPlannerType(source string) string {
	switch source {
	case "KIS":
		return "PRICE_INCREMENTAL_CHECK"
	case "BLS":
		return "MACRO_RELEASE_CHECK"
	case "SEC":
		return "SEC_INTERVAL_CHECK"
	case "NEWS":
		return "NEWS_RAW_CHECK"
	default:
		return "GENERIC_CHECK"
	}
}

func stringFromPointer(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func scanWorkerTarget(row rowScanner) (domain.WorkerTarget, error) {
	var value domain.WorkerTarget
	err := row.Scan(
		&value.TargetKey,
		&value.DisplayName,
		&value.Environment,
		&value.Driver,
		&value.Enabled,
		&value.MinCapacity,
		&value.MaxCapacity,
		&value.DesiredCapacity,
		&value.ScaleInIdleSeconds,
		&value.Config,
		&value.CreatedAt,
		&value.UpdatedAt,
	)
	return value, err
}

func scanWorkerPolicy(row rowScanner) (domain.WorkerPolicy, error) {
	var value domain.WorkerPolicy
	err := row.Scan(
		&value.ID,
		&value.PolicyKey,
		&value.TargetKey,
		&value.Source,
		&value.WorkerName,
		&value.WorkerRole,
		&value.Enabled,
		&value.Mode,
		&value.Topic,
		&value.ConsumerGroup,
		&value.ScaleOutLagThreshold,
		&value.ScaleOutQueueThreshold,
		&value.BatchSize,
		&value.MaxConcurrency,
		&value.ScaleInIdleSeconds,
		&value.AllowedStartTime,
		&value.AllowedEndTime,
		&value.CheckIntervalHours,
		&value.Priority,
		&value.Config,
		&value.CreatedAt,
		&value.UpdatedAt,
	)
	return value, err
}

func scanWorkerHeartbeat(row rowScanner) (domain.WorkerNodeHeartbeat, error) {
	var value domain.WorkerNodeHeartbeat
	err := row.Scan(
		&value.NodeName,
		&value.TargetKey,
		&value.Status,
		&value.Idle,
		&value.AcceptingNewWork,
		&value.RunningWorkers,
		&value.RunningJobItems,
		&value.ProcessingSourceObjects,
		&value.LastHeartbeatAt,
		&value.Metadata,
		&value.CreatedAt,
		&value.UpdatedAt,
	)
	return value, err
}

func scanPlannerRun(row rowScanner) (domain.CollectionPlannerRun, error) {
	var value domain.CollectionPlannerRun
	err := row.Scan(
		&value.ID,
		&value.Source,
		&value.PlannerType,
		&value.Status,
		&value.DetectedCount,
		&value.PlannedCount,
		&value.SkippedCount,
		&value.Reason,
		&value.PolicySnapshot,
		&value.Details,
		&value.PipelineJobID,
		&value.StartedAt,
		&value.CompletedAt,
		&value.ErrorMessage,
		&value.CreatedAt,
	)
	return value, err
}

func scanWorkerScaleCommand(row rowScanner) (domain.WorkerScaleCommand, error) {
	var value domain.WorkerScaleCommand
	err := row.Scan(
		&value.ID,
		&value.TargetKey,
		&value.CommandType,
		&value.RequestedCapacity,
		&value.PreviousCapacity,
		&value.Reason,
		&value.TriggeredBy,
		&value.Status,
		&value.PlannerRunID,
		&value.Details,
		&value.RequestedAt,
		&value.StartedAt,
		&value.CompletedAt,
		&value.ErrorMessage,
	)
	return value, err
}

func scanWorkerScaleCommandExecution(row rowScanner) (domain.WorkerScaleCommandExecution, error) {
	var value domain.WorkerScaleCommandExecution
	err := row.Scan(
		&value.Command.ID,
		&value.Command.TargetKey,
		&value.Command.CommandType,
		&value.Command.RequestedCapacity,
		&value.Command.PreviousCapacity,
		&value.Command.Reason,
		&value.Command.TriggeredBy,
		&value.Command.Status,
		&value.Command.PlannerRunID,
		&value.Command.Details,
		&value.Command.RequestedAt,
		&value.Command.StartedAt,
		&value.Command.CompletedAt,
		&value.Command.ErrorMessage,
		&value.Target.TargetKey,
		&value.Target.DisplayName,
		&value.Target.Environment,
		&value.Target.Driver,
		&value.Target.Enabled,
		&value.Target.MinCapacity,
		&value.Target.MaxCapacity,
		&value.Target.DesiredCapacity,
		&value.Target.ScaleInIdleSeconds,
		&value.Target.Config,
		&value.Target.CreatedAt,
		&value.Target.UpdatedAt,
	)
	return value, err
}
