package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
	"github.com/robfig/cron/v3"
)

type Service struct {
	store store.Store
	now   func() time.Time
}

// New는 HTTP·Scheduler가 공유할 Pipeline 애플리케이션 서비스를 만든다.
func New(st store.Store) *Service {
	return &Service{store: st, now: time.Now}
}

// Ping은 저장소의 연결 상태를 그대로 확인한다.
func (s *Service) Ping(ctx context.Context) error {
	return s.store.Ping(ctx)
}

// CreateAdminSession creates a browser admin session and returns the raw token for the HttpOnly cookie.
func (s *Service) CreateAdminSession(ctx context.Context, actor string, ipAddress *string, userAgent *string, ttl time.Duration) (string, domain.AdminSession, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "admin"
	}
	if ttl <= 0 {
		return "", domain.AdminSession{}, errors.New("admin session ttl must be positive")
	}
	token, err := newSecureToken(32)
	if err != nil {
		return "", domain.AdminSession{}, err
	}
	session, err := s.store.CreateAdminSession(ctx, domain.CreateAdminSessionRequest{
		SessionTokenHash: hashAdminSessionToken(token),
		Actor:            actor,
		IPAddress:        ipAddress,
		UserAgent:        userAgent,
		ExpiresAt:        s.now().Add(ttl),
	})
	if err != nil {
		return "", domain.AdminSession{}, err
	}
	return token, session, nil
}

// GetAdminSession validates a browser cookie token against the DB-backed session table.
func (s *Service) GetAdminSession(ctx context.Context, token string) (domain.AdminSession, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return domain.AdminSession{}, store.ErrNotFound
	}
	now := s.now()
	session, err := s.store.GetActiveAdminSession(ctx, hashAdminSessionToken(token), now)
	if err != nil {
		return domain.AdminSession{}, err
	}
	if err := s.store.TouchAdminSession(ctx, session.ID, now); err != nil {
		return domain.AdminSession{}, err
	}
	session.LastSeenAt = now
	return session, nil
}

// RevokeAdminSession invalidates a browser admin session during logout.
func (s *Service) RevokeAdminSession(ctx context.Context, token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	return s.store.RevokeAdminSession(ctx, hashAdminSessionToken(token), s.now())
}

// ListSchedules는 페이지 크기를 보정한 뒤 등록된 Schedule 목록을 조회한다.
func (s *Service) ListSchedules(ctx context.Context, filter domain.ListFilter) ([]domain.Schedule, error) {
	return s.store.ListSchedules(ctx, normalizeFilter(filter))
}

// GetSchedule은 관리자 상세 화면에 필요한 Schedule 한 건을 조회한다.
func (s *Service) GetSchedule(ctx context.Context, id int64) (domain.Schedule, error) {
	return s.store.GetSchedule(ctx, id)
}

// CreateSchedule은 Cron·timezone·JSON을 검증하고 첫 실행시각을 계산해 저장한다.
func (s *Service) CreateSchedule(ctx context.Context, req domain.CreateScheduleRequest) (domain.Schedule, error) {
	if err := validateSchedule(req.JobType, req.CronExpression, req.Timezone, req.Parameters); err != nil {
		return domain.Schedule{}, err
	}
	nextRun, err := nextCronTime(req.CronExpression, req.Timezone, s.now())
	if err != nil {
		return domain.Schedule{}, err
	}
	return s.store.CreateSchedule(ctx, req, nextRun)
}

// UpdateSchedule은 변경된 Cron을 검증하고 다음 실행시각도 함께 다시 계산한다.
func (s *Service) UpdateSchedule(ctx context.Context, id int64, req domain.UpdateScheduleRequest) (domain.Schedule, error) {
	if err := validateSchedule(req.JobType, req.CronExpression, req.Timezone, req.Parameters); err != nil {
		return domain.Schedule{}, err
	}
	nextRun, err := nextCronTime(req.CronExpression, req.Timezone, s.now())
	if err != nil {
		return domain.Schedule{}, err
	}
	return s.store.UpdateSchedule(ctx, id, req, nextRun)
}

// SetScheduleEnabled는 일정 활성화 시 next_run_at을 재계산하고 비활성화 시 제거한다.
func (s *Service) SetScheduleEnabled(ctx context.Context, id int64, enabled bool) (domain.Schedule, error) {
	if !enabled {
		return s.store.SetScheduleEnabled(ctx, id, false, nil)
	}
	schedule, err := s.store.GetSchedule(ctx, id)
	if err != nil {
		return domain.Schedule{}, err
	}
	nextRun, err := nextCronTime(schedule.CronExpression, schedule.Timezone, s.now())
	if err != nil {
		return domain.Schedule{}, err
	}
	return s.store.SetScheduleEnabled(ctx, id, true, &nextRun)
}

// CreateJob은 수동 Job 요청을 검증하고 필요하면 멱등 키와 요청자를 자동 지정한다.
// DeleteSchedule은 운영 실수를 줄이기 위해 store에서 비활성 schedule만 삭제하도록 위임한다.
func (s *Service) DeleteSchedule(ctx context.Context, id int64) error {
	return s.store.DeleteSchedule(ctx, id)
}

func (s *Service) CreateJob(ctx context.Context, req domain.CreateJobRequest) (domain.JobDetail, error) {
	if err := validateJobRequest(req.JobType, req.Source, req.ItemKeys, req.Parameters); err != nil {
		return domain.JobDetail{}, err
	}
	if req.RequestedBy == "" {
		req.RequestedBy = "ADMIN"
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = newIdempotencyKey("manual")
	}
	return s.store.CreateJob(ctx, req)
}

// ListJobs는 상태와 페이지 조건에 맞는 Job 목록을 조회한다.
func (s *Service) ListJobs(ctx context.Context, filter domain.ListFilter) ([]domain.Job, error) {
	return s.store.ListJobs(ctx, normalizeFilter(filter))
}

// GetJob은 Job과 하위 Item을 함께 조회해 상세 화면 모델을 반환한다.
func (s *Service) GetJob(ctx context.Context, id int64) (domain.JobDetail, error) {
	return s.store.GetJob(ctx, id)
}

// RetryJob은 실패한 Item만 포함하는 새로운 재시도 Job 생성을 요청한다.
func (s *Service) RetryJob(ctx context.Context, id int64, requestedBy string) (domain.JobDetail, error) {
	if requestedBy == "" {
		requestedBy = "ADMIN"
	}
	return s.store.RetryJob(ctx, id, requestedBy, newIdempotencyKey(fmt.Sprintf("retry-job-%d", id)))
}

// CancelJob은 대기 Job을 취소하거나 실행 중 Job에 취소 요청 상태를 기록한다.
func (s *Service) CancelJob(ctx context.Context, id int64) (domain.Job, error) {
	return s.store.CancelJob(ctx, id)
}

// ListItems는 특정 Job의 세부 처리 단위를 상태·페이지 조건으로 조회한다.
func (s *Service) ListItems(ctx context.Context, jobID int64, filter domain.ListFilter) ([]domain.JobItem, error) {
	return s.store.ListItems(ctx, jobID, normalizeFilter(filter))
}

// RetryItem은 실패·취소된 Item을 Worker가 다시 가져갈 수 있도록 QUEUED로 되돌린다.
func (s *Service) RetryItem(ctx context.Context, id int64) (domain.JobItem, error) {
	return s.store.RetryItem(ctx, id)
}

// ListOutbox는 Kafka 전달 상태를 확인할 Outbox 이벤트 목록을 조회한다.
func (s *Service) ListOutbox(ctx context.Context, filter domain.ListFilter) ([]domain.OutboxEvent, error) {
	return s.store.ListOutbox(ctx, normalizeFilter(filter))
}

// RetryOutbox는 DEAD·RETRY_WAIT 이벤트를 Relay가 재발행할 PENDING으로 전환한다.
// MarkRawObjectReady is called after a collector writes raw data and records source_object.
// The controller owns topic routing, idempotency keys, and event payload shape.
func (s *Service) MarkRawObjectReady(ctx context.Context, sourceObjectID int64) (domain.OutboxEvent, error) {
	if sourceObjectID <= 0 {
		return domain.OutboxEvent{}, errors.New("sourceObjectId must be positive")
	}
	return s.store.InsertRawObjectReadyOutbox(ctx, sourceObjectID)
}

func (s *Service) RetryOutbox(ctx context.Context, id int64) (domain.OutboxEvent, error) {
	return s.store.RetryOutbox(ctx, id)
}

// ListProcessedEvents는 Kafka Consumer가 처리한 이벤트 이력을 최신순으로 조회한다.
func (s *Service) ListProcessedEvents(ctx context.Context, filter domain.ListFilter) ([]domain.ProcessedEvent, error) {
	return s.store.ListProcessedEvents(ctx, normalizeFilter(filter))
}

// ListWorkers는 controller 내부 장기 실행 worker의 desired/runtime 상태를 조회한다.
func (s *Service) ListWorkers(ctx context.Context) ([]domain.ManagedWorker, error) {
	return s.store.ListWorkers(ctx)
}

// StartWorker는 worker의 desired_state를 ENABLED로 바꿔 manager가 계속 실행하게 한다.
func (s *Service) StartWorker(ctx context.Context, name string) (domain.ManagedWorker, error) {
	return s.store.SetWorkerDesiredState(ctx, strings.TrimSpace(name), "ENABLED")
}

// StopWorker는 worker의 desired_state를 DISABLED로 바꿔 manager가 context cancel로 멈추게 한다.
func (s *Service) StopWorker(ctx context.Context, name string) (domain.ManagedWorker, error) {
	return s.store.SetWorkerDesiredState(ctx, strings.TrimSpace(name), "DISABLED")
}

// RunWorkerOnce는 worker를 상시 실행하지 않고 다음 manager tick에서 1회만 실행하도록 요청한다.
func (s *Service) RunWorkerOnce(ctx context.Context, name string) (domain.ManagedWorker, error) {
	return s.store.RequestWorkerRunOnce(ctx, strings.TrimSpace(name))
}

// ListWorkerTargets는 외부 Python worker node를 켜고 끄는 대상 목록을 조회한다.
func (s *Service) ListWorkerTargets(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerTarget, error) {
	return s.store.ListWorkerTargets(ctx, normalizeWorkerOpsFilter(filter))
}

// ListWorkerPolicies는 KIS/BLS/SEC/NEWS별 worker 실행 정책을 조회한다.
// UpdateWorkerTarget은 외부 worker node를 로컬 Docker 또는 AWS ASG 중 어떤 방식으로 조작할지 바꾼다.
func (s *Service) UpdateWorkerTarget(ctx context.Context, targetKey string, req domain.UpdateWorkerTargetRequest) (domain.WorkerTarget, error) {
	targetKey = strings.TrimSpace(targetKey)
	if targetKey == "" {
		return domain.WorkerTarget{}, errors.New("targetKey is required")
	}
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	req.Environment = strings.ToUpper(strings.TrimSpace(req.Environment))
	req.Driver = strings.ToUpper(strings.TrimSpace(req.Driver))
	if req.DisplayName == "" {
		return domain.WorkerTarget{}, errors.New("displayName is required")
	}
	if req.Environment != "LOCAL" && req.Environment != "AWS" {
		return domain.WorkerTarget{}, errors.New("environment must be LOCAL or AWS")
	}
	if req.Driver != "LOCAL_DOCKER" && req.Driver != "AWS_ASG" {
		return domain.WorkerTarget{}, errors.New("driver must be LOCAL_DOCKER or AWS_ASG")
	}
	if req.Environment == "LOCAL" && req.Driver != "LOCAL_DOCKER" {
		return domain.WorkerTarget{}, errors.New("LOCAL environment requires LOCAL_DOCKER driver")
	}
	if req.Environment == "AWS" && req.Driver != "AWS_ASG" {
		return domain.WorkerTarget{}, errors.New("AWS environment requires AWS_ASG driver")
	}
	if req.MinCapacity < 0 || req.MaxCapacity < req.MinCapacity {
		return domain.WorkerTarget{}, errors.New("capacity range is invalid")
	}
	if req.DesiredCapacity < req.MinCapacity || req.DesiredCapacity > req.MaxCapacity {
		return domain.WorkerTarget{}, errors.New("desiredCapacity must be inside min/max capacity")
	}
	if req.ScaleInIdleSeconds < 0 {
		return domain.WorkerTarget{}, errors.New("scaleInIdleSeconds must be zero or positive")
	}
	if len(req.Config) == 0 {
		req.Config = json.RawMessage(`{}`)
	}
	if !json.Valid(req.Config) {
		return domain.WorkerTarget{}, errors.New("config must be valid JSON")
	}
	return s.store.UpdateWorkerTarget(ctx, targetKey, req)
}

func (s *Service) ListWorkerPolicies(ctx context.Context, filter domain.WorkerPolicyFilter) ([]domain.WorkerPolicy, error) {
	filter.TargetKey = strings.TrimSpace(filter.TargetKey)
	filter.Source = strings.ToUpper(strings.TrimSpace(filter.Source))
	normalized := normalizeFilter(domain.ListFilter{Limit: filter.Limit, Offset: filter.Offset})
	filter.Limit = normalized.Limit
	filter.Offset = normalized.Offset
	return s.store.ListWorkerPolicies(ctx, filter)
}

// GetWorkerAgentPolicies는 Python worker-agent가 부팅/refresh 시점에 읽는 정책 묶음이다.
// UpdateWorkerPolicy는 lag threshold, batch size, 실행 허용 시간 같은 운영 정책을 수정한다.
// 이 값은 controller detector와 Python worker-agent가 함께 읽는 source of truth다.
func (s *Service) UpdateWorkerPolicy(ctx context.Context, id int64, req domain.UpdateWorkerPolicyRequest) (domain.WorkerPolicy, error) {
	if id <= 0 {
		return domain.WorkerPolicy{}, errors.New("policy id must be positive")
	}
	if req.ScaleOutLagThreshold < 0 || req.ScaleOutQueueThreshold < 0 {
		return domain.WorkerPolicy{}, errors.New("threshold values must be zero or positive")
	}
	if req.BatchSize <= 0 {
		return domain.WorkerPolicy{}, errors.New("batchSize must be positive")
	}
	if req.MaxConcurrency <= 0 {
		return domain.WorkerPolicy{}, errors.New("maxConcurrency must be positive")
	}
	if req.ScaleInIdleSeconds != nil && *req.ScaleInIdleSeconds < 0 {
		return domain.WorkerPolicy{}, errors.New("scaleInIdleSeconds must be zero or positive")
	}
	if req.CheckIntervalHours != nil && *req.CheckIntervalHours <= 0 {
		return domain.WorkerPolicy{}, errors.New("checkIntervalHours must be positive")
	}
	if err := validateOptionalClock(req.AllowedStartTime, "allowedStartTime"); err != nil {
		return domain.WorkerPolicy{}, err
	}
	if err := validateOptionalClock(req.AllowedEndTime, "allowedEndTime"); err != nil {
		return domain.WorkerPolicy{}, err
	}
	if len(req.Config) == 0 {
		req.Config = json.RawMessage(`{}`)
	}
	if !json.Valid(req.Config) {
		return domain.WorkerPolicy{}, errors.New("config must be valid JSON")
	}
	return s.store.UpdateWorkerPolicy(ctx, id, req)
}

func (s *Service) GetWorkerAgentPolicies(ctx context.Context, targetKey string) (domain.WorkerAgentPolicies, error) {
	targetKey = strings.TrimSpace(targetKey)
	if targetKey == "" {
		return domain.WorkerAgentPolicies{}, errors.New("targetKey is required")
	}
	return s.store.GetWorkerAgentPolicies(ctx, targetKey)
}

// UpsertWorkerHeartbeat는 Python worker node가 보내는 heartbeat를 저장한다.
func (s *Service) UpsertWorkerHeartbeat(ctx context.Context, req domain.WorkerHeartbeatRequest) (domain.WorkerNodeHeartbeat, error) {
	req.NodeName = strings.TrimSpace(req.NodeName)
	req.TargetKey = strings.TrimSpace(req.TargetKey)
	req.Status = strings.ToUpper(strings.TrimSpace(req.Status))
	if req.NodeName == "" || req.TargetKey == "" {
		return domain.WorkerNodeHeartbeat{}, errors.New("nodeName and targetKey are required")
	}
	if req.Status == "" {
		req.Status = "RUNNING"
	}
	if req.RunningJobItems < 0 || req.ProcessingSourceObjects < 0 {
		return domain.WorkerNodeHeartbeat{}, errors.New("running counts must be zero or positive")
	}
	if len(req.RunningWorkers) == 0 {
		req.RunningWorkers = json.RawMessage(`[]`)
	}
	if !json.Valid(req.RunningWorkers) {
		return domain.WorkerNodeHeartbeat{}, errors.New("runningWorkers must be valid JSON")
	}
	if len(req.Metadata) == 0 {
		req.Metadata = json.RawMessage(`{}`)
	}
	if !json.Valid(req.Metadata) {
		return domain.WorkerNodeHeartbeat{}, errors.New("metadata must be valid JSON")
	}
	return s.store.UpsertWorkerHeartbeat(ctx, req)
}

// ListWorkerHeartbeats는 최근 Python worker node heartbeat를 조회한다.
func (s *Service) ListWorkerHeartbeats(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerNodeHeartbeat, error) {
	return s.store.ListWorkerHeartbeats(ctx, normalizeWorkerOpsFilter(filter))
}

// CreateWorkerScaleCommand는 target desired capacity를 바꾸고 scale command 이력을 남긴다.
func (s *Service) CreateWorkerScaleCommand(
	ctx context.Context,
	targetKey string,
	req domain.CreateWorkerScaleCommandRequest,
) (domain.WorkerScaleCommand, error) {
	targetKey = strings.TrimSpace(targetKey)
	if targetKey == "" {
		return domain.WorkerScaleCommand{}, errors.New("targetKey is required")
	}
	if req.RequestedCapacity < 0 {
		return domain.WorkerScaleCommand{}, errors.New("requestedCapacity must be zero or positive")
	}
	req.Reason = strings.ToUpper(strings.TrimSpace(req.Reason))
	if req.Reason == "" {
		req.Reason = "MANUAL_REQUEST"
	}
	req.TriggeredBy = strings.ToUpper(strings.TrimSpace(req.TriggeredBy))
	if req.TriggeredBy == "" {
		req.TriggeredBy = "MANUAL"
	}
	if len(req.Details) == 0 {
		req.Details = json.RawMessage(`{}`)
	}
	if !json.Valid(req.Details) {
		return domain.WorkerScaleCommand{}, errors.New("details must be valid JSON")
	}
	return s.store.CreateWorkerScaleCommand(ctx, targetKey, req)
}

// ListWorkerScaleCommands는 scale-out/in 요청 이력을 조회한다.
func (s *Service) ListWorkerScaleCommands(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerScaleCommand, error) {
	return s.store.ListWorkerScaleCommands(ctx, normalizeWorkerOpsFilter(filter))
}

// RunCollectionPlanner는 외부 API 호출 없이 DB 기준으로 수집 필요성을 탐지한다.
func (s *Service) RunCollectionPlanner(ctx context.Context, req domain.RunCollectionPlannerRequest) (domain.CollectionPlannerRun, error) {
	req.Source = strings.ToUpper(strings.TrimSpace(req.Source))
	if req.Source == "" {
		return domain.CollectionPlannerRun{}, errors.New("source is required")
	}
	req.PlannerType = strings.TrimSpace(req.PlannerType)
	if len(req.Details) == 0 {
		req.Details = json.RawMessage(`{}`)
	}
	if !json.Valid(req.Details) {
		return domain.CollectionPlannerRun{}, errors.New("details must be valid JSON")
	}
	return s.store.RunCollectionPlanner(ctx, req)
}

// ListCollectionPlannerRuns는 최근 detector 판단 결과를 조회한다.
func (s *Service) ListCollectionPlannerRuns(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.CollectionPlannerRun, error) {
	return s.store.ListCollectionPlannerRuns(ctx, normalizeWorkerOpsFilter(filter))
}

// ListCoverage는 데이터셋별 완전성 검사 결과를 조회한다.
// GetKafkaLagSummary는 Kafka consumer lag의 최신 상태와 최근 히스토리를 반환한다.
func (s *Service) GetKafkaLagSummary(ctx context.Context, limit int) (domain.KafkaLagSummary, error) {
	return s.store.GetKafkaLagSummary(ctx, normalizeFilter(domain.ListFilter{Limit: limit}).Limit)
}

func (s *Service) ListCoverage(ctx context.Context, filter domain.ListFilter) ([]domain.Coverage, error) {
	return s.store.ListCoverage(ctx, normalizeFilter(filter))
}

// ListGaps는 Coverage 검사에서 발견한 실제 누락 목록을 조회한다.
func (s *Service) ListGaps(ctx context.Context, filter domain.ListFilter) ([]domain.Gap, error) {
	return s.store.ListGaps(ctx, normalizeFilter(filter))
}

// BackfillGap은 OPEN Gap을 해결할 새 Job을 만들고 두 레코드를 연결한다.
func (s *Service) BackfillGap(ctx context.Context, id int64, req domain.BackfillGapRequest) (domain.JobDetail, error) {
	if err := validateJobRequest(req.JobType, req.Source, req.ItemKeys, req.Parameters); err != nil {
		return domain.JobDetail{}, err
	}
	if req.RequestedBy == "" {
		req.RequestedBy = "ADMIN"
	}
	return s.store.BackfillGap(ctx, id, req, newIdempotencyKey(fmt.Sprintf("gap-%d", id)))
}

// GetSummary는 관리자 Overview 카드에 사용할 상태별 집계를 반환한다.
func (s *Service) GetSummary(ctx context.Context) (domain.Summary, error) {
	return s.store.GetSummary(ctx)
}

// ListMacroSeries는 관리자 화면에서 선택 가능한 매크로 지표 카탈로그를 조회한다.
func (s *Service) ListMacroSeries(ctx context.Context, provider string, filter domain.ListFilter) ([]domain.MacroSeries, error) {
	provider = strings.ToUpper(strings.TrimSpace(provider))
	if provider == "" {
		provider = "BLS"
	}
	return s.store.ListMacroSeries(ctx, provider, normalizeFilter(filter))
}

// GetBLSInventory는 BLS 수집량과 연도별 coverage를 Data Inventory 화면에 제공한다.
func (s *Service) GetBLSInventory(ctx context.Context) (domain.BLSInventory, error) {
	return s.store.GetBLSInventory(ctx)
}

// GetAssetCompanyLinkSummary는 Company 연결·미연결과 후보 상태 집계를 반환한다.
func (s *Service) GetAssetCompanyLinkSummary(ctx context.Context) (domain.AssetCompanyLinkSummary, error) {
	return s.store.GetAssetCompanyLinkSummary(ctx)
}

// ListAssetCompanyMatches는 관리자 필터를 검증한 뒤 Company 연결 후보를 조회한다.
func (s *Service) ListAssets(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.AssetOption, error) {
	filter, err := normalizeAssetLinkFilter(filter)
	if err != nil {
		return nil, err
	}
	return s.store.ListAssets(ctx, filter)
}

func (s *Service) ListAssetCompanyMatches(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.AssetCompanyMatch, error) {
	filter, err := normalizeAssetLinkFilter(filter)
	if err != nil {
		return nil, err
	}
	return s.store.ListAssetCompanyMatches(ctx, filter)
}

// ListUnmatchedAssets는 아직 Company가 확정되지 않은 활성 Asset을 조회한다.
func (s *Service) ListUnmatchedAssets(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.UnmatchedAsset, error) {
	filter, err := normalizeAssetLinkFilter(filter)
	if err != nil {
		return nil, err
	}
	return s.store.ListUnmatchedAssets(ctx, filter)
}

// ConfirmAssetCompanyMatch는 검토자를 보정하고 후보 확정 트랜잭션을 요청한다.
func (s *Service) ConfirmAssetCompanyMatch(ctx context.Context, id int64, reviewedBy string) (domain.AssetCompanyMatch, error) {
	reviewedBy = normalizeReviewer(reviewedBy)
	return s.store.ConfirmAssetCompanyMatch(ctx, id, reviewedBy)
}

// RejectAssetCompanyMatch는 검토자를 보정하고 미확정 후보 거절을 요청한다.
func (s *Service) RejectAssetCompanyMatch(ctx context.Context, id int64, reviewedBy string) (domain.AssetCompanyMatch, error) {
	reviewedBy = normalizeReviewer(reviewedBy)
	return s.store.RejectAssetCompanyMatch(ctx, id, reviewedBy)
}

// GetPriceQualitySummary는 차트 표시 가능성과 가격 품질 이슈 집계를 반환한다.
func (s *Service) GetPriceQualitySummary(ctx context.Context) (domain.PriceQualitySummary, error) {
	return s.store.GetPriceQualitySummary(ctx)
}

// ListPriceQualityIssues는 관리자 필터를 검증한 뒤 가격 품질 이슈 목록을 조회한다.
func (s *Service) ListPriceQualityIssues(ctx context.Context, filter domain.PriceQualityIssueFilter) ([]domain.PriceQualityIssue, error) {
	filter, err := normalizePriceQualityIssueFilter(filter)
	if err != nil {
		return nil, err
	}
	return s.store.ListPriceQualityIssues(ctx, filter)
}

// RunDueSchedules는 한 tick에서 예약 Job 활성화, Schedule 실행, Job 상태 집계를 수행한다.
func (s *Service) GetPriceBarInventory(ctx context.Context, filter domain.AssetLinkFilter) (domain.PriceBarInventory, error) {
	filter, err := normalizeAssetLinkFilter(filter)
	if err != nil {
		return domain.PriceBarInventory{}, err
	}
	return s.store.GetPriceBarInventory(ctx, filter)
}

func (s *Service) RunDueSchedules(ctx context.Context, limit int) (int, error) {
	now := s.now().UTC()
	activated, activationErr := s.store.ActivateDueJobs(ctx, now, limit)
	schedules, err := s.store.ListDueSchedules(ctx, now, limit)
	if err != nil {
		return int(activated), errors.Join(activationErr, err)
	}

	triggered := int(activated)
	var failures []error
	if activationErr != nil {
		failures = append(failures, activationErr)
	}
	for _, schedule := range schedules {
		if schedule.NextRunAt == nil {
			continue
		}
		var params domain.ScheduleParameters
		if err := json.Unmarshal(schedule.Parameters, &params); err != nil {
			failures = append(failures, fmt.Errorf("schedule %d parameters: %w", schedule.ID, err))
			continue
		}
		if params.Source == "" || len(params.ItemKeys) == 0 {
			failures = append(failures, fmt.Errorf("schedule %d requires parameters.source and parameters.itemKeys", schedule.ID))
			continue
		}
		nextRun, err := nextCronTime(schedule.CronExpression, schedule.Timezone, *schedule.NextRunAt)
		if err != nil {
			failures = append(failures, fmt.Errorf("schedule %d next run: %w", schedule.ID, err))
			continue
		}
		_, err = s.store.TriggerSchedule(ctx, schedule, params.Source, params.ItemKeys, *schedule.NextRunAt, nextRun)
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("schedule %d trigger: %w", schedule.ID, err))
			continue
		}
		triggered++
	}
	if _, err := s.store.AggregateJobStatuses(ctx, limit*10); err != nil {
		failures = append(failures, fmt.Errorf("aggregate job statuses: %w", err))
	}
	return triggered, errors.Join(failures...)
}

// validateSchedule은 Schedule 생성 전에 필수값과 Cron·timezone·JSON 형식을 확인한다.
func validateSchedule(jobType, expression, timezone string, parameters json.RawMessage) error {
	if strings.TrimSpace(jobType) == "" {
		return errors.New("jobType is required")
	}
	if !json.Valid(normalizeJSON(parameters)) {
		return errors.New("parameters must be valid JSON")
	}
	_, err := nextCronTime(expression, timezone, time.Now())
	return err
}

// validateJobRequest는 Job 필수값, Item 중복, parameters JSON 형식을 확인한다.
func validateJobRequest(jobType, source string, itemKeys []string, parameters json.RawMessage) error {
	if strings.TrimSpace(jobType) == "" || strings.TrimSpace(source) == "" {
		return errors.New("jobType and source are required")
	}
	if len(itemKeys) == 0 {
		return errors.New("at least one itemKey is required")
	}
	seen := make(map[string]struct{}, len(itemKeys))
	for _, itemKey := range itemKeys {
		itemKey = strings.TrimSpace(itemKey)
		if itemKey == "" {
			return errors.New("itemKeys cannot contain an empty value")
		}
		if _, exists := seen[itemKey]; exists {
			return fmt.Errorf("duplicate itemKey: %s", itemKey)
		}
		seen[itemKey] = struct{}{}
	}
	if !json.Valid(normalizeJSON(parameters)) {
		return errors.New("parameters must be valid JSON")
	}
	return nil
}

// nextCronTime은 Schedule timezone에서 다음 Cron 실행시각을 구해 UTC로 반환한다.
func nextCronTime(expression, timezone string, after time.Time) (time.Time, error) {
	if strings.TrimSpace(expression) == "" {
		return time.Time{}, errors.New("cronExpression is required")
	}
	if strings.TrimSpace(timezone) == "" {
		return time.Time{}, errors.New("timezone is required")
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timezone: %w", err)
	}
	schedule, err := cron.ParseStandard(expression)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cronExpression: %w", err)
	}
	return schedule.Next(after.In(location)).UTC(), nil
}

// normalizeFilter는 목록 API의 기본 limit과 최대 limit을 강제한다.
func normalizeFilter(filter domain.ListFilter) domain.ListFilter {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 200 {
		filter.Limit = 200
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	return filter
}

func normalizeWorkerOpsFilter(filter domain.WorkerOpsFilter) domain.WorkerOpsFilter {
	filter.TargetKey = strings.TrimSpace(filter.TargetKey)
	filter.Source = strings.ToUpper(strings.TrimSpace(filter.Source))
	filter.Status = strings.ToUpper(strings.TrimSpace(filter.Status))
	normalized := normalizeFilter(domain.ListFilter{Limit: filter.Limit, Offset: filter.Offset})
	filter.Limit = normalized.Limit
	filter.Offset = normalized.Offset
	return filter
}

// normalizeAssetLinkFilter는 연결 API 필터의 대문자 코드와 페이지 범위를 검증한다.
func validateOptionalClock(value *string, fieldName string) error {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	if _, err := time.Parse("15:04", strings.TrimSpace(*value)); err != nil {
		return fmt.Errorf("%s must use HH:MM format", fieldName)
	}
	return nil
}

func normalizeAssetLinkFilter(filter domain.AssetLinkFilter) (domain.AssetLinkFilter, error) {
	filter.Status = strings.ToUpper(strings.TrimSpace(filter.Status))
	filter.AssetType = strings.ToUpper(strings.TrimSpace(filter.AssetType))
	filter.Exchange = strings.ToUpper(strings.TrimSpace(filter.Exchange))
	filter.Query = strings.TrimSpace(filter.Query)
	filter.Limit = normalizeFilter(domain.ListFilter{
		Limit: filter.Limit, Offset: filter.Offset,
	}).Limit
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	allowedStatuses := map[string]bool{
		"": true, "CANDIDATE": true, "CONFIRMED": true,
		"REVIEW_REQUIRED": true, "REJECTED": true,
	}
	if !allowedStatuses[filter.Status] {
		return domain.AssetLinkFilter{}, fmt.Errorf("invalid match status: %s", filter.Status)
	}
	return filter, nil
}

// normalizePriceQualityIssueFilter는 품질 이슈 API의 enum 필터와 페이지 범위를 검증한다.
func normalizePriceQualityIssueFilter(filter domain.PriceQualityIssueFilter) (domain.PriceQualityIssueFilter, error) {
	filter.Status = strings.ToUpper(strings.TrimSpace(filter.Status))
	filter.Severity = strings.ToUpper(strings.TrimSpace(filter.Severity))
	filter.Query = strings.TrimSpace(filter.Query)
	normalized := normalizeFilter(domain.ListFilter{Limit: filter.Limit, Offset: filter.Offset})
	filter.Limit = normalized.Limit
	filter.Offset = normalized.Offset

	allowedStatuses := map[string]bool{"": true, "OPEN": true, "RESOLVED": true, "IGNORED": true}
	if !allowedStatuses[filter.Status] {
		return domain.PriceQualityIssueFilter{}, fmt.Errorf("invalid issue status: %s", filter.Status)
	}
	allowedSeverities := map[string]bool{"": true, "INFO": true, "WARNING": true, "ERROR": true}
	if !allowedSeverities[filter.Severity] {
		return domain.PriceQualityIssueFilter{}, fmt.Errorf("invalid issue severity: %s", filter.Severity)
	}
	return filter, nil
}

// normalizeReviewer는 빈 관리자 식별자를 기본 운영 주체로 대체한다.
func normalizeReviewer(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "ADMIN"
	}
	return value
}

// normalizeJSON은 생략된 JSON 필드를 PostgreSQL에 저장 가능한 빈 객체로 바꾼다.
func normalizeJSON(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage(`{}`)
	}
	return value
}

// newIdempotencyKey는 수동·재시도 요청을 중복 생성하지 않을 무작위 키를 만든다.
func newIdempotencyKey(prefix string) string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%s:%d", prefix, time.Now().UnixNano())
	}
	return prefix + ":" + hex.EncodeToString(value[:])
}

func newSecureToken(size int) (string, error) {
	if size <= 0 {
		size = 32
	}
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func hashAdminSessionToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
