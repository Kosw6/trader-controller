package store

import (
	"context"
	"errors"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
)

var (
	ErrValidation   = errors.New("invalid request")
	ErrNotFound     = errors.New("resource not found")
	ErrConflict     = errors.New("resource already exists")
	ErrInvalidState = errors.New("resource state does not allow this operation")
)

type Store interface {
	ListAssetAliases(context.Context, domain.AssetLinkFilter) ([]domain.AssetAliases, error)
	SaveAssetAlias(context.Context, int64, int64, string, string) (domain.AssetAlias, error)
	DeleteAssetAlias(context.Context, int64, int64) error
	Ping(ctx context.Context) error
	CreateAdminSession(ctx context.Context, req domain.CreateAdminSessionRequest) (domain.AdminSession, error)
	GetActiveAdminSession(ctx context.Context, tokenHash string, now time.Time) (domain.AdminSession, error)
	TouchAdminSession(ctx context.Context, id int64, seenAt time.Time) error
	RevokeAdminSession(ctx context.Context, tokenHash string, revokedAt time.Time) error

	ListSchedules(ctx context.Context, filter domain.ListFilter) ([]domain.Schedule, error)
	GetSchedule(ctx context.Context, id int64) (domain.Schedule, error)
	CreateSchedule(ctx context.Context, req domain.CreateScheduleRequest, nextRun time.Time) (domain.Schedule, error)
	UpdateSchedule(ctx context.Context, id int64, req domain.UpdateScheduleRequest, nextRun time.Time) (domain.Schedule, error)
	SetScheduleEnabled(ctx context.Context, id int64, enabled bool, nextRun *time.Time) (domain.Schedule, error)
	DeleteSchedule(ctx context.Context, id int64) error
	ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]domain.Schedule, error)
	TriggerSchedule(ctx context.Context, schedule domain.Schedule, source string, itemKeys []string, scheduledFor, nextRun time.Time) (domain.Job, error)

	CreateJob(ctx context.Context, req domain.CreateJobRequest) (domain.JobDetail, error)
	ListJobs(ctx context.Context, filter domain.ListFilter) ([]domain.Job, error)
	GetJob(ctx context.Context, id int64) (domain.JobDetail, error)
	ActivateDueJobs(ctx context.Context, now time.Time, limit int) (int64, error)
	AggregateJobStatuses(ctx context.Context, limit int) (int64, error)
	RetryJob(ctx context.Context, id int64, requestedBy, idempotencyKey string) (domain.JobDetail, error)
	CancelJob(ctx context.Context, id int64) (domain.Job, error)
	ListItems(ctx context.Context, jobID int64, filter domain.ListFilter) ([]domain.JobItem, error)
	RetryItem(ctx context.Context, id int64) (domain.JobItem, error)

	ListOutbox(ctx context.Context, filter domain.ListFilter) ([]domain.OutboxEvent, error)
	InsertRawObjectReadyOutbox(ctx context.Context, sourceObjectID int64) (domain.OutboxEvent, error)
	RetryOutbox(ctx context.Context, id int64) (domain.OutboxEvent, error)
	ListProcessedEvents(ctx context.Context, filter domain.ListFilter) ([]domain.ProcessedEvent, error)
	ListSourceObjects(ctx context.Context, filter domain.ListFilter) ([]domain.SourceObjectStatus, error)
	ListWorkers(ctx context.Context) ([]domain.ManagedWorker, error)
	SetWorkerDesiredState(ctx context.Context, name, desiredState string) (domain.ManagedWorker, error)
	RequestWorkerRunOnce(ctx context.Context, name string) (domain.ManagedWorker, error)
	ListWorkerTargets(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerTarget, error)
	UpdateWorkerTarget(ctx context.Context, targetKey string, req domain.UpdateWorkerTargetRequest) (domain.WorkerTarget, error)
	ListWorkerPolicies(ctx context.Context, filter domain.WorkerPolicyFilter) ([]domain.WorkerPolicy, error)
	UpdateWorkerPolicy(ctx context.Context, id int64, req domain.UpdateWorkerPolicyRequest) (domain.WorkerPolicy, error)
	GetWorkerAgentPolicies(ctx context.Context, targetKey string) (domain.WorkerAgentPolicies, error)
	UpsertWorkerHeartbeat(ctx context.Context, req domain.WorkerHeartbeatRequest) (domain.WorkerNodeHeartbeat, error)
	ListWorkerHeartbeats(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerNodeHeartbeat, error)
	CreateWorkerScaleCommand(ctx context.Context, targetKey string, req domain.CreateWorkerScaleCommandRequest) (domain.WorkerScaleCommand, error)
	ListWorkerScaleCommands(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerScaleCommand, error)
	CreateLagTriggeredScaleCommands(ctx context.Context, limit int) (int64, error)
	CreateIdleScaleInCommands(ctx context.Context, limit int) (int64, error)
	RunCollectionPlanner(ctx context.Context, req domain.RunCollectionPlannerRequest) (domain.CollectionPlannerRun, error)
	ListCollectionPlannerRuns(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.CollectionPlannerRun, error)
	RecordKafkaLagSnapshot(ctx context.Context, snapshot domain.KafkaLagSnapshot) error
	GetKafkaLagSummary(ctx context.Context, limit int) (domain.KafkaLagSummary, error)
	ListCoverage(ctx context.Context, filter domain.ListFilter) ([]domain.Coverage, error)
	ListGaps(ctx context.Context, filter domain.ListFilter) ([]domain.Gap, error)
	BackfillGap(ctx context.Context, id int64, req domain.BackfillGapRequest, idempotencyKey string) (domain.JobDetail, error)
	GetSummary(ctx context.Context) (domain.Summary, error)
	ListMacroSeries(ctx context.Context, provider string, filter domain.ListFilter) ([]domain.MacroSeries, error)
	GetBLSInventory(ctx context.Context) (domain.BLSInventory, error)

	GetAssetCompanyLinkSummary(ctx context.Context) (domain.AssetCompanyLinkSummary, error)
	ListAssets(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.AssetOption, error)
	ListAssetCompanyMatches(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.AssetCompanyMatch, error)
	ListUnmatchedAssets(ctx context.Context, filter domain.AssetLinkFilter) ([]domain.UnmatchedAsset, error)
	ConfirmAssetCompanyMatch(ctx context.Context, id int64, reviewedBy string) (domain.AssetCompanyMatch, error)
	RejectAssetCompanyMatch(ctx context.Context, id int64, reviewedBy string) (domain.AssetCompanyMatch, error)
	GetPriceQualitySummary(ctx context.Context) (domain.PriceQualitySummary, error)
	ListPriceQualityIssues(ctx context.Context, filter domain.PriceQualityIssueFilter) ([]domain.PriceQualityIssue, error)
	GetPriceBarInventory(ctx context.Context, filter domain.AssetLinkFilter) (domain.PriceBarInventory, error)
}
