package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/recovery"
	"github.com/kosw006/trader-controller/internal/recoverycoord"
	"github.com/kosw006/trader-controller/internal/store"
	"golang.org/x/crypto/bcrypt"
)

type Service interface {
	ListAssetAliases(context.Context, domain.AssetLinkFilter) ([]domain.AssetAliases, error)
	SaveAssetAlias(context.Context, int64, int64, domain.SaveAssetAlias) (domain.AssetAlias, error)
	DeleteAssetAlias(context.Context, int64, int64) error
	Ping(ctx context.Context) error
	CreateAdminSession(ctx context.Context, actor string, ipAddress *string, userAgent *string, ttl time.Duration) (string, domain.AdminSession, error)
	GetAdminSession(ctx context.Context, token string) (domain.AdminSession, error)
	RevokeAdminSession(ctx context.Context, token string) error
	ListSchedules(ctx context.Context, filter domain.ListFilter) ([]domain.Schedule, error)
	GetSchedule(ctx context.Context, id int64) (domain.Schedule, error)
	CreateSchedule(ctx context.Context, req domain.CreateScheduleRequest) (domain.Schedule, error)
	UpdateSchedule(ctx context.Context, id int64, req domain.UpdateScheduleRequest) (domain.Schedule, error)
	SetScheduleEnabled(ctx context.Context, id int64, enabled bool) (domain.Schedule, error)
	DeleteSchedule(ctx context.Context, id int64) error
	CreateJob(ctx context.Context, req domain.CreateJobRequest) (domain.JobDetail, error)
	ListJobs(ctx context.Context, filter domain.ListFilter) ([]domain.Job, error)
	GetJob(ctx context.Context, id int64) (domain.JobDetail, error)
	RecoveryPreview(ctx context.Context, id int64) (recovery.Preview, error)
	RetryJob(ctx context.Context, id int64, requestedBy string) (domain.JobDetail, error)
	CancelJob(ctx context.Context, id int64) (domain.Job, error)
	ListItems(ctx context.Context, jobID int64, filter domain.ListFilter) ([]domain.JobItem, error)
	RetryItem(ctx context.Context, id int64) (domain.JobItem, error)
	ListOutbox(ctx context.Context, filter domain.ListFilter) ([]domain.OutboxEvent, error)
	MarkRawObjectReady(ctx context.Context, sourceObjectID int64) (domain.OutboxEvent, error)
	RetryOutbox(ctx context.Context, id int64) (domain.OutboxEvent, error)
	ListProcessedEvents(ctx context.Context, filter domain.ListFilter) ([]domain.ProcessedEvent, error)
	ListSourceObjects(ctx context.Context, filter domain.ListFilter) ([]domain.SourceObjectStatus, error)
	ListWorkers(ctx context.Context) ([]domain.ManagedWorker, error)
	StartWorker(ctx context.Context, name string) (domain.ManagedWorker, error)
	StopWorker(ctx context.Context, name string) (domain.ManagedWorker, error)
	RunWorkerOnce(ctx context.Context, name string) (domain.ManagedWorker, error)
	ListWorkerTargets(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerTarget, error)
	UpdateWorkerTarget(ctx context.Context, targetKey string, req domain.UpdateWorkerTargetRequest) (domain.WorkerTarget, error)
	ListWorkerPolicies(ctx context.Context, filter domain.WorkerPolicyFilter) ([]domain.WorkerPolicy, error)
	UpdateWorkerPolicy(ctx context.Context, id int64, req domain.UpdateWorkerPolicyRequest) (domain.WorkerPolicy, error)
	GetWorkerAgentPolicies(ctx context.Context, targetKey string) (domain.WorkerAgentPolicies, error)
	UpsertWorkerHeartbeat(ctx context.Context, req domain.WorkerHeartbeatRequest) (domain.WorkerNodeHeartbeat, error)
	ListWorkerHeartbeats(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerNodeHeartbeat, error)
	CreateWorkerScaleCommand(ctx context.Context, targetKey string, req domain.CreateWorkerScaleCommandRequest) (domain.WorkerScaleCommand, error)
	ListWorkerScaleCommands(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.WorkerScaleCommand, error)
	RunCollectionPlanner(ctx context.Context, req domain.RunCollectionPlannerRequest) (domain.CollectionPlannerRun, error)
	ListCollectionPlannerRuns(ctx context.Context, filter domain.WorkerOpsFilter) ([]domain.CollectionPlannerRun, error)
	GetKafkaLagSummary(ctx context.Context, limit int) (domain.KafkaLagSummary, error)
	ListCoverage(ctx context.Context, filter domain.ListFilter) ([]domain.Coverage, error)
	ListGaps(ctx context.Context, filter domain.ListFilter) ([]domain.Gap, error)
	BackfillGap(ctx context.Context, id int64, req domain.BackfillGapRequest) (domain.JobDetail, error)
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

type Server struct {
	mapping           MappingService
	coordinator       *recoverycoord.Coordinator
	newsRecovery      bool
	service           Service
	adminAPIKey       string
	adminUsername     string
	adminPasswordHash string
	adminSessionTTL   time.Duration
	adminCookieName   string
	adminCookieSecure bool
	logger            *slog.Logger
	handler           http.Handler
}

type AuthConfig struct {
	AdminAPIKey       string
	AdminUsername     string
	AdminPasswordHash string
	SessionTTL        time.Duration
	CookieName        string
	CookieSecure      bool
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

// WriteHeader는 실제 응답 상태를 기록해 요청 로그에서 성공/실패를 구분할 수 있게 한다.
func (w *loggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Write는 응답 크기를 기록하고, 명시적 WriteHeader가 없었던 200 응답도 추적한다.
func (w *loggingResponseWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	written, err := w.ResponseWriter.Write(value)
	w.bytes += written
	return written, err
}

// Option configures an explicitly enabled API capability.
type Option func(*Server)

func WithRecoveryCoordinator(c *recoverycoord.Coordinator) Option {
	return func(s *Server) { s.coordinator = c }
}

func WithNewsRecovery() Option { return func(s *Server) { s.newsRecovery = true } }

// New는 관리자 endpoint와 공통 middleware를 등록한 HTTP Server를 만든다.
func New(service Service, auth AuthConfig, logger *slog.Logger, options ...Option) *Server {
	if auth.AdminUsername == "" {
		auth.AdminUsername = "admin"
	}
	if auth.SessionTTL <= 0 {
		auth.SessionTTL = 8 * time.Hour
	}
	if auth.CookieName == "" {
		auth.CookieName = "trader_admin_session"
	}
	s := &Server{
		service:           service,
		adminAPIKey:       auth.AdminAPIKey,
		adminUsername:     auth.AdminUsername,
		adminPasswordHash: auth.AdminPasswordHash,
		adminSessionTTL:   auth.SessionTTL,
		adminCookieName:   auth.CookieName,
		adminCookieSecure: auth.CookieSecure,
		logger:            logger,
	}
	mux := http.NewServeMux()
	for _, option := range options {
		option(s)
	}
	if s.coordinator != nil {
		s.recoveryRoutes(mux)
	}
	if s.mapping != nil {
		s.mappingRoutes(mux)
	}
	mux.HandleFunc("GET /healthz", s.health)
	s.newsRoutes(mux)
	s.priceMoveRoutes(mux)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("POST /api/v1/admin/auth/login", s.adminLogin)
	mux.HandleFunc("GET /api/v1/admin/auth/me", s.adminMe)
	mux.HandleFunc("POST /api/v1/admin/auth/logout", s.adminLogout)

	mux.HandleFunc("GET /api/v1/admin/pipeline/summary", s.summary)
	mux.HandleFunc("GET /api/v1/admin/pipeline/schedules", s.listSchedules)
	mux.HandleFunc("POST /api/v1/admin/pipeline/schedules", s.createSchedule)
	mux.HandleFunc("GET /api/v1/admin/pipeline/schedules/{id}", s.getSchedule)
	mux.HandleFunc("PUT /api/v1/admin/pipeline/schedules/{id}", s.updateSchedule)
	mux.HandleFunc("DELETE /api/v1/admin/pipeline/schedules/{id}", s.deleteSchedule)
	mux.HandleFunc("POST /api/v1/admin/pipeline/schedules/{id}/enable", s.enableSchedule)
	mux.HandleFunc("POST /api/v1/admin/pipeline/schedules/{id}/disable", s.disableSchedule)

	mux.HandleFunc("GET /api/v1/admin/pipeline/jobs", s.listJobs)
	mux.HandleFunc("GET /api/v1/admin/pipeline/jobs/{id}/recovery-preview", s.recoveryPreview)
	mux.HandleFunc("POST /api/v1/admin/pipeline/jobs", s.createJob)
	mux.HandleFunc("GET /api/v1/admin/pipeline/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /api/v1/admin/pipeline/jobs/{id}/retry", s.retryJob)
	mux.HandleFunc("POST /api/v1/admin/pipeline/jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("GET /api/v1/admin/pipeline/items", s.listPipelineItems)
	mux.HandleFunc("GET /api/v1/admin/pipeline/jobs/{id}/items", s.listJobItems)
	mux.HandleFunc("POST /api/v1/admin/pipeline/items/{id}/retry", s.retryItem)

	mux.HandleFunc("GET /api/v1/admin/pipeline/outbox", s.listOutbox)
	mux.HandleFunc("POST /api/v1/admin/pipeline/outbox/{id}/retry", s.retryOutbox)
	mux.HandleFunc("GET /api/v1/admin/source-objects", s.listSourceObjects)
	mux.HandleFunc("POST /api/v1/admin/source-objects/{id}/raw-ready", s.markRawObjectReady)
	mux.HandleFunc("GET /api/v1/admin/pipeline/processed-events", s.listProcessedEvents)
	mux.HandleFunc("GET /api/v1/admin/workers", s.listWorkers)
	mux.HandleFunc("POST /api/v1/admin/workers/{name}/start", s.startWorker)
	mux.HandleFunc("POST /api/v1/admin/workers/{name}/stop", s.stopWorker)
	mux.HandleFunc("POST /api/v1/admin/workers/{name}/run-once", s.runWorkerOnce)
	mux.HandleFunc("GET /api/v1/admin/worker-targets", s.listWorkerTargets)
	mux.HandleFunc("PUT /api/v1/admin/worker-targets/{name}", s.updateWorkerTarget)
	mux.HandleFunc("GET /api/v1/admin/worker-policies", s.listWorkerPolicies)
	mux.HandleFunc("PUT /api/v1/admin/worker-policies/{id}", s.updateWorkerPolicy)
	mux.HandleFunc("GET /api/v1/admin/worker-heartbeats", s.listWorkerHeartbeats)
	mux.HandleFunc("POST /api/v1/admin/worker-targets/{name}/scale", s.createWorkerScaleCommand)
	mux.HandleFunc("GET /api/v1/admin/worker-scale-commands", s.listWorkerScaleCommands)
	mux.HandleFunc("POST /api/v1/admin/collection-planner/run", s.runCollectionPlanner)
	mux.HandleFunc("GET /api/v1/admin/collection-planner/runs", s.listCollectionPlannerRuns)
	mux.HandleFunc("GET /api/v1/worker-agent/policies", s.workerAgentPolicies)
	mux.HandleFunc("POST /api/v1/worker-agent/heartbeat", s.workerAgentHeartbeat)
	mux.HandleFunc("GET /api/v1/admin/kafka/lag", s.kafkaLagSummary)
	mux.HandleFunc("GET /api/v1/admin/pipeline/coverage", s.listCoverage)
	mux.HandleFunc("GET /api/v1/admin/pipeline/gaps", s.listGaps)
	mux.HandleFunc("POST /api/v1/admin/pipeline/gaps/{id}/backfill", s.backfillGap)
	mux.HandleFunc("GET /api/v1/admin/macro-series", s.listMacroSeries)
	mux.HandleFunc("GET /api/v1/admin/inventory/bls", s.blsInventory)
	mux.HandleFunc("GET /api/v1/admin/inventory/price-bars", s.priceBarInventory)

	mux.HandleFunc("GET /api/v1/admin/assets/company-links/summary", s.assetCompanyLinkSummary)
	mux.HandleFunc("GET /api/v1/admin/assets", s.listAssets)
	mux.HandleFunc("GET /api/v1/admin/asset-aliases", s.listAssetAliases)
	mux.HandleFunc("POST /api/v1/admin/assets/{id}/aliases", s.saveAssetAlias)
	mux.HandleFunc("PUT /api/v1/admin/assets/{id}/aliases/{aliasId}", s.saveAssetAlias)
	mux.HandleFunc("DELETE /api/v1/admin/assets/{id}/aliases/{aliasId}", s.deleteAssetAlias)
	mux.HandleFunc("GET /api/v1/admin/assets/company-matches", s.listAssetCompanyMatches)
	mux.HandleFunc("GET /api/v1/admin/assets/unmatched", s.listUnmatchedAssets)
	mux.HandleFunc("POST /api/v1/admin/assets/company-matches/{id}/confirm", s.confirmAssetCompanyMatch)
	mux.HandleFunc("POST /api/v1/admin/assets/company-matches/{id}/reject", s.rejectAssetCompanyMatch)

	mux.HandleFunc("GET /api/v1/admin/price-quality/summary", s.priceQualitySummary)
	mux.HandleFunc("GET /api/v1/admin/price-quality/issues", s.listPriceQualityIssues)
	// 미들웨어 체인 로깅->리커버->검증->mux->실제 핸들러
	s.handler = s.logging(s.recoverer(s.authenticate(mux)))
	return s
}

// Handler는 net/http Server나 httptest에서 사용할 최종 middleware chain을 반환한다.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// health는 외부 의존성을 확인하지 않고 프로세스 생존 여부만 반환한다.
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready는 제한 시간 안에 PostgreSQL 연결이 가능한지 확인한다.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.service.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database_not_ready", "database is not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// summary는 Schedule·Job·Outbox·Coverage·Gap의 상태별 집계를 반환한다.
func (s *Server) adminLogin(w http.ResponseWriter, r *http.Request) {
	var req domain.AdminLoginRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if s.adminPasswordHash == "" {
		writeError(w, http.StatusServiceUnavailable, "admin_login_disabled", "admin password hash is not configured")
		return
	}
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(req.Username)), []byte(s.adminUsername)) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid admin credentials")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(s.adminPasswordHash), []byte(req.Password)); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid admin credentials")
		return
	}
	ipAddress := requestIP(r)
	userAgent := strings.TrimSpace(r.UserAgent())
	var userAgentPtr *string
	if userAgent != "" {
		userAgentPtr = &userAgent
	}
	token, session, err := s.service.CreateAdminSession(r.Context(), s.adminUsername, ipAddress, userAgentPtr, s.adminSessionTTL)
	if err != nil {
		s.respond(w, nil, err, http.StatusOK)
		return
	}
	s.setAdminSessionCookie(w, token, session.ExpiresAt)
	writeJSON(w, http.StatusOK, domain.AdminAuthResponse{
		Authenticated: true,
		Actor:         session.Actor,
		ExpiresAt:     &session.ExpiresAt,
	})
}

func (s *Server) adminMe(w http.ResponseWriter, r *http.Request) {
	if session, ok := s.sessionFromRequest(r); ok {
		writeJSON(w, http.StatusOK, domain.AdminAuthResponse{
			Authenticated: true,
			Actor:         session.Actor,
			ExpiresAt:     &session.ExpiresAt,
		})
		return
	}
	if s.validAdminAPIKey(r) {
		writeJSON(w, http.StatusOK, domain.AdminAuthResponse{
			Authenticated: true,
			Actor:         "internal-api-key",
		})
		return
	}
	writeError(w, http.StatusUnauthorized, "unauthorized", "admin session required")
}

func (s *Server) adminLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(s.adminCookieName); err == nil {
		if revokeErr := s.service.RevokeAdminSession(r.Context(), cookie.Value); revokeErr != nil {
			s.logger.Warn("admin logout revoke failed", "error", revokeErr)
		}
	}
	s.clearAdminSessionCookie(w)
	writeJSON(w, http.StatusOK, map[string]bool{"loggedOut": true})
}

func (s *Server) summary(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.GetSummary(r.Context())
	s.respond(w, value, err, http.StatusOK)
}

// listSchedules는 활성 상태와 페이지 조건으로 Schedule 목록을 반환한다.
func (s *Server) listSchedules(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListSchedules(r.Context(), listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// getSchedule은 URL의 Schedule ID로 상세 정보를 조회한다.
func (s *Server) getSchedule(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.GetSchedule(r.Context(), id)
	s.respond(w, value, err, http.StatusOK)
}

// createSchedule은 JSON 요청을 검증 계층에 전달하고 생성 결과를 반환한다.
func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateScheduleRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.CreateSchedule(r.Context(), req)
	s.respond(w, value, err, http.StatusCreated)
}

// updateSchedule은 기존 Schedule의 Cron·timezone·parameters를 교체한다.
func (s *Server) updateSchedule(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req domain.UpdateScheduleRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.UpdateSchedule(r.Context(), id, req)
	s.respond(w, value, err, http.StatusOK)
}

// enableSchedule은 중지된 Schedule의 다음 실행시각을 계산해 활성화한다.
func (s *Server) enableSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, true)
}

// disableSchedule은 Schedule을 비활성화하고 다음 실행시각을 제거한다.
func (s *Server) disableSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, false)
}

// setScheduleEnabled는 enable·disable endpoint가 공유하는 상태 변경 로직이다.
// deleteSchedule은 비활성 schedule만 삭제한다. 활성 schedule은 먼저 disable해야 한다.
func (s *Server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := s.service.DeleteSchedule(r.Context(), id); err != nil {
		s.respond(w, nil, err, http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) setScheduleEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.SetScheduleEnabled(r.Context(), id, enabled)
	s.respond(w, value, err, http.StatusOK)
}

// listJobs는 상태와 페이지 조건에 맞는 Job 목록을 반환한다.
func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListJobs(r.Context(), listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// createJob은 관리자가 즉시 또는 미래 시각에 실행할 Job과 Item을 생성한다.
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateJobRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.CreateJob(r.Context(), req)
	s.respond(w, value, err, http.StatusCreated)
}

// getJob은 Job 메타데이터와 하위 Item을 함께 반환한다.
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.GetJob(r.Context(), id)
	s.respond(w, value, err, http.StatusOK)
}

// recoveryPreview returns advice without changing any Job or Item.
func (s *Server) recoveryPreview(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.RecoveryPreview(r.Context(), id)
	s.respond(w, value, err, http.StatusOK)
}

// retryJob은 기존 Job의 실패한 Item으로 새 재시도 Job을 만든다.
func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		RequestedBy string `json:"requestedBy"`
	}
	if r.ContentLength > 0 && !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.RetryJob(r.Context(), id, req.RequestedBy)
	s.respond(w, value, err, http.StatusCreated)
}

// cancelJob은 대기 Job을 취소하거나 실행 중 Job에 취소 의사를 기록한다.
func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.CancelJob(r.Context(), id)
	s.respond(w, value, err, http.StatusOK)
}

// listPipelineItems는 특정 Job에 묶지 않고 실패·스킵 Item을 모아 보는 운영자 목록이다.
func (s *Server) listPipelineItems(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListItems(r.Context(), 0, listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// listJobItems는 특정 Job의 기업·시계열·기간별 Item 상태를 반환한다.
func (s *Server) listJobItems(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.ListItems(r.Context(), id, listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// retryItem은 실패·취소된 Item 하나를 다시 QUEUED 상태로 전환한다.
func (s *Server) retryItem(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.RetryItem(r.Context(), id)
	s.respond(w, value, err, http.StatusOK)
}

// listOutbox는 Kafka 전달 상태별 Outbox 이벤트를 반환한다.
func (s *Server) listOutbox(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListOutbox(r.Context(), listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// retryOutbox는 관리자가 DEAD·RETRY_WAIT 이벤트를 재발행 대기로 돌린다.
func (s *Server) retryOutbox(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.RetryOutbox(r.Context(), id)
	s.respond(w, value, err, http.StatusOK)
}

// listProcessedEvents는 Kafka Consumer가 처리한 이벤트 성공/실패/중복 처리 이력을 반환한다.
// markRawObjectReady lets a collector report that a source_object raw file is ready for ETL.
// The controller reads source_object again and creates a standardized RAW_OBJECT_READY outbox event.
func (s *Server) markRawObjectReady(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	value, err := s.service.MarkRawObjectReady(r.Context(), id)
	s.respond(w, value, err, http.StatusCreated)
}

func (s *Server) listProcessedEvents(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListProcessedEvents(r.Context(), listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// listWorkers는 controller 내부 goroutine worker의 desired/runtime 상태를 반환한다.
func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListWorkers(r.Context())
	s.respond(w, value, err, http.StatusOK)
}

// startWorker는 worker desired_state를 ENABLED로 변경한다.
func (s *Server) startWorker(w http.ResponseWriter, r *http.Request) {
	name, ok := pathName(w, r)
	if !ok {
		return
	}
	value, err := s.service.StartWorker(r.Context(), name)
	s.respond(w, value, err, http.StatusOK)
}

// stopWorker는 worker desired_state를 DISABLED로 변경한다.
func (s *Server) stopWorker(w http.ResponseWriter, r *http.Request) {
	name, ok := pathName(w, r)
	if !ok {
		return
	}
	value, err := s.service.StopWorker(r.Context(), name)
	s.respond(w, value, err, http.StatusOK)
}

// runWorkerOnce는 worker를 상시 켜지 않고 다음 manager tick에서 1회만 실행하도록 요청한다.
func (s *Server) runWorkerOnce(w http.ResponseWriter, r *http.Request) {
	name, ok := pathName(w, r)
	if !ok {
		return
	}
	value, err := s.service.RunWorkerOnce(r.Context(), name)
	s.respond(w, value, err, http.StatusOK)
}

// listWorkerTargets는 외부 Python worker node target의 desired capacity와 actuator driver를 보여준다.
func (s *Server) listWorkerTargets(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListWorkerTargets(r.Context(), workerOpsFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// listWorkerPolicies는 source별 scale/ETL 정책을 관리자 화면에 제공한다.
// updateWorkerTarget은 local docker와 AWS ASG 전환에 필요한 target 설정을 수정한다.
func (s *Server) updateWorkerTarget(w http.ResponseWriter, r *http.Request) {
	targetKey, ok := pathName(w, r)
	if !ok {
		return
	}
	var req domain.UpdateWorkerTargetRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.UpdateWorkerTarget(r.Context(), targetKey, req)
	s.respond(w, value, err, http.StatusOK)
}

func (s *Server) listWorkerPolicies(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	value, err := s.service.ListWorkerPolicies(r.Context(), domain.WorkerPolicyFilter{
		TargetKey: query.Get("targetKey"),
		Source:    query.Get("source"),
		Limit:     queryInt(r, "limit", 50),
		Offset:    queryInt(r, "offset", 0),
	})
	s.respond(w, value, err, http.StatusOK)
}

// listWorkerHeartbeats는 Python worker-agent가 보낸 마지막 heartbeat 목록을 반환한다.
// updateWorkerPolicy는 관리자 페이지에서 source별 lag/queue/batch/시간 정책을 수정한다.
func (s *Server) updateWorkerPolicy(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req domain.UpdateWorkerPolicyRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.UpdateWorkerPolicy(r.Context(), id, req)
	s.respond(w, value, err, http.StatusOK)
}

func (s *Server) listWorkerHeartbeats(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListWorkerHeartbeats(r.Context(), workerOpsFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// createWorkerScaleCommand는 관리자 수동 scale-out/in 요청을 DB command로 기록한다.
func (s *Server) createWorkerScaleCommand(w http.ResponseWriter, r *http.Request) {
	targetKey, ok := pathName(w, r)
	if !ok {
		return
	}
	var req domain.CreateWorkerScaleCommandRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.CreateWorkerScaleCommand(r.Context(), targetKey, req)
	s.respond(w, value, err, http.StatusCreated)
}

// listWorkerScaleCommands는 최근 scale-out/in 요청 이력을 반환한다.
func (s *Server) listWorkerScaleCommands(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListWorkerScaleCommands(r.Context(), workerOpsFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// runCollectionPlanner는 DB 상태만 보고 source별 수집 필요성을 탐지해 기록한다.
func (s *Server) runCollectionPlanner(w http.ResponseWriter, r *http.Request) {
	var req domain.RunCollectionPlannerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.RunCollectionPlanner(r.Context(), req)
	s.respond(w, value, err, http.StatusCreated)
}

// listCollectionPlannerRuns는 최근 detector 실행 결과를 반환한다.
func (s *Server) listCollectionPlannerRuns(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListCollectionPlannerRuns(r.Context(), workerOpsFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// workerAgentPolicies는 Python worker-agent가 자기 target에 맞는 정책을 읽는 endpoint다.
func (s *Server) workerAgentPolicies(w http.ResponseWriter, r *http.Request) {
	targetKey := strings.TrimSpace(r.URL.Query().Get("targetKey"))
	value, err := s.service.GetWorkerAgentPolicies(r.Context(), targetKey)
	s.respond(w, value, err, http.StatusOK)
}

// workerAgentHeartbeat는 Python worker-agent가 현재 idle/running 상태를 보고하는 endpoint다.
func (s *Server) workerAgentHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req domain.WorkerHeartbeatRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.UpsertWorkerHeartbeat(r.Context(), req)
	s.respond(w, value, err, http.StatusOK)
}

// listCoverage는 데이터셋·기간별 완전성 검사 결과를 반환한다.
// kafkaLagSummary는 consumer lag monitor가 저장한 최신 측정값과 최근 히스토리를 반환한다.
func (s *Server) kafkaLagSummary(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.GetKafkaLagSummary(r.Context(), queryInt(r, "limit", 20))
	s.respond(w, value, err, http.StatusOK)
}

func (s *Server) listCoverage(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListCoverage(r.Context(), listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// listGaps는 Coverage 검사에서 발견한 누락 목록을 반환한다.
func (s *Server) listGaps(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListGaps(r.Context(), listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// backfillGap은 OPEN Gap을 해결할 백필 Job을 생성한다.
func (s *Server) backfillGap(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req domain.BackfillGapRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.BackfillGap(r.Context(), id, req)
	s.respond(w, value, err, http.StatusCreated)
}

// listMacroSeries는 관리자 페이지가 BLS series id를 직접 입력하지 않고 선택할 수 있도록 카탈로그를 반환한다.
func (s *Server) listMacroSeries(w http.ResponseWriter, r *http.Request) {
	provider := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("provider")))
	value, err := s.service.ListMacroSeries(r.Context(), provider, listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// blsInventory는 BLS series/category/year별 적재량을 Data Inventory 화면에 제공한다.
func (s *Server) blsInventory(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.GetBLSInventory(r.Context())
	s.respond(w, value, err, http.StatusOK)
}

// priceBarInventory는 OHLCV 원값이 아니라 asset별 수집 기간과 row count만 운영자에게 제공한다.
func (s *Server) priceBarInventory(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.GetPriceBarInventory(r.Context(), assetLinkFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// assetCompanyLinkSummary는 Asset 연결 완료·미완료와 Match 상태 집계를 반환한다.
func (s *Server) assetCompanyLinkSummary(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.GetAssetCompanyLinkSummary(r.Context())
	s.respond(w, value, err, http.StatusOK)
}

// listAssetCompanyMatches는 검토 상태와 Asset 조건으로 Company 연결 후보를 조회한다.
func (s *Server) listAssets(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListAssets(r.Context(), assetLinkFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

func (s *Server) listAssetCompanyMatches(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListAssetCompanyMatches(r.Context(), assetLinkFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// listUnmatchedAssets는 아직 Company가 확정되지 않은 활성 Asset을 조회한다.
func (s *Server) listUnmatchedAssets(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListUnmatchedAssets(r.Context(), assetLinkFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// confirmAssetCompanyMatch는 후보를 확정하고 같은 트랜잭션에서 Asset 현재 Company를 연결한다.
func (s *Server) confirmAssetCompanyMatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req domain.ReviewAssetCompanyMatchRequest
	if r.ContentLength > 0 && !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.ConfirmAssetCompanyMatch(r.Context(), id, req.ReviewedBy)
	s.respond(w, value, err, http.StatusOK)
}

// rejectAssetCompanyMatch는 미확정 후보를 거절하고 검토 주체와 시각을 기록한다.
func (s *Server) rejectAssetCompanyMatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req domain.ReviewAssetCompanyMatchRequest
	if r.ContentLength > 0 && !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.RejectAssetCompanyMatch(r.Context(), id, req.ReviewedBy)
	s.respond(w, value, err, http.StatusOK)
}

// priceQualitySummary는 price_bar의 차트 사용 가능성과 품질 이슈 수를 요약한다.
func (s *Server) priceQualitySummary(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.GetPriceQualitySummary(r.Context())
	s.respond(w, value, err, http.StatusOK)
}

// listPriceQualityIssues는 운영자가 확인할 최근 가격 품질 이슈를 필터링해서 반환한다.
func (s *Server) listPriceQualityIssues(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListPriceQualityIssues(r.Context(), priceQualityIssueFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

// respond는 서비스 오류를 일관된 HTTP 상태와 JSON 오류 형식으로 변환한다.
func (s *Server) respond(w http.ResponseWriter, value any, err error, successStatus int) {
	if err == nil {
		writeJSON(w, successStatus, value)
		return
	}
	switch {
	case errors.Is(err, store.ErrValidation):
		writeError(w, http.StatusBadRequest, "validation_error", err.Error())
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrInvalidState):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	default:
		if isValidationError(err) {
			writeError(w, http.StatusBadRequest, "validation_error", err.Error())
			return
		}
		s.logger.Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// authenticate allows internal API-key calls and browser admin-session cookie calls.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/api/v1/admin/auth/login" {
			next.ServeHTTP(w, r)
			return
		}
		if s.validAdminAPIKey(r) {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := s.sessionFromRequest(r); ok {
			next.ServeHTTP(w, r)
			return
		}
		writeError(w, http.StatusUnauthorized, "unauthorized", "admin session required")
	})
}

func (s *Server) validAdminAPIKey(r *http.Request) bool {
	if s.adminAPIKey == "" {
		return false
	}
	provided := r.Header.Get("X-Admin-API-Key")
	return subtle.ConstantTimeCompare([]byte(provided), []byte(s.adminAPIKey)) == 1
}

func (s *Server) sessionFromRequest(r *http.Request) (domain.AdminSession, bool) {
	cookie, err := r.Cookie(s.adminCookieName)
	if err != nil || strings.TrimSpace(cookie.Value) == "" {
		return domain.AdminSession{}, false
	}
	session, err := s.service.GetAdminSession(r.Context(), cookie.Value)
	if err != nil {
		return domain.AdminSession{}, false
	}
	return session, true
}

func (s *Server) setAdminSessionCookie(w http.ResponseWriter, token string, expiresAt time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     s.adminCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.adminCookieSecure,
		SameSite: http.SameSiteLaxMode,
		Expires:  expiresAt,
		MaxAge:   int(time.Until(expiresAt).Seconds()),
	})
}

func (s *Server) clearAdminSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     s.adminCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   s.adminCookieSecure,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
	})
}

func requestIP(r *http.Request) *string {
	value := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if value != "" {
		value = strings.TrimSpace(strings.Split(value, ",")[0])
	}
	if value == "" {
		value = strings.TrimSpace(r.Header.Get("X-Real-IP"))
	}
	if value == "" {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			value = host
		} else {
			value = strings.TrimSpace(r.RemoteAddr)
		}
	}
	if net.ParseIP(value) == nil {
		return nil
	}
	return &value
}

// logging은 각 HTTP 요청의 method·path·처리시간을 구조화 로그로 남긴다.
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &loggingResponseWriter{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		if recorder.status == 0 {
			recorder.status = http.StatusOK
		}
		attrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"bytes", recorder.bytes,
			"duration", time.Since(started),
		}
		if err := r.Context().Err(); err != nil {
			attrs = append(attrs, "context_error", err)
			s.logger.Warn("http request completed with context error", attrs...)
			return
		}
		if recorder.status >= 500 {
			s.logger.Error("http request failed", attrs...)
			return
		}
		if recorder.status >= 400 {
			s.logger.Warn("http request rejected", attrs...)
			return
		}
		s.logger.Info("http request", attrs...)
	})
}

// recoverer는 Handler panic이 프로세스 전체 장애로 전파되지 않게 500으로 변환한다.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("panic recovered", "panic", recovered)
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// decodeJSON은 요청 크기와 알 수 없는 필드를 제한하고 JSON 객체 하나만 허용한다.
func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	//target은 호출부에서 지정해서 넘겨주기에 아무것이나 받게 설정
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	//json이 클라이언트 오류로 여러개 보내졌을 것을 방지
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return false
	}
	return true
}

// pathID는 Go 1.22 PathValue의 ID를 양의 정수로 검증한다.
func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_id", "path id must be a positive integer")
		return 0, false
	}
	return id, true
}

// pathName은 worker name처럼 문자열 path parameter를 읽고 빈 값을 거른다.
func pathName(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid_name", "path name must not be empty")
		return "", false
	}
	return name, true
}

// listFilter는 목록 endpoint의 공통 status·limit·offset query를 읽는다.
func listFilter(r *http.Request) domain.ListFilter {
	return domain.ListFilter{
		Status: strings.ToUpper(r.URL.Query().Get("status")),
		Limit:  queryInt(r, "limit", 50),
		Offset: queryInt(r, "offset", 0),
	}
}

// workerOpsFilter는 worker target, planner, scale command API가 공유하는 query parameter를 읽는다.
func workerOpsFilter(r *http.Request) domain.WorkerOpsFilter {
	query := r.URL.Query()
	return domain.WorkerOpsFilter{
		TargetKey: query.Get("targetKey"),
		Source:    query.Get("source"),
		Status:    query.Get("status"),
		Limit:     queryInt(r, "limit", 50),
		Offset:    queryInt(r, "offset", 0),
	}
}

// assetLinkFilter는 Company 연결 목록의 상태·종목·거래소·검색·페이지 조건을 읽는다.
func assetLinkFilter(r *http.Request) domain.AssetLinkFilter {
	query := r.URL.Query()
	return domain.AssetLinkFilter{
		Status:    query.Get("status"),
		AssetType: query.Get("assetType"),
		Exchange:  query.Get("exchange"),
		Query:     query.Get("q"),
		Limit:     queryInt(r, "limit", 50),
		Offset:    queryInt(r, "offset", 0),
	}
}

// priceQualityIssueFilter는 가격 품질 이슈 목록의 상태, 심각도, 검색어 조건을 읽는다.
func priceQualityIssueFilter(r *http.Request) domain.PriceQualityIssueFilter {
	query := r.URL.Query()
	return domain.PriceQualityIssueFilter{
		Status:   query.Get("status"),
		Severity: query.Get("severity"),
		Query:    query.Get("q"),
		Limit:    queryInt(r, "limit", 50),
		Offset:   queryInt(r, "offset", 0),
	}
}

// queryInt는 정수 query parameter가 없거나 잘못됐을 때 기본값을 반환한다.
func queryInt(r *http.Request, key string, fallback int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// writeJSON은 Content-Type과 상태코드를 설정한 뒤 값을 JSON으로 직렬화한다.
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeError는 모든 실패 응답을 동일한 error.code·message 구조로 만든다.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

// isValidationError는 사용자 입력 오류와 서버 내부 오류를 구분한다.
func isValidationError(err error) bool {
	message := err.Error()
	validationFragments := []string{
		"required", "invalid cronExpression", "invalid timezone", "valid JSON",
		"itemKey", "duplicate", "positive", "requestedCapacity", "details",
		"threshold", "batchSize", "maxConcurrency", "HH:MM", "config",
	}
	for _, fragment := range validationFragments {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}
