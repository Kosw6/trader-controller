package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/newscollection"
	"github.com/kosw006/trader-controller/internal/store"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

type newsAPIService struct {
	Service
	job      domain.CreateJobRequest
	schedule domain.CreateScheduleRequest
}

type stockNewsAPI struct {
	newsAPIService
	resolutions int
}

func (s *stockNewsAPI) GetNewsReadiness(_ context.Context, jobID int64) (domain.NewsReadiness, error) {
	return domain.NewsReadiness{JobID: jobID, Items: 1, StoredItems: 1, StoredObservations: 3, ViewObservations: 3, LinkedObservations: 3, UniqueLinks: 3}, nil
}

func TestNewsReadinessIsAdminOnlyAndRequiresJob(t *testing.T) {
	svc := &stockNewsAPI{}
	server := New(svc, AuthConfig{AdminAPIKey: "test-key"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, tc := range []struct {
		query  string
		auth   bool
		status int
	}{{"jobId=13", false, 401}, {"jobId=-1", true, 400}, {"jobId=oops", true, 400}, {"jobId=13", true, 200}} {
		r := httptest.NewRequest("GET", "/api/v1/admin/news/readiness?"+tc.query, nil)
		if tc.auth {
			r.Header.Set("X-Admin-API-Key", "test-key")
		}
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.query, w.Code, w.Body.String())
		}
		if w.Code == 200 {
			var v domain.NewsReadiness
			if json.Unmarshal(w.Body.Bytes(), &v) != nil || v.UniqueLinks != 3 {
				t.Fatal(w.Body.String())
			}
		}
	}
}

func (s *stockNewsAPI) ListNewsCandidates(_ context.Context, jobID, assetID int64, limit, offset int) ([]domain.NewsCandidate, error) {
	return []domain.NewsCandidate{{URLHash: "fixture", URL: "https://example.org/news", SourceObjectID: 7, ObservationHash: "snapshot", Metadata: json.RawMessage(`{"title":"NVIDIA earnings","originalArticleVerified":false}`)}}, nil
}

func TestNewsCandidatesAreAdminOnlyAndRequireBoundedFilters(t *testing.T) {
	svc := &stockNewsAPI{}
	server := New(svc, AuthConfig{AdminAPIKey: "test-key"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, tc := range []struct {
		query  string
		auth   bool
		status int
	}{
		{"jobId=1", false, 401}, {"", true, 400}, {"jobId=invalid", true, 400}, {"jobId=1&assetId=-1", true, 400}, {"jobId=1&offset=-1", true, 400}, {"jobId=1&limit=x", true, 400}, {"jobId=1&assetId=42&offset=0&limit=50", true, 200},
	} {
		r := httptest.NewRequest("GET", "/api/v1/admin/news/candidates?"+tc.query, nil)
		if tc.auth {
			r.Header.Set("X-Admin-API-Key", "test-key")
		}
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s got %d: %s", tc.query, w.Code, w.Body.String())
		}
	}
}

func (s *stockNewsAPI) ResolveNewsCollection(_ context.Context, r newscollection.Request) (newscollection.Request, error) {
	s.resolutions++
	if len(r.AssetIDs) != 1 || r.AssetIDs[0] != 42 {
		return r, fmt.Errorf("%w: selected stock unavailable", store.ErrValidation)
	}
	t, _ := newscollection.CatalogTarget(domain.AssetOption{ID: 42, Ticker: "NVDA", Name: "NVIDIA CORP", Exchange: "NASDAQ", AssetType: "EQUITY"})
	r.Targets = []newscollection.Target{t}
	return r, nil
}

func TestSelectedNewsAPIResolvesBeforePlanAndPreservesScheduleIDs(t *testing.T) {
	svc := &stockNewsAPI{}
	server := New(svc, AuthConfig{AdminAPIKey: "test-key"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	call := func(path, body string, auth bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		if auth {
			r.Header.Set("X-Admin-API-Key", "test-key")
		}
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		return w
	}
	body := `{"provider":"RSS","assetIds":[42],"startDate":"2016-12-30","endDate":"2016-12-30"}`
	if w := call("/api/v1/admin/news/plan", body, false); w.Code != 401 || svc.resolutions != 0 {
		t.Fatal("unauthorized resolution")
	}
	w := call("/api/v1/admin/news/plan", body, true)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var p domain.CreateJobRequest
	json.Unmarshal(w.Body.Bytes(), &p)
	var params newscollection.Parameters
	json.Unmarshal(p.Parameters, &params)
	if params.Contract != "news-v2" || params.Targets[0].AssetID != 42 || svc.resolutions != 1 {
		t.Fatal(params)
	}
	if w = call("/api/v1/admin/news/runs", strings.Replace(body, "42", "43", 1), true); w.Code != 400 || svc.job.Source != "" {
		t.Fatal("missing stock created job")
	}
	if w = call("/api/v1/admin/news/schedules", `{"news":{"provider":"RSS","assetIds":[42],"lookbackDays":1},"cronExpression":"0 9 * * *","timezone":"Asia/Seoul","enabled":false}`, true); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if !strings.Contains(string(svc.schedule.Parameters), `"assetIds":[42]`) || strings.Contains(string(svc.schedule.Parameters), "organizationNames") {
		t.Fatal("resolved identity persisted as mutable template")
	}
}

func (s *newsAPIService) CreateJob(_ context.Context, r domain.CreateJobRequest) (domain.JobDetail, error) {
	s.job = r
	return domain.JobDetail{Job: domain.Job{ID: 42}}, nil
}
func (s *newsAPIService) CreateSchedule(_ context.Context, r domain.CreateScheduleRequest) (domain.Schedule, error) {
	s.schedule = r
	return domain.Schedule{ID: 4}, nil
}

func TestNewsAPIAuthPlanRunAndDisabledSchedule(t *testing.T) {
	svc := &newsAPIService{}
	server := New(svc, AuthConfig{AdminAPIKey: "test-key"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := func(path, body string, auth bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		if auth {
			r.Header.Set("X-Admin-API-Key", "test-key")
		}
		r.Header.Set("Idempotency-Key", "test-news-request")
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		return w
	}
	body := `{"provider":"RSS","queries":["NVIDIA earnings"],"startDate":"2016-12-30","endDate":"2016-12-30"}`
	if w := request("/api/v1/admin/news/plan", body, false); w.Code != 401 {
		t.Fatalf("auth %d", w.Code)
	}
	w := request("/api/v1/admin/news/plan", body, true)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var p domain.CreateJobRequest
	json.Unmarshal(w.Body.Bytes(), &p)
	if len(p.ItemKeys) != 1 || p.Source != "NEWS" {
		t.Fatal(p)
	}
	if svc.job.Source != "" {
		t.Fatal("plan created a job")
	}
	if w = request("/api/v1/admin/news/runs", body, true); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if svc.job.IdempotencyKey != "test-news-request" || svc.job.RequestedBy != "internal-api-key" {
		t.Fatal(svc.job)
	}
	if w = request("/api/v1/admin/news/schedules", `{"news":{"provider":"GDELT","lookbackDays":1},"cronExpression":"0 9 * * *","timezone":"Asia/Seoul","enabled":false}`, true); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if svc.schedule.Enabled || svc.schedule.JobType != "NEWS_GDELT_COLLECT" {
		t.Fatal(svc.schedule)
	}
	if w = request("/api/v1/admin/news/runs", `{"provider":"GDELT","startDate":"2016-01-01","endDate":"2016-02-01"}`, true); w.Code != 400 {
		t.Fatalf("unbounded %d", w.Code)
	}
}

func TestNewsRecoveryPlanRequiresServerCapability(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		opts := []Option{}
		if enabled {
			opts = append(opts, WithNewsRecovery())
		}
		server := New(&newsAPIService{}, AuthConfig{AdminAPIKey: "test-key"}, slog.New(slog.NewTextHandler(io.Discard, nil)), opts...)
		body := `{"provider":"RSS","queries":["NVIDIA"],"startDate":"2016-12-30","endDate":"2016-12-30","durableRecovery":true}`
		request := httptest.NewRequest("POST", "/api/v1/admin/news/plan", strings.NewReader(body))
		request.Header.Set("X-Admin-API-Key", "test-key")
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, request)
		if !enabled {
			if w.Code != 503 {
				t.Fatal("disabled recovery accepted")
			}
			continue
		}
		if w.Code != 200 {
			t.Fatal("enabled recovery rejected")
		}
		var plan domain.CreateJobRequest
		if json.Unmarshal(w.Body.Bytes(), &plan) != nil {
			t.Fatal("invalid plan")
		}
		var p newscollection.Parameters
		if json.Unmarshal(plan.Parameters, &p) != nil || p.RecoveryVersion != "news-coordinated-v1" {
			t.Fatal("missing pinned recovery version")
		}
	}
}
