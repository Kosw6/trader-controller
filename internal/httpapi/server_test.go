package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kosw006/trader-controller/internal/domain"
)

type stubService struct {
	Service
	summary         domain.Summary
	linkSummary     domain.AssetCompanyLinkSummary
	unmatched       []domain.UnmatchedAsset
	unmatchedFilter domain.AssetLinkFilter
	confirmed       domain.AssetCompanyMatch
	confirmReviewer string
	priceSummary    domain.PriceQualitySummary
	qualityIssues   []domain.PriceQualityIssue
	qualityFilter   domain.PriceQualityIssueFilter
	jobItems        []domain.JobItem
	itemFilter      domain.ListFilter
	macroSeries     []domain.MacroSeries
	macroProvider   string
	macroFilter     domain.ListFilter
	blsInventory    domain.BLSInventory
	priceInventory  domain.PriceBarInventory
	priceFilter     domain.AssetLinkFilter
	rawReadyID      int64
	rawReadyEvent   domain.OutboxEvent
}

func (s *stubService) Ping(context.Context) error {
	return nil
}

func (s *stubService) GetSummary(context.Context) (domain.Summary, error) {
	return s.summary, nil
}

func (s *stubService) GetAssetCompanyLinkSummary(context.Context) (domain.AssetCompanyLinkSummary, error) {
	return s.linkSummary, nil
}

func (s *stubService) ListUnmatchedAssets(_ context.Context, filter domain.AssetLinkFilter) ([]domain.UnmatchedAsset, error) {
	s.unmatchedFilter = filter
	return s.unmatched, nil
}

func (s *stubService) ConfirmAssetCompanyMatch(_ context.Context, _ int64, reviewer string) (domain.AssetCompanyMatch, error) {
	s.confirmReviewer = reviewer
	return s.confirmed, nil
}

func (s *stubService) GetPriceQualitySummary(context.Context) (domain.PriceQualitySummary, error) {
	return s.priceSummary, nil
}

func (s *stubService) ListPriceQualityIssues(_ context.Context, filter domain.PriceQualityIssueFilter) ([]domain.PriceQualityIssue, error) {
	s.qualityFilter = filter
	return s.qualityIssues, nil
}

func (s *stubService) ListItems(_ context.Context, _ int64, filter domain.ListFilter) ([]domain.JobItem, error) {
	s.itemFilter = filter
	return s.jobItems, nil
}

func (s *stubService) ListMacroSeries(_ context.Context, provider string, filter domain.ListFilter) ([]domain.MacroSeries, error) {
	s.macroProvider = provider
	s.macroFilter = filter
	return s.macroSeries, nil
}

func (s *stubService) GetBLSInventory(context.Context) (domain.BLSInventory, error) {
	return s.blsInventory, nil
}

func (s *stubService) GetPriceBarInventory(_ context.Context, filter domain.AssetLinkFilter) (domain.PriceBarInventory, error) {
	s.priceFilter = filter
	return s.priceInventory, nil
}

func (s *stubService) MarkRawObjectReady(_ context.Context, sourceObjectID int64) (domain.OutboxEvent, error) {
	s.rawReadyID = sourceObjectID
	return s.rawReadyEvent, nil
}

func (s *stubService) ListSourceObjects(context.Context, domain.ListFilter) ([]domain.SourceObjectStatus, error) {
	return nil, nil
}

func (s *stubService) UpdateWorkerPolicy(_ context.Context, _ int64, req domain.UpdateWorkerPolicyRequest) (domain.WorkerPolicy, error) {
	return domain.WorkerPolicy{
		ID:                     1,
		Enabled:                req.Enabled,
		ScaleOutLagThreshold:   req.ScaleOutLagThreshold,
		ScaleOutQueueThreshold: req.ScaleOutQueueThreshold,
		BatchSize:              req.BatchSize,
		MaxConcurrency:         req.MaxConcurrency,
		Config:                 req.Config,
	}, nil
}

func TestHealthDoesNotRequireAuthentication(t *testing.T) {
	server := New(&stubService{}, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestAdminEndpointRequiresAPIKey(t *testing.T) {
	server := New(&stubService{}, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/pipeline/summary", nil)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestAdminSummaryWithAPIKey(t *testing.T) {
	service := &stubService{summary: domain.Summary{SchedulesEnabled: 3}}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/pipeline/summary", nil)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), `"schedulesEnabled":3`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestListUnmatchedAssetsPassesAdminFilters(t *testing.T) {
	service := &stubService{unmatched: []domain.UnmatchedAsset{{ID: 1, Ticker: "AAPL"}}}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/admin/assets/unmatched?assetType=EQUITY&exchange=NASDAQ&q=AAPL&limit=20",
		nil,
	)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if service.unmatchedFilter.AssetType != "EQUITY" || service.unmatchedFilter.Exchange != "NASDAQ" || service.unmatchedFilter.Query != "AAPL" {
		t.Fatalf("filter = %+v", service.unmatchedFilter)
	}
	if !strings.Contains(recorder.Body.String(), `"ticker":"AAPL"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestConfirmAssetCompanyMatchReadsReviewer(t *testing.T) {
	service := &stubService{confirmed: domain.AssetCompanyMatch{ID: 7, Status: "CONFIRMED"}}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/admin/assets/company-matches/7/confirm",
		strings.NewReader(`{"reviewedBy":"operator@example.com"}`),
	)
	request.Header.Set("X-Admin-API-Key", "secret")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if service.confirmReviewer != "operator@example.com" {
		t.Fatalf("reviewer = %q", service.confirmReviewer)
	}
}

func TestPriceQualitySummaryWithAPIKey(t *testing.T) {
	service := &stubService{priceSummary: domain.PriceQualitySummary{OpenIssues: 12}}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/price-quality/summary", nil)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), `"openIssues":12`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestListPriceQualityIssuesPassesFilters(t *testing.T) {
	service := &stubService{qualityIssues: []domain.PriceQualityIssue{{ID: 1, Ticker: "AA", IssueCode: "LOW_ABOVE_PRICE"}}}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/admin/price-quality/issues?status=OPEN&severity=WARNING&q=AA&limit=8",
		nil,
	)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if service.qualityFilter.Status != "OPEN" || service.qualityFilter.Severity != "WARNING" || service.qualityFilter.Query != "AA" {
		t.Fatalf("filter = %+v", service.qualityFilter)
	}
	if !strings.Contains(recorder.Body.String(), `"issueCode":"LOW_ABOVE_PRICE"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestListPipelineItemsPassesFilters(t *testing.T) {
	service := &stubService{jobItems: []domain.JobItem{{ID: 3, JobID: 2, ItemKey: "SEC:0001045810", Status: "FAILED"}}}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/admin/pipeline/items?status=FAILED&limit=10",
		nil,
	)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if service.itemFilter.Status != "FAILED" || service.itemFilter.Limit != 10 {
		t.Fatalf("filter = %+v", service.itemFilter)
	}
	if !strings.Contains(recorder.Body.String(), `"itemKey":"SEC:0001045810"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestListMacroSeriesWithProvider(t *testing.T) {
	service := &stubService{
		macroSeries: []domain.MacroSeries{{
			ID:               8,
			Provider:         "BLS",
			ExternalSeriesID: "CUSR0000SA0",
			Name:             "Consumer Price Index",
			Category:         "CPI",
			Frequency:        "MONTHLY",
			Unit:             "INDEX",
			CountryCode:      "US",
		}},
	}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/macro-series?provider=BLS&limit=20", nil)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if service.macroProvider != "BLS" || service.macroFilter.Limit != 20 {
		t.Fatalf("provider = %q filter = %+v", service.macroProvider, service.macroFilter)
	}
	if !strings.Contains(recorder.Body.String(), `"externalSeriesId":"CUSR0000SA0"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestBLSInventoryWithAPIKey(t *testing.T) {
	service := &stubService{
		blsInventory: domain.BLSInventory{
			TotalSeries:       6,
			TotalObservations: 459,
			Categories: []domain.BLSInventoryCategory{{
				Category:         "CPI",
				SeriesCount:      2,
				ObservationCount: 154,
			}},
		},
	}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/inventory/bls", nil)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), `"totalObservations":459`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestMarkRawObjectReadyWithAPIKey(t *testing.T) {
	sourceObjectID := int64(8751)
	service := &stubService{
		rawReadyEvent: domain.OutboxEvent{
			ID:             9,
			SourceObjectID: &sourceObjectID,
			EventType:      "RAW_OBJECT_READY",
			Topic:          "trader.raw.kis.ready",
			IdempotencyKey: "RAW_OBJECT_READY:8751",
		},
	}
	server := New(service, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/source-objects/8751/raw-ready", nil)
	request.Header.Set("X-Admin-API-Key", "secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if service.rawReadyID != 8751 {
		t.Fatalf("rawReadyID = %d, want 8751", service.rawReadyID)
	}
	if !strings.Contains(recorder.Body.String(), `"eventType":"RAW_OBJECT_READY"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}
