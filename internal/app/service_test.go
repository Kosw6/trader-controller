package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

type stubStore struct {
	store.Store
	createdSchedule domain.Schedule
	dueSchedules    []domain.Schedule
	triggerErr      error
	triggerCount    int
	rawReadyID      int64
}

func (s *stubStore) ActivateDueJobs(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func (s *stubStore) AggregateJobStatuses(context.Context, int) (int64, error) {
	return 0, nil
}

func (s *stubStore) CreateSchedule(_ context.Context, req domain.CreateScheduleRequest, nextRun time.Time) (domain.Schedule, error) {
	s.createdSchedule = domain.Schedule{
		ID:             1,
		JobType:        req.JobType,
		CronExpression: req.CronExpression,
		Timezone:       req.Timezone,
		Enabled:        req.Enabled,
		Parameters:     req.Parameters,
		NextRunAt:      &nextRun,
	}
	return s.createdSchedule, nil
}

func (s *stubStore) ListDueSchedules(_ context.Context, _ time.Time, _ int) ([]domain.Schedule, error) {
	return s.dueSchedules, nil
}

func (s *stubStore) TriggerSchedule(
	_ context.Context,
	_ domain.Schedule,
	_ string,
	_ []string,
	_, _ time.Time,
) (domain.Job, error) {
	s.triggerCount++
	return domain.Job{ID: 10}, s.triggerErr
}

func (s *stubStore) InsertRawObjectReadyOutbox(_ context.Context, sourceObjectID int64) (domain.OutboxEvent, error) {
	s.rawReadyID = sourceObjectID
	return domain.OutboxEvent{
		ID:             7,
		SourceObjectID: &sourceObjectID,
		EventType:      "RAW_OBJECT_READY",
		Topic:          "trader.raw.kis.ready",
		IdempotencyKey: "RAW_OBJECT_READY:8751",
	}, nil
}

func TestCreateScheduleValidatesCronAndComputesNextRun(t *testing.T) {
	st := &stubStore{}
	service := New(st)
	service.now = func() time.Time {
		return time.Date(2026, 6, 21, 8, 30, 0, 0, time.UTC)
	}

	created, err := service.CreateSchedule(context.Background(), domain.CreateScheduleRequest{
		JobType:        "BLS_COLLECT",
		CronExpression: "0 9 * * *",
		Timezone:       "Asia/Seoul",
		Enabled:        true,
		Parameters:     json.RawMessage(`{"source":"BLS","itemKeys":["BLS:WPUFD4"]}`),
	})
	if err != nil {
		t.Fatalf("CreateSchedule() error = %v", err)
	}
	want := time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC)
	if created.NextRunAt == nil || !created.NextRunAt.Equal(want) {
		t.Fatalf("nextRunAt = %v, want %v", created.NextRunAt, want)
	}
}

func TestCreateScheduleRejectsInvalidCron(t *testing.T) {
	service := New(&stubStore{})
	_, err := service.CreateSchedule(context.Background(), domain.CreateScheduleRequest{
		JobType:        "BLS_COLLECT",
		CronExpression: "not-a-cron",
		Timezone:       "UTC",
		Parameters:     json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("CreateSchedule() expected validation error")
	}
}

func TestCreateJobRejectsDuplicateItemKeys(t *testing.T) {
	service := New(&stubStore{})
	_, err := service.CreateJob(context.Background(), domain.CreateJobRequest{
		JobType:    "SEC_COLLECT",
		Source:     "SEC",
		Parameters: json.RawMessage(`{}`),
		ItemKeys:   []string{"SEC:NVDA", "SEC:NVDA"},
	})
	if err == nil {
		t.Fatal("CreateJob() expected duplicate item key error")
	}
}

func TestMarkRawObjectReadyDelegatesToStore(t *testing.T) {
	st := &stubStore{}
	service := New(st)

	event, err := service.MarkRawObjectReady(context.Background(), 8751)
	if err != nil {
		t.Fatalf("MarkRawObjectReady() error = %v", err)
	}
	if st.rawReadyID != 8751 {
		t.Fatalf("rawReadyID = %d, want 8751", st.rawReadyID)
	}
	if event.EventType != "RAW_OBJECT_READY" || event.Topic != "trader.raw.kis.ready" {
		t.Fatalf("event = %+v", event)
	}
}

func TestRunDueSchedulesIgnoresConcurrentConflict(t *testing.T) {
	due := time.Date(2026, 6, 21, 9, 0, 0, 0, time.UTC)
	st := &stubStore{
		dueSchedules: []domain.Schedule{{
			ID:             1,
			JobType:        "BLS_COLLECT",
			CronExpression: "0 * * * *",
			Timezone:       "UTC",
			Enabled:        true,
			Parameters:     json.RawMessage(`{"source":"BLS","itemKeys":["BLS:WPUFD4"]}`),
			NextRunAt:      &due,
		}},
		triggerErr: store.ErrConflict,
	}
	service := New(st)
	service.now = func() time.Time { return due }

	triggered, err := service.RunDueSchedules(context.Background(), 10)
	if err != nil {
		t.Fatalf("RunDueSchedules() error = %v", err)
	}
	if triggered != 0 || st.triggerCount != 1 {
		t.Fatalf("triggered = %d, calls = %d, want 0 and 1", triggered, st.triggerCount)
	}
}

func TestRunDueSchedulesReturnsNonConflictErrors(t *testing.T) {
	due := time.Date(2026, 6, 21, 9, 0, 0, 0, time.UTC)
	st := &stubStore{
		dueSchedules: []domain.Schedule{{
			ID:             2,
			JobType:        "BLS_COLLECT",
			CronExpression: "0 * * * *",
			Timezone:       "UTC",
			Parameters:     json.RawMessage(`{"source":"BLS","itemKeys":["BLS:WPUFD4"]}`),
			NextRunAt:      &due,
		}},
		triggerErr: errors.New("database unavailable"),
	}
	service := New(st)
	service.now = func() time.Time { return due }

	triggered, err := service.RunDueSchedules(context.Background(), 10)
	if err == nil || triggered != 0 {
		t.Fatalf("triggered = %d, err = %v, want 0 and error", triggered, err)
	}
}

func TestNormalizeAssetLinkFilterValidatesStatusAndBounds(t *testing.T) {
	filter, err := normalizeAssetLinkFilter(domain.AssetLinkFilter{
		Status: " review_required ", AssetType: " equity ", Exchange: " nasdaq ", Limit: 999,
	})
	if err != nil {
		t.Fatalf("normalizeAssetLinkFilter() error = %v", err)
	}
	if filter.Status != "REVIEW_REQUIRED" || filter.AssetType != "EQUITY" || filter.Exchange != "NASDAQ" || filter.Limit != 200 {
		t.Fatalf("filter = %+v", filter)
	}
	if _, err := normalizeAssetLinkFilter(domain.AssetLinkFilter{Status: "UNKNOWN"}); err == nil {
		t.Fatal("normalizeAssetLinkFilter() expected invalid status error")
	}
}

func TestNormalizeReviewerUsesAdminDefault(t *testing.T) {
	if got := normalizeReviewer("  "); got != "ADMIN" {
		t.Fatalf("normalizeReviewer() = %q, want ADMIN", got)
	}
}
