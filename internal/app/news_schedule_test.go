package app

import (
	"context"
	"encoding/json"
	"github.com/kosw006/trader-controller/internal/domain"
	"testing"
	"time"
)

type newsScheduleStore struct {
	stubStore
	jobSchedule domain.Schedule
	keys        []string
}

func (s *newsScheduleStore) TriggerSchedule(_ context.Context, schedule domain.Schedule, source string, keys []string, _, _ time.Time) (domain.Job, error) {
	s.jobSchedule = schedule
	s.keys = keys
	return domain.Job{ID: 1, Source: source}, nil
}

func TestNewsScheduleResolvesDelayedTickAndKeepsStoredTemplate(t *testing.T) {
	due := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	raw := json.RawMessage(`{"news":{"provider":"RSS","queries":["NVIDIA earnings"],"lookbackDays":1}}`)
	st := &newsScheduleStore{stubStore: stubStore{dueSchedules: []domain.Schedule{{ID: 1, JobType: "NEWS_RSS_COLLECT", NextRunAt: &due, CronExpression: "0 9 * * *", Timezone: "Asia/Seoul", Parameters: raw}}}}
	s := New(st)
	s.now = func() time.Time { return due.Add(48 * time.Hour) }
	count, err := s.RunDueSchedules(context.Background(), 10)
	if err != nil || count != 1 {
		t.Fatalf("%d %v", count, err)
	}
	if len(st.keys) != 1 || st.keys[0][len(st.keys[0])-10:] != "2026-10-30" {
		t.Fatal(st.keys)
	}
	if string(st.dueSchedules[0].Parameters) != string(raw) || string(st.jobSchedule.Parameters) == string(raw) {
		t.Fatal("schedule mutated or unresolved template in job")
	}
	var p map[string]any
	json.Unmarshal(st.jobSchedule.Parameters, &p)
	if p["contract"] != "news-v1" || p["provider"] != "RSS" {
		t.Fatal(p)
	}
}

func TestGenericScheduleRejectsNewsProviderMismatch(t *testing.T) {
	s := New(&stubStore{})
	_, err := s.CreateSchedule(context.Background(), domain.CreateScheduleRequest{JobType: "NEWS_GDELT_COLLECT", CronExpression: "0 9 * * *", Timezone: "Asia/Seoul", Parameters: json.RawMessage(`{"news":{"provider":"RSS","queries":["NVIDIA"],"lookbackDays":1}}`)})
	if err == nil {
		t.Fatal("provider mismatch accepted")
	}
}
