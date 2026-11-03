package app

import (
	"context"
	"encoding/json"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/newscollection"
	"testing"
	"time"
)

type targetStore struct {
	newsScheduleStore
	assets  map[int64]domain.AssetOption
	seenIDs []int64
}

func (s *targetStore) ListAssets(_ context.Context, f domain.AssetLinkFilter) ([]domain.AssetOption, error) {
	s.seenIDs = append(s.seenIDs, f.AssetID)
	if a, ok := s.assets[f.AssetID]; ok {
		return []domain.AssetOption{a}, nil
	}
	return []domain.AssetOption{}, nil
}

func TestCatalogSelectionExactIDsAndInactiveRejection(t *testing.T) {
	st := &targetStore{assets: map[int64]domain.AssetOption{42: {ID: 42, Ticker: "NVDA", Name: "NVIDIA CORP", Exchange: "NASDAQ", AssetType: "EQUITY"}}}
	s := New(st)
	r, err := s.ResolveNewsCollection(context.Background(), newscollection.Request{Provider: "RSS", AssetIDs: []int64{42}})
	if err != nil || len(r.Targets) != 1 || st.seenIDs[0] != 42 {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err = s.ResolveNewsCollection(context.Background(), newscollection.Request{Provider: "RSS", AssetIDs: []int64{43}}); err == nil {
		t.Fatal("inactive/missing accepted")
	}
}

func TestSelectedScheduleResolvesCatalogOnEveryDueRun(t *testing.T) {
	due := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	raw := json.RawMessage(`{"news":{"provider":"RSS","assetIds":[42],"lookbackDays":1}}`)
	st := &targetStore{assets: map[int64]domain.AssetOption{42: {ID: 42, Ticker: "NVDA", Name: "NVIDIA CORP", Exchange: "NASDAQ", AssetType: "EQUITY"}}}
	st.dueSchedules = []domain.Schedule{{ID: 1, JobType: "NEWS_RSS_COLLECT", NextRunAt: &due, CronExpression: "0 9 * * *", Timezone: "Asia/Seoul", Parameters: raw}}
	s := New(st)
	s.now = func() time.Time { return due.Add(time.Hour) }
	count, err := s.RunDueSchedules(context.Background(), 1)
	if err != nil || count != 1 {
		t.Fatalf("%d %v", count, err)
	}
	var p newscollection.Parameters
	json.Unmarshal(st.jobSchedule.Parameters, &p)
	if p.Contract != newscollection.TargetVersion || p.Targets[0].Ticker != "NVDA" {
		t.Fatal(p)
	}
	if string(st.dueSchedules[0].Parameters) != string(raw) {
		t.Fatal("stored schedule template mutated")
	}
	if _, err = s.CreateSchedule(context.Background(), domain.CreateScheduleRequest{JobType: "NEWS_RSS_COLLECT", CronExpression: "0 9 * * *", Timezone: "Asia/Seoul", Parameters: raw}); err != nil {
		t.Fatal(err)
	}
	delete(st.assets, 42)
	if _, err = s.RunDueSchedules(context.Background(), 1); err == nil {
		t.Fatal("inactive scheduled target downloaded")
	}
}

type candidateStore struct {
	stubStore
	seen [4]int64
}

func (s *candidateStore) ListNewsCandidates(_ context.Context, jobID, assetID int64, limit, offset int) ([]domain.NewsCandidate, error) {
	s.seen = [4]int64{jobID, assetID, int64(limit), int64(offset)}
	return []domain.NewsCandidate{}, nil
}
func TestCandidateQueryClampsPageAndRejectsInvalidScope(t *testing.T) {
	st := &candidateStore{}
	s := New(st)
	if _, err := s.ListNewsCandidates(context.Background(), 1, 42, 500, 0); err != nil {
		t.Fatal(err)
	}
	if st.seen != [4]int64{1, 42, 100, 0} {
		t.Fatal(st.seen)
	}
	for _, v := range [][4]int64{{0, 0, 50, 0}, {1, -1, 50, 0}, {1, 42, 50, -1}, {1, 42, 50, 1000001}} {
		if _, err := s.ListNewsCandidates(context.Background(), v[0], v[1], int(v[2]), int(v[3])); err == nil {
			t.Fatal("invalid filters accepted")
		}
	}
}
