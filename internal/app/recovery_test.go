package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

type recoveryStore struct {
	store.Store
	calls int
	id    int64
	err   error
}

func (s *recoveryStore) GetJob(_ context.Context, id int64) (domain.JobDetail, error) {
	s.calls++
	s.id = id
	return domain.JobDetail{Job: domain.Job{ID: id}}, s.err
}

func TestRecoveryPreviewReadsOnly(t *testing.T) {
	st := &recoveryStore{}
	s := New(st)
	now := time.Date(2026, 10, 22, 4, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	p, err := s.RecoveryPreview(context.Background(), 7)
	if err != nil || p.JobID != 7 || !p.ObservedAt.Equal(now) || st.calls != 1 || st.id != 7 || p.ExecutionAllowed {
		t.Fatalf("preview %+v error %v", p, err)
	}
	// Any write through the nil embedded Store would panic; only GetJob is implemented.
	st.err = store.ErrNotFound
	_, err = s.RecoveryPreview(context.Background(), 8)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("lost read failure: %v", err)
	}
}
