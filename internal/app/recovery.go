package app

import (
	"context"

	"github.com/kosw006/trader-controller/internal/recovery"
)

// RecoveryPreview performs only the existing detail read, never a retry/write.
func (s *Service) RecoveryPreview(ctx context.Context, id int64) (recovery.Preview, error) {
	detail, err := s.store.GetJob(ctx, id)
	if err != nil {
		return recovery.Preview{}, err
	}
	return recovery.Build(detail, s.now()), nil
}
