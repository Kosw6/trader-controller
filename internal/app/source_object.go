package app

import (
	"context"

	"github.com/kosw006/trader-controller/internal/domain"
)

// ListSourceObjects는 raw 원본이 ETL과 lineage로 이어졌는지 운영 화면에 보여준다.
func (s *Service) ListSourceObjects(ctx context.Context, filter domain.ListFilter) ([]domain.SourceObjectStatus, error) {
	return s.store.ListSourceObjects(ctx, normalizeFilter(filter))
}
