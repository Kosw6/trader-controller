package app

import (
	"context"
	"fmt"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
	"strings"
	"time"
)

type priceMoveStore interface {
	GetPriceMoveRule(context.Context) (domain.PriceMoveRule, error)
	SavePriceMoveRule(context.Context, domain.PriceMoveRule, string) (domain.PriceMoveRule, error)
	ListPriceMoves(context.Context, domain.PriceMoveFilter) (domain.PriceMoveResult, error)
}

func (s *Service) GetPriceMoveRule(ctx context.Context) (domain.PriceMoveRule, error) {
	reader, ok := s.store.(priceMoveStore)
	if !ok {
		return domain.PriceMoveRule{}, fmt.Errorf("price move storage unavailable")
	}
	return reader.GetPriceMoveRule(ctx)
}
func (s *Service) SavePriceMoveRule(ctx context.Context, r domain.PriceMoveRule, actor string) (domain.PriceMoveRule, error) {
	if err := r.Validate(); err != nil {
		return r, fmt.Errorf("%w: %v", store.ErrValidation, err)
	}
	reader, ok := s.store.(priceMoveStore)
	if !ok {
		return r, fmt.Errorf("price move storage unavailable")
	}
	return reader.SavePriceMoveRule(ctx, r, actor)
}
func (s *Service) ListPriceMoves(ctx context.Context, f domain.PriceMoveFilter) (domain.PriceMoveResult, error) {
	f.Query = strings.TrimSpace(f.Query)
	if f.Mode == "" {
		f.Mode = "SINGLE"
	}
	if f.Mode != "SINGLE" && f.Mode != "EVENTS" && f.Mode != "PERIOD" {
		return domain.PriceMoveResult{}, fmt.Errorf("%w: invalid price move mode", store.ErrValidation)
	}
	if (f.Start == "") != (f.End == "") || (f.Date != "" && (f.Start != "" || f.Mode != "SINGLE")) || (f.Mode == "SINGLE" && f.Start != "") {
		return domain.PriceMoveResult{}, fmt.Errorf("%w: 시작일·종료일을 함께 지정해주세요", store.ErrValidation)
	}
	if f.Start != "" {
		start, e1 := time.Parse("2006-01-02", f.Start)
		end, e2 := time.Parse("2006-01-02", f.End)
		if e1 != nil || e2 != nil || start.After(end) || end.After(time.Now().UTC()) || end.Sub(start) > 365*24*time.Hour || (f.Mode == "PERIOD" && start.Equal(end)) {
			return domain.PriceMoveResult{}, fmt.Errorf("%w: 미래 날짜를 제외한 최대 366일 범위가 필요합니다. 기간 등락은 서로 다른 시작일·종료일을 지정해주세요", store.ErrValidation)
		}
	}
	if len(f.Query) > 80 || f.Offset < 0 || f.Offset > 10000 || (f.Direction != "ALL" && f.Direction != "UP" && f.Direction != "DOWN") {
		return domain.PriceMoveResult{}, fmt.Errorf("%w: invalid price move filters", store.ErrValidation)
	}
	if f.Date != "" {
		d, err := time.Parse("2006-01-02", f.Date)
		if err != nil || d.After(time.Now().UTC()) {
			return domain.PriceMoveResult{}, fmt.Errorf("%w: 미래 날짜를 제외한 YYYY-MM-DD 기준일이 필요합니다", store.ErrValidation)
		}
	}
	reader, ok := s.store.(priceMoveStore)
	if !ok {
		return domain.PriceMoveResult{}, fmt.Errorf("price move storage unavailable")
	}
	return reader.ListPriceMoves(ctx, f)
}
