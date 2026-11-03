package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/newscollection"
	"github.com/kosw006/trader-controller/internal/store"
)

func (s *Service) ResolveNewsCollection(ctx context.Context, req newscollection.Request) (newscollection.Request, error) {
	if req.AssetIDs == nil {
		return req, nil
	}
	if err := newscollection.ValidateSelection(req); err != nil {
		return req, fmt.Errorf("%w: %s", store.ErrValidation, err)
	}
	req.Targets = nil
	for _, id := range req.AssetIDs {
		rows, err := s.ListAssets(ctx, domain.AssetLinkFilter{AssetID: id, AssetType: "EQUITY", Limit: 1})
		if err != nil {
			return req, err
		}
		if len(rows) != 1 || rows[0].ID != id {
			return req, fmt.Errorf("%w: selected stock %d is inactive or unavailable", store.ErrValidation, id)
		}
		target, err := newscollection.CatalogTarget(rows[0])
		if err != nil {
			return req, fmt.Errorf("%w: %s", store.ErrValidation, err)
		}
		req.Targets = append(req.Targets, target)
	}
	return req, nil
}

func (s *Service) validateNewsScheduleTargets(ctx context.Context, jobType string, raw json.RawMessage) error {
	if !newscollection.IsJob(jobType) {
		return nil
	}
	var envelope struct {
		News newscollection.Request `json:"news"`
	}
	if json.Unmarshal(raw, &envelope) != nil || len(envelope.News.AssetIDs) == 0 {
		return nil
	}
	req, err := s.ResolveNewsCollection(ctx, envelope.News)
	if err != nil {
		return err
	}
	plan, err := newscollection.Build(req, s.now().UTC())
	if err != nil {
		return fmt.Errorf("%w: %s", store.ErrValidation, err)
	}
	if plan.JobType != jobType {
		return fmt.Errorf("%w: news schedule provider mismatch", store.ErrValidation)
	}
	return nil
}

func (s *Service) ListNewsCandidates(ctx context.Context, jobID, assetID int64, limit, offset int) ([]domain.NewsCandidate, error) {
	if jobID <= 0 || assetID < 0 || offset < 0 || offset > 1000000 {
		return nil, fmt.Errorf("%w: invalid news candidate filters", store.ErrValidation)
	}
	reader, ok := s.store.(interface {
		ListNewsCandidates(context.Context, int64, int64, int, int) ([]domain.NewsCandidate, error)
	})
	if !ok {
		return nil, fmt.Errorf("news candidate storage is unavailable")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	return reader.ListNewsCandidates(ctx, jobID, assetID, limit, offset)
}

func (s *Service) GetNewsReadiness(ctx context.Context, jobID int64) (domain.NewsReadiness, error) {
	if jobID <= 0 {
		return domain.NewsReadiness{}, fmt.Errorf("%w: invalid news job", store.ErrValidation)
	}
	reader, ok := s.store.(interface {
		GetNewsReadiness(context.Context, int64) (domain.NewsReadiness, error)
	})
	if !ok {
		return domain.NewsReadiness{}, fmt.Errorf("news readiness storage is unavailable")
	}
	return reader.GetNewsReadiness(ctx, jobID)
}
