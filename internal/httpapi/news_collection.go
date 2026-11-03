package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/newscollection"
	"net/http"
	"strconv"
	"time"
)

func (s *Server) resolveNewsSelection(r *http.Request, req newscollection.Request) (newscollection.Request, error) {
	if len(req.AssetIDs) == 0 {
		return req, nil
	}
	resolver, ok := s.service.(interface {
		ResolveNewsCollection(context.Context, newscollection.Request) (newscollection.Request, error)
	})
	if !ok {
		return req, fmt.Errorf("stock selection resolver is unavailable")
	}
	return resolver.ResolveNewsCollection(r.Context(), req)
}

// Uses the existing admin authentication, job status/cancel/retry APIs and scheduler.
func (s *Server) newsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/admin/news/readiness", func(w http.ResponseWriter, r *http.Request) {
		jobID, err := strconv.ParseInt(r.URL.Query().Get("jobId"), 10, 64)
		if err != nil || jobID <= 0 {
			writeError(w, 400, "invalid_news_job", "positive jobId is required")
			return
		}
		reader, ok := s.service.(interface {
			GetNewsReadiness(context.Context, int64) (domain.NewsReadiness, error)
		})
		if !ok {
			writeError(w, 503, "news_readiness_unavailable", "news readiness storage is unavailable")
			return
		}
		value, e := reader.GetNewsReadiness(r.Context(), jobID)
		s.respond(w, value, e, http.StatusOK)
	})
	mux.HandleFunc("GET /api/v1/admin/news/candidates", func(w http.ResponseWriter, r *http.Request) {
		jobID, err := strconv.ParseInt(r.URL.Query().Get("jobId"), 10, 64)
		if err != nil || jobID <= 0 {
			writeError(w, 400, "invalid_news_job", "positive jobId is required")
			return
		}
		assetID := int64(0)
		if value := r.URL.Query().Get("assetId"); value != "" {
			assetID, err = strconv.ParseInt(value, 10, 64)
			if err != nil || assetID < 0 {
				writeError(w, 400, "invalid_news_asset", "invalid assetId")
				return
			}
		}
		limit, offset := 50, 0
		for name, dest := range map[string]*int{"limit": &limit, "offset": &offset} {
			if value := r.URL.Query().Get(name); value != "" {
				*dest, err = strconv.Atoi(value)
				if err != nil || *dest < 0 {
					writeError(w, 400, "invalid_news_page", "invalid page")
					return
				}
			}
		}
		reader, ok := s.service.(interface {
			ListNewsCandidates(context.Context, int64, int64, int, int) ([]domain.NewsCandidate, error)
		})
		if !ok {
			writeError(w, 503, "news_candidates_unavailable", "news candidate storage is unavailable")
			return
		}
		value, e := reader.ListNewsCandidates(r.Context(), jobID, assetID, limit, offset)
		s.respond(w, value, e, http.StatusOK)
	})
	for _, action := range []string{"plan", "runs"} {
		action := action
		mux.HandleFunc("POST /api/v1/admin/news/"+action, func(w http.ResponseWriter, r *http.Request) {
			var req newscollection.Request
			if !decodeJSON(w, r, &req) {
				return
			}
			if req.DurableRecovery && !s.newsRecovery {
				writeError(w, 503, "news_recovery_disabled", "news recovery is not enabled")
				return
			}
			req.DurableRecovery = s.newsRecovery
			resolved, err := s.resolveNewsSelection(r, req)
			if err != nil {
				s.respond(w, nil, err, 0)
				return
			}
			plan, err := newscollection.Build(resolved, time.Now().UTC())
			if err != nil {
				writeError(w, 400, "invalid_news_plan", err.Error())
				return
			}
			if action == "plan" {
				writeJSON(w, 200, plan)
				return
			}
			plan.RequestedBy = "internal-api-key"
			if session, ok := s.sessionFromRequest(r); ok {
				plan.RequestedBy = session.Actor
			}
			// Caller can repeat this header safely through the existing job idempotency constraint.
			plan.IdempotencyKey = r.Header.Get("Idempotency-Key")
			v, e := s.service.CreateJob(r.Context(), plan)
			s.respond(w, v, e, http.StatusAccepted)
		})
	}
	mux.HandleFunc("POST /api/v1/admin/news/schedules", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			News           newscollection.Request `json:"news"`
			CronExpression string                 `json:"cronExpression"`
			Timezone       string                 `json:"timezone"`
			Enabled        bool                   `json:"enabled"`
		}
		if !decodeJSON(w, r, &req) {
			return
		}
		if req.News.DurableRecovery && !s.newsRecovery {
			writeError(w, 503, "news_recovery_disabled", "news recovery is not enabled")
			return
		}
		req.News.DurableRecovery = s.newsRecovery
		if req.News.LookbackDays == 0 {
			writeError(w, 400, "invalid_news_schedule", "schedules require lookbackDays")
			return
		}
		resolved, err := s.resolveNewsSelection(r, req.News)
		if err != nil {
			s.respond(w, nil, err, 0)
			return
		}
		plan, err := newscollection.Build(resolved, time.Now().UTC())
		if err != nil {
			writeError(w, 400, "invalid_news_schedule", err.Error())
			return
		}
		raw, _ := json.Marshal(map[string]any{"news": req.News})
		v, e := s.service.CreateSchedule(r.Context(), domain.CreateScheduleRequest{JobType: plan.JobType, CronExpression: req.CronExpression, Timezone: req.Timezone, Enabled: req.Enabled, Parameters: raw})
		s.respond(w, v, e, http.StatusCreated)
	})
}
