package httpapi

import (
	"context"
	"github.com/kosw006/trader-controller/internal/financialmapping"
	"net/http"
	"strconv"
)

type MappingService interface {
	Items(context.Context, int64, int) ([]financialmapping.Item, error)
	Companies(context.Context, string) ([]financialmapping.Company, error)
	State(context.Context) (financialmapping.State, error)
	Start(context.Context, financialmapping.Request, string) (financialmapping.Run, error)
	SaveSchedule(context.Context, int64, financialmapping.ScheduleRequest) (financialmapping.Schedule, error)
}

func WithFinancialMapping(service MappingService) Option {
	return func(s *Server) { s.mapping = service }
}
func (s *Server) mappingRoutes(mux *http.ServeMux) {
	const root = "/api/v1/admin/financial-mapping"
	mux.HandleFunc("GET "+root+"/companies", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.mapping.Companies(r.Context(), r.URL.Query().Get("q"))
		s.respond(w, v, e, http.StatusOK)
	})
	mux.HandleFunc("GET "+root, func(w http.ResponseWriter, r *http.Request) {
		v, e := s.mapping.State(r.Context())
		s.respond(w, v, e, http.StatusOK)
	})
	mux.HandleFunc("GET "+root+"/runs/{id}/items", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		offset := 0
		if value := r.URL.Query().Get("offset"); value != "" {
			var e error
			offset, e = strconv.Atoi(value)
			if e != nil {
				writeError(w, 400, "invalid_offset", "페이지 범위를 확인하세요")
				return
			}
		}
		v, e := s.mapping.Items(r.Context(), id, offset)
		s.respond(w, v, e, http.StatusOK)
	})
	mux.HandleFunc("POST "+root+"/runs", func(w http.ResponseWriter, r *http.Request) {
		var req financialmapping.Request
		if !decodeJSON(w, r, &req) {
			return
		}
		actor := "internal-api-key"
		if session, ok := s.sessionFromRequest(r); ok {
			actor = session.Actor
		}
		v, e := s.mapping.Start(r.Context(), req, actor)
		s.respond(w, v, e, http.StatusAccepted)
	})
	mux.HandleFunc("POST "+root+"/schedules", func(w http.ResponseWriter, r *http.Request) {
		var req financialmapping.ScheduleRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		v, e := s.mapping.SaveSchedule(r.Context(), 0, req)
		s.respond(w, v, e, http.StatusCreated)
	})
	mux.HandleFunc("PUT "+root+"/schedules/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		var req financialmapping.ScheduleRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		v, e := s.mapping.SaveSchedule(r.Context(), id, req)
		s.respond(w, v, e, http.StatusOK)
	})
}
