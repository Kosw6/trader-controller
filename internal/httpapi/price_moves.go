package httpapi

import (
	"context"
	"github.com/kosw006/trader-controller/internal/domain"
	"net/http"
	"strconv"
)

type priceMoveService interface {
	GetPriceMoveRule(context.Context) (domain.PriceMoveRule, error)
	SavePriceMoveRule(context.Context, domain.PriceMoveRule, string) (domain.PriceMoveRule, error)
	ListPriceMoves(context.Context, domain.PriceMoveFilter) (domain.PriceMoveResult, error)
}

func (s *Server) priceMoveRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/admin/price-moves/rule", func(w http.ResponseWriter, r *http.Request) {
		api, ok := s.service.(priceMoveService)
		if !ok {
			writeError(w, 503, "price_moves_unavailable", "price move storage unavailable")
			return
		}
		v, e := api.GetPriceMoveRule(r.Context())
		s.respond(w, v, e, 200)
	})
	mux.HandleFunc("PUT /api/v1/admin/price-moves/rule", func(w http.ResponseWriter, r *http.Request) {
		var rule domain.PriceMoveRule
		if !decodeJSON(w, r, &rule) {
			return
		}
		api, ok := s.service.(priceMoveService)
		if !ok {
			writeError(w, 503, "price_moves_unavailable", "price move storage unavailable")
			return
		}
		actor := "internal-api-key"
		if session, ok := s.sessionFromRequest(r); ok {
			actor = session.Actor
		}
		v, e := api.SavePriceMoveRule(r.Context(), rule, actor)
		s.respond(w, v, e, 200)
	})
	mux.HandleFunc("GET /api/v1/admin/price-moves", func(w http.ResponseWriter, r *http.Request) {
		f := domain.PriceMoveFilter{Date: r.URL.Query().Get("date"), Query: r.URL.Query().Get("q"), Direction: r.URL.Query().Get("direction")}
		f.Start, f.End, f.Mode = r.URL.Query().Get("start"), r.URL.Query().Get("end"), r.URL.Query().Get("mode")
		if f.Mode == "" {
			if f.Start != "" || f.End != "" {
				f.Mode = "EVENTS"
			} else {
				f.Mode = "SINGLE"
			}
		}
		if f.Direction == "" {
			f.Direction = "ALL"
		}
		if value := r.URL.Query().Get("offset"); value != "" {
			n, e := strconv.Atoi(value)
			if e != nil {
				writeError(w, 400, "invalid_offset", "offset must be an integer")
				return
			}
			f.Offset = n
		}
		api, ok := s.service.(priceMoveService)
		if !ok {
			writeError(w, 503, "price_moves_unavailable", "price move storage unavailable")
			return
		}
		v, e := api.ListPriceMoves(r.Context(), f)
		s.respond(w, v, e, 200)
	})
}
