package httpapi

import (
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/kosw006/trader-controller/internal/recoverycoord"
)

// Internal worker endpoints are opt-in AND require the API key (not cookies).
// Apply is deliberately not an arbitrary SQL/phase-completion HTTP endpoint.
func (s *Server) recoveryRoutes(mux *http.ServeMux) {
	// Browser read-only endpoints use the existing outer admin-session middleware.
	// Mutating worker routes below continue requiring the API key.
	mux.HandleFunc("GET /api/v1/admin/pipeline/recovery/overview", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.coordinator.Summary(r.Context())
		coordRespond(w, v, e)
	})
	mux.HandleFunc("GET /api/v1/admin/pipeline/recovery/operations", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.coordinator.StatusPage(r.Context(), r.URL.Query().Get("after"))
		coordRespond(w, map[string]any{"rows": v}, e)
	})
	wrap := func(f http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !s.validAdminAPIKey(r) {
				writeError(w, 401, "unauthorized", "worker API key required")
				return
			}
			f(w, r)
		}
	}
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work", wrap(s.registerRecoveryWork))
	mux.HandleFunc("GET /api/v1/admin/pipeline/recovery/work", wrap(func(w http.ResponseWriter, r *http.Request) {
		v, e := s.coordinator.StatusPage(r.Context(), r.URL.Query().Get("after"))
		coordRespond(w, map[string]any{"rows": v}, e)
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/hold-owned", wrap(func(w http.ResponseWriter, r *http.Request) {
		var l recoverycoord.Lease
		if !decodeJSON(w, r, &l) {
			return
		}
		l.WorkKey = r.PathValue("key")
		coordRespond(w, map[string]bool{"held": true}, s.coordinator.HoldOwned(r.Context(), l))
	}))
	mux.HandleFunc("GET /api/v1/admin/pipeline/recovery/work/{key}", wrap(func(w http.ResponseWriter, r *http.Request) {
		v, e := s.coordinator.Read(r.Context(), r.PathValue("key"))
		coordRespond(w, v, e)
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/claim", wrap(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Owner string `json:"owner"`
		}
		if !decodeJSON(w, r, &body) {
			return
		}
		v, e := s.coordinator.Claim(r.Context(), r.PathValue("key"), body.Owner)
		coordRespond(w, v, e)
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/renew", wrap(func(w http.ResponseWriter, r *http.Request) {
		var l recoverycoord.Lease
		if !decodeJSON(w, r, &l) {
			return
		}
		l.WorkKey = r.PathValue("key")
		v, e := s.coordinator.Renew(r.Context(), l)
		coordRespond(w, v, e)
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/requests", wrap(s.reserveRecoveryRequest))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/dispatch-raw", wrap(func(w http.ResponseWriter, r *http.Request) {
		var l recoverycoord.Lease
		if !decodeJSON(w, r, &l) {
			return
		}
		l.WorkKey = r.PathValue("key")
		coordRespond(w, map[string]bool{"enqueued": true}, s.coordinator.DispatchRaw(r.Context(), l))
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/requests/{request}/report", wrap(s.reportRecoveryRequest))
	mux.HandleFunc("GET /api/v1/admin/pipeline/recovery/work/{key}/requests/{request}", wrap(func(w http.ResponseWriter, r *http.Request) {
		v, e := s.coordinator.ReadRequest(r.Context(), r.PathValue("key"), r.PathValue("request"))
		coordRespond(w, v, e)
	}))
	mux.HandleFunc("GET /api/v1/admin/pipeline/recovery/work/{key}/receipts/{receipt}", wrap(func(w http.ResponseWriter, r *http.Request) {
		v, e := s.coordinator.ReadReceipt(r.Context(), r.PathValue("key"), r.PathValue("receipt"))
		coordRespond(w, v, e)
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/pause", wrap(func(w http.ResponseWriter, r *http.Request) {
		coordRespond(w, map[string]bool{"paused": true}, s.coordinator.Pause(r.Context(), r.PathValue("key")))
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/pause-owned", wrap(func(w http.ResponseWriter, r *http.Request) {
		var l recoverycoord.Lease
		if !decodeJSON(w, r, &l) {
			return
		}
		l.WorkKey = r.PathValue("key")
		coordRespond(w, map[string]bool{"paused": true}, s.coordinator.PauseOwned(r.Context(), l))
	}))
	mux.HandleFunc("POST /api/v1/admin/pipeline/recovery/work/{key}/resume", wrap(func(w http.ResponseWriter, r *http.Request) {
		coordRespond(w, map[string]bool{"resumed": true}, s.coordinator.Resume(r.Context(), r.PathValue("key")))
	}))
}
func (s *Server) registerRecoveryWork(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Identity recoverycoord.Identity `json:"identity"`
		Policy   recoverycoord.Policy   `json:"policy"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	v, e := s.coordinator.Register(r.Context(), body.Identity, body.Policy)
	coordRespond(w, v, e)
}
func (s *Server) reserveRecoveryRequest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Lease     recoverycoord.Lease `json:"lease"`
		RequestID string              `json:"requestId"`
		Operation string              `json:"operation"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	body.Lease.WorkKey = r.PathValue("key")
	coordRespond(w, map[string]bool{"sendOnce": true}, s.coordinator.Reserve(r.Context(), body.Lease, body.RequestID, body.Operation))
}
func (s *Server) reportRecoveryRequest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Lease        recoverycoord.Lease `json:"lease"`
		Outcome      string              `json:"outcome"`
		RetryAfterMS int64               `json:"retryAfterMs"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	body.Lease.WorkKey = r.PathValue("key")
	coordRespond(w, map[string]bool{"recorded": true}, s.coordinator.Report(r.Context(), body.Lease, r.PathValue("request"), body.Outcome, body.RetryAfterMS))
}
func coordRespond(w http.ResponseWriter, value any, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, value)
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "not_found", "recovery record not found")
		return
	}
	if errors.Is(err, recoverycoord.ErrInvalid) {
		writeError(w, 400, "invalid_contract", recoverycoord.ErrInvalid.Error())
		return
	}
	for _, safe := range []error{recoverycoord.ErrConflict, recoverycoord.ErrLease, recoverycoord.ErrBudget, recoverycoord.ErrCooling, recoverycoord.ErrReview, recoverycoord.ErrReserved, recoverycoord.ErrReceipt, recoverycoord.ErrUncertain} {
		if errors.Is(err, safe) {
			codes := map[error]string{recoverycoord.ErrConflict: "contract_conflict", recoverycoord.ErrLease: "lease_lost", recoverycoord.ErrBudget: "budget_exhausted", recoverycoord.ErrCooling: "quota_wait", recoverycoord.ErrReview: "review_required", recoverycoord.ErrReserved: "already_reserved", recoverycoord.ErrReceipt: "already_committed", recoverycoord.ErrUncertain: "commit_uncertain"}
			writeError(w, 409, codes[safe], safe.Error())
			return
		}
	}
	// Never expose a DB error's SQL, parameters or connection details.
	writeError(w, 503, "recovery_unavailable", "coordination unavailable; do not send or retry writes")
}
