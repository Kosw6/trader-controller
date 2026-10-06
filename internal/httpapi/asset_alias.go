package httpapi

import (
	"github.com/kosw006/trader-controller/internal/domain"
	"net/http"
	"strconv"
)

func (s *Server) listAssetAliases(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListAssetAliases(r.Context(), assetLinkFilter(r))
	s.respond(w, value, err, http.StatusOK)
}

func aliasPathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("aliasId"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, 400, "invalid_id", "올바른 별칭 ID가 필요합니다")
		return 0, false
	}
	return id, true
}

func (s *Server) saveAssetAlias(w http.ResponseWriter, r *http.Request) {
	assetID, ok := pathID(w, r)
	if !ok {
		return
	}
	var aliasID int64
	if r.Method == http.MethodPut {
		aliasID, ok = aliasPathID(w, r)
		if !ok {
			return
		}
	}
	var req domain.SaveAssetAlias
	if !decodeJSON(w, r, &req) {
		return
	}
	value, err := s.service.SaveAssetAlias(r.Context(), assetID, aliasID, req)
	status := http.StatusOK
	if r.Method == http.MethodPost {
		status = http.StatusCreated
	}
	s.respond(w, value, err, status)
}

func (s *Server) deleteAssetAlias(w http.ResponseWriter, r *http.Request) {
	assetID, ok := pathID(w, r)
	if !ok {
		return
	}
	aliasID, ok := aliasPathID(w, r)
	if !ok {
		return
	}
	err := s.service.DeleteAssetAlias(r.Context(), assetID, aliasID)
	s.respond(w, map[string]bool{"deleted": true}, err, http.StatusOK)
}
