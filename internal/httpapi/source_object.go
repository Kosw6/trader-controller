package httpapi

import "net/http"

// listSourceObjects는 raw 원본의 수집 상태와 lineage 연결 수를 최근순으로 반환한다.
func (s *Server) listSourceObjects(w http.ResponseWriter, r *http.Request) {
	value, err := s.service.ListSourceObjects(r.Context(), listFilter(r))
	s.respond(w, value, err, http.StatusOK)
}
