package domain

import "encoding/json"

type NewsCandidate struct {
	URLHash         string          `json:"urlHash"`
	URL             string          `json:"url"`
	SourceObjectID  int64           `json:"sourceObjectId"`
	ObservationHash string          `json:"observationHash"`
	Metadata        json.RawMessage `json:"metadata"`
}

// Counts refer to observations, not articles or verified article bodies.
type NewsReadiness struct {
	JobID              int64 `json:"jobId"`
	Items              int64 `json:"items"`
	StoredItems        int64 `json:"storedItems"`
	StoredObservations int64 `json:"storedObservations"`
	ViewObservations   int64 `json:"viewObservations"`
	LinkedObservations int64 `json:"linkedObservations"`
	UniqueLinks        int64 `json:"uniqueLinks"`
	MissingTitles      int64 `json:"missingTitles"`
	Unclassified       int64 `json:"unclassified"`
}
