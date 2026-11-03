package postgres

import (
	"context"
	"github.com/kosw006/trader-controller/internal/domain"
)

// Checkpoint source IDs preserve job attribution even when immutable raw objects are reused.
func (s *Store) ListNewsCandidates(ctx context.Context, jobID, assetID int64, limit, offset int) ([]domain.NewsCandidate, error) {
	rows, err := s.pool.Query(ctx, `WITH sources AS (
 SELECT DISTINCT CASE WHEN i.checkpoint->>'sourceId' ~ '^[0-9]+$'
   THEN (i.checkpoint->>'sourceId')::bigint END AS id,
   i.checkpoint->>'contractHash' AS contract_hash,j.parameters->>'contract' AS contract_version
 FROM pipeline_job_item i JOIN pipeline_job j ON j.id=i.job_id
 WHERE i.job_id=$1 AND j.source='NEWS' AND i.status IN ('COLLECTED','FAILED')
   AND i.checkpoint->>'stage'='CANDIDATES_STORED'
 ) SELECT c.url_hash,c.url,o.source_object_id,o.observation_hash,
 jsonb_build_object('provider',o.metadata->'provider','title',o.metadata->'title',
  'publisher',o.metadata->'publisher','providerReportedPublishedAt',o.metadata->'providerReportedPublishedAt',
  'gdeltBatchAt',o.metadata->'gdeltBatchAt','dateMeaning',o.metadata->'dateMeaning',
  'targetMatches',o.metadata->'targetMatches','association',o.metadata->'association',
  'providerTitle',o.metadata->'providerTitle','titleEnrichment',o.metadata->'titleEnrichment',
  'assetRelevance',o.metadata->'assetRelevance','enrichmentPolicy',o.metadata->'enrichmentPolicy',
  'originalArticleVerified',o.metadata->'originalArticleVerified','tickerIdentityVerified',o.metadata->'tickerIdentityVerified')
 FROM news_collection_observation base JOIN news_collection_candidate c ON c.url_hash=base.url_hash
 CROSS JOIN LATERAL (
  SELECT revision.* FROM news_collection_observation revision
  WHERE revision.url_hash=base.url_hash AND revision.source_object_id=base.source_object_id
   AND (revision.observation_hash=base.observation_hash OR
    (revision.metadata->>'originalObservationHash'=base.observation_hash
     AND revision.metadata->>'enrichmentPolicy'='news-title-relevance-v1'
     AND revision.metadata->>'collectionContractHash' IS NOT DISTINCT FROM base.metadata->>'collectionContractHash'))
  ORDER BY (revision.metadata->>'enrichmentPolicy'='news-title-relevance-v1') DESC NULLS LAST,
   revision.created_at DESC,revision.observation_hash LIMIT 1
 ) o
 WHERE base.source_object_id IN (SELECT id FROM sources WHERE id IS NOT NULL)
 AND NOT EXISTS (SELECT 1 FROM news_collection_observation parent
  WHERE parent.url_hash=base.url_hash AND parent.source_object_id=base.source_object_id
   AND parent.observation_hash=base.metadata->>'originalObservationHash')
 AND EXISTS (SELECT 1 FROM sources s WHERE s.id=base.source_object_id AND
  (base.metadata->>'collectionContractHash'=s.contract_hash OR
   (s.contract_version='news-v1' AND base.metadata->>'collectionContractHash' IS NULL)))
 AND ($2::bigint=0 OR o.metadata->'targetMatches' @> jsonb_build_array(jsonb_build_object('assetId',$2::bigint)))
 ORDER BY o.created_at DESC,o.url_hash,o.source_object_id,o.observation_hash LIMIT $3 OFFSET $4`, jobID, assetID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []domain.NewsCandidate{}
	for rows.Next() {
		var row domain.NewsCandidate
		if err := rows.Scan(&row.URLHash, &row.URL, &row.SourceObjectID, &row.ObservationHash, &row.Metadata); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
