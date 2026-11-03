package postgres

import (
	"context"
	"github.com/kosw006/trader-controller/internal/domain"
)

func (s *Store) GetNewsReadiness(ctx context.Context, jobID int64) (domain.NewsReadiness, error) {
	result := domain.NewsReadiness{JobID: jobID}
	err := s.pool.QueryRow(ctx, `WITH job AS (
 SELECT id FROM pipeline_job WHERE id=$1 AND source='NEWS'
 ), items AS (
 SELECT i.* FROM pipeline_job_item i JOIN job j ON j.id=i.job_id
 ), selected AS (
 SELECT DISTINCT o.url_hash,o.source_object_id,o.observation_hash,o.metadata
 FROM items i JOIN pipeline_job j ON j.id=i.job_id
 JOIN news_collection_observation base ON base.source_object_id::text=i.checkpoint->>'sourceId'
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
 WHERE i.status IN ('COLLECTED','FAILED') AND i.checkpoint->>'stage'='CANDIDATES_STORED'
 AND (base.metadata->>'collectionContractHash'=i.checkpoint->>'contractHash'
  OR (j.parameters->>'contract'='news-v1' AND base.metadata->>'collectionContractHash' IS NULL))
 AND NOT EXISTS (SELECT 1 FROM news_collection_observation parent
  WHERE parent.url_hash=base.url_hash AND parent.source_object_id=base.source_object_id
  AND parent.observation_hash=base.metadata->>'originalObservationHash')
 ), visible AS (
 SELECT s.* FROM selected s JOIN public.ai_news_discovery d
 USING(url_hash,source_object_id,observation_hash)
 ), linked AS (
 SELECT v.* FROM visible v WHERE EXISTS (
  SELECT 1 FROM jsonb_array_elements(v.metadata->'targetMatches') t
  JOIN public.asset a ON a.id::text=t->>'assetId' AND a.active
  WHERE v.metadata->'requestedAssets' @> jsonb_build_array(jsonb_build_object('assetId',a.id,'ticker',a.ticker))
 ))
 SELECT (SELECT count(*) FROM items),
 (SELECT count(*) FROM items WHERE checkpoint->>'stage'='CANDIDATES_STORED'),
 (SELECT count(*) FROM selected),(SELECT count(*) FROM visible),(SELECT count(*) FROM linked),
 (SELECT count(DISTINCT url_hash) FROM linked),
 (SELECT count(*) FROM linked WHERE nullif(btrim(metadata->>'title'),'') IS NULL),
 (SELECT count(*) FROM linked WHERE NOT EXISTS (
  SELECT 1 FROM jsonb_array_elements(coalesce(metadata->'assetRelevance','[]'::jsonb)) r
  WHERE r->>'policy'='news-title-relevance-v1' AND r->>'category' IN ('DIRECT','INDIRECT','MENTION')
 )) FROM job`, jobID).Scan(&result.Items, &result.StoredItems, &result.StoredObservations, &result.ViewObservations, &result.LinkedObservations, &result.UniqueLinks, &result.MissingTitles, &result.Unclassified)
	return result, mapError(err)
}
