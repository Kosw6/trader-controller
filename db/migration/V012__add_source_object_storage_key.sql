-- Store environment-independent raw object keys.
-- object_uri stays as the currently resolvable URI for compatibility, while
-- storage_key becomes the long-term portable identifier under a local/S3 base.
ALTER TABLE source_object
    ADD COLUMN IF NOT EXISTS storage_backend VARCHAR(20) NOT NULL DEFAULT 'LOCAL',
    ADD COLUMN IF NOT EXISTS storage_key TEXT;

UPDATE source_object
SET storage_key = CASE
        WHEN storage_key IS NOT NULL THEN storage_key
        WHEN position('/pipeline_output/' in object_uri) > 0 THEN
            substring(object_uri from position('/pipeline_output/' in object_uri) + length('/pipeline_output/'))
        WHEN position('/raw/' in object_uri) > 0 THEN
            substring(object_uri from position('/raw/' in object_uri) + length('/raw/'))
        ELSE
            lower(provider) || '/' || lower(data_type) || '/' || coalesce(external_id, id::text) || '-' || substring(content_hash from 1 for 16)
    END
WHERE storage_key IS NULL;

ALTER TABLE source_object
    ALTER COLUMN storage_key SET NOT NULL;

ALTER TABLE source_object
    ADD CONSTRAINT ck_source_object_storage_backend
        CHECK (storage_backend IN ('LOCAL', 'S3'));

CREATE INDEX IF NOT EXISTS idx_source_object_storage_key
    ON source_object (storage_key);

CREATE INDEX IF NOT EXISTS idx_source_object_storage_backend
    ON source_object (storage_backend, collected_at DESC);
