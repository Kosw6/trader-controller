-- Discovery staging only. No news_article/public/RAG promotion or existing-row mutation.
CREATE TABLE news_collection_candidate (
    url_hash CHAR(64) PRIMARY KEY,
    url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE news_collection_observation (
    url_hash CHAR(64) NOT NULL REFERENCES news_collection_candidate(url_hash),
    source_object_id BIGINT NOT NULL REFERENCES source_object(id),
    observation_hash CHAR(64) NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (url_hash, source_object_id, observation_hash)
);
CREATE INDEX idx_news_collection_observation_source ON news_collection_observation(source_object_id);
-- Shared pacing for multiple worker processes. No provider credentials are stored.
CREATE TABLE news_collection_pacing (
    provider VARCHAR(10) PRIMARY KEY CHECK (provider IN ('RSS', 'GDELT')),
    next_request_at TIMESTAMPTZ NOT NULL
);
