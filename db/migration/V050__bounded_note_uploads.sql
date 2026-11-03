-- Application schema/search_path, additive only; existing notes are unchanged.
CREATE TABLE note_upload (
    id uuid PRIMARY KEY,
    owner_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    target_note_id bigint,
    base_revision varchar(64),
    metadata jsonb NOT NULL,
    content_bytes integer NOT NULL CHECK (content_bytes BETWEEN 0 AND 49152),
    part_count integer NOT NULL CHECK (part_count BETWEEN 1 AND 64),
    content_sha256 varchar(64) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','COMPLETED','CANCELLED')),
    result_note_id bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL DEFAULT clock_timestamp() + interval '15 minutes',
    receipt_expires_at timestamptz NOT NULL DEFAULT clock_timestamp() + interval '24 hours',
    CHECK ((target_note_id IS NULL AND base_revision IS NULL) OR
           (target_note_id > 0 AND base_revision ~ '^[0-9a-f]{64}$')),
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((status='COMPLETED') = (result_note_id IS NOT NULL))
);
CREATE INDEX note_upload_owner_created ON note_upload(owner_id,created_at);
CREATE INDEX note_upload_expiry ON note_upload(expires_at) WHERE status='OPEN';
CREATE INDEX note_upload_receipt_expiry ON note_upload(receipt_expires_at);

CREATE TABLE note_upload_part (
    upload_id uuid NOT NULL REFERENCES note_upload(id) ON DELETE CASCADE,
    part_no integer NOT NULL CHECK (part_no BETWEEN 0 AND 63),
    content text NOT NULL CHECK (octet_length(content) <= 4096),
    PRIMARY KEY(upload_id,part_no)
);
