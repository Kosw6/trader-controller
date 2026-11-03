-- Immutable private graphs; source group/page deliberately have no FK.
SET lock_timeout = '3s';
CREATE TABLE private_canvas_snapshot (
 id uuid PRIMARY KEY,
 owner_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 source_team_id bigint NOT NULL,
 source_page_id bigint NOT NULL,
 slot smallint NOT NULL CHECK (slot BETWEEN 1 AND 5),
 title varchar(120) NOT NULL CHECK (length(btrim(title)) BETWEEN 1 AND 120),
 team_name varchar(255) NOT NULL,
 page_title varchar(255) NOT NULL,
 graph jsonb NOT NULL CHECK (jsonb_typeof(graph)='object'),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(owner_id,source_team_id,slot),
 CHECK(octet_length(graph::text)<=4194304)
);
CREATE INDEX private_canvas_snapshot_owner ON private_canvas_snapshot(owner_id,created_at DESC);
CREATE TABLE private_canvas_snapshot_copy (
 owner_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 request_id uuid NOT NULL,
 snapshot_id uuid NOT NULL,
 page_id bigint NOT NULL,
 directory_id bigint NOT NULL,
 PRIMARY KEY(owner_id,request_id)
);
RESET lock_timeout;
