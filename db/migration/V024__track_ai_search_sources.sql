-- 검색 변경의 원자성 경계. JPA dirty checking, bulk SQL, FK cascade를 같은 경로로 추적한다.
-- 원본 테이블은 기존 서비스 schema에 존재해야 한다. 운영 데이터 삭제/날짜 추정 보정 없음.
CREATE TABLE search_source_state (
    source_type varchar(8) NOT NULL CHECK (source_type IN ('NOTE','NODE','EDGE')),
    source_id bigint NOT NULL CHECK (source_id > 0),
    revision bigint NOT NULL CHECK (revision > 0),
    deleted boolean NOT NULL DEFAULT false,
    last_txid bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_type, source_id)
);
CREATE TABLE search_source_outbox (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version=1),
    source_type varchar(8) NOT NULL,
    source_id bigint NOT NULL,
    source_revision bigint NOT NULL,
    change_kinds text[] NOT NULL CHECK (cardinality(change_kinds)>0 AND change_kinds <@ ARRAY['CONTENT','SCOPE','RELATION','DELETE']::text[]),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    UNIQUE(source_type,source_id,source_revision)
);
CREATE INDEX ix_search_source_outbox_pending ON search_source_outbox(occurred_at,event_id) WHERE published_at IS NULL;

-- 같은 원본·같은 트랜잭션의 영향은 하나의 revision / 이벤트로 합친다.
CREATE FUNCTION ai_search_touch(kind text, sid bigint, changes text[], is_deleted boolean DEFAULT false)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE r bigint; old_deleted boolean; old_tx bigint; tx bigint := txid_current();
BEGIN
    IF sid IS NULL THEN RETURN; END IF;
    INSERT INTO search_source_state(source_type,source_id,revision,deleted,last_txid)
        VALUES(kind,sid,1,is_deleted,tx) ON CONFLICT DO NOTHING;
    SELECT revision,deleted,last_txid INTO r,old_deleted,old_tx FROM search_source_state
        WHERE source_type=kind AND source_id=sid FOR UPDATE;
    -- 삭제된 노드에 cascade 관계 삭제가 뒤늦게 도달해도 되살리지 않는다.
    IF old_deleted AND NOT is_deleted THEN RETURN; END IF;
    IF old_tx <> tx THEN r := r + 1; END IF;
    UPDATE search_source_state SET revision=r, deleted=old_deleted OR is_deleted,
        last_txid=tx, updated_at=clock_timestamp() WHERE source_type=kind AND source_id=sid;
    IF old_deleted OR is_deleted THEN changes := ARRAY['DELETE']; END IF;
    INSERT INTO search_source_outbox(source_type,source_id,source_revision,change_kinds)
        VALUES(kind,sid,r,changes)
        ON CONFLICT(source_type,source_id,source_revision) DO UPDATE SET change_kinds =
        CASE WHEN 'DELETE'=ANY(EXCLUDED.change_kinds) OR 'DELETE'=ANY(search_source_outbox.change_kinds)
          THEN ARRAY['DELETE']::text[] ELSE
          ARRAY(SELECT DISTINCT v FROM unnest(search_source_outbox.change_kinds || EXCLUDED.change_kinds) v ORDER BY v) END;
END $$;

CREATE FUNCTION ai_search_source_changed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE old_data jsonb; new_data jsonb; data jsonb; k text := upper(TG_TABLE_NAME); sid bigint;
        fields text[]; changes text[] := ARRAY[]::text[]; item record;
BEGIN
    IF TG_OP <> 'INSERT' THEN old_data := to_jsonb(OLD); END IF;
    IF TG_OP <> 'DELETE' THEN new_data := to_jsonb(NEW); END IF;
    data := coalesce(new_data,old_data); sid := (data->>'id')::bigint;
    IF TG_OP='INSERT' AND EXISTS(SELECT 1 FROM search_source_state WHERE source_type=k AND source_id=sid AND deleted)
      THEN RAISE EXCEPTION '검색 원본 ID 재사용은 허용되지 않습니다'; END IF;
    IF TG_OP='DELETE' THEN
      changes := ARRAY['DELETE'];
    ELSIF TG_OP='INSERT' THEN
      changes := ARRAY['CONTENT','SCOPE','RELATION'];
    ELSE
      fields := CASE k WHEN 'NOTE' THEN ARRAY['subject','content','stock_symb','note_date']
                WHEN 'NODE' THEN ARRAY['subject','content','symb','record_date'] ELSE ARRAY['label'] END;
      IF EXISTS(SELECT 1 FROM unnest(fields) f WHERE old_data->f IS DISTINCT FROM new_data->f)
        THEN changes := array_append(changes,'CONTENT'); END IF;
      fields := CASE k WHEN 'NOTE' THEN ARRAY['user_id'] ELSE ARRAY['page_id'] END;
      IF EXISTS(SELECT 1 FROM unnest(fields) f WHERE old_data->f IS DISTINCT FROM new_data->f)
        THEN changes := array_append(changes,'SCOPE'); END IF;
      IF k='EDGE' AND (old_data->'source_id' IS DISTINCT FROM new_data->'source_id' OR old_data->'target_id' IS DISTINCT FROM new_data->'target_id')
        THEN changes := array_append(changes,'RELATION'); END IF;
    END IF;
    IF cardinality(changes)=0 THEN RETURN NULL; END IF;
    PERFORM ai_search_touch(k,sid,changes,TG_OP='DELETE');
    IF k='EDGE' AND (TG_OP <> 'UPDATE' OR 'RELATION'=ANY(changes) OR 'SCOPE'=ANY(changes)) THEN
      FOR item IN SELECT DISTINCT id FROM unnest(ARRAY[(old_data->>'source_id')::bigint,(old_data->>'target_id')::bigint,
                     (new_data->>'source_id')::bigint,(new_data->>'target_id')::bigint]) id WHERE id IS NOT NULL ORDER BY id LOOP
        PERFORM ai_search_touch('NODE',item.id,ARRAY['RELATION']);
      END LOOP;
    END IF;
    IF k='NODE' AND ('SCOPE'=ANY(changes) OR TG_OP='DELETE') THEN
      FOR item IN SELECT DISTINCT note_id id FROM node_note_link WHERE node_id=sid ORDER BY note_id LOOP
        PERFORM ai_search_touch('NOTE',item.id,ARRAY['SCOPE']);
      END LOOP;
    END IF;
    RETURN NULL;
END $$;

CREATE FUNCTION ai_search_link_changed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE o jsonb; n jsonb; item record;
BEGIN
    IF TG_OP <> 'INSERT' THEN o := to_jsonb(OLD); END IF;
    IF TG_OP <> 'DELETE' THEN n := to_jsonb(NEW); END IF;
    IF TG_OP='UPDATE' AND o->'node_id'=n->'node_id' AND o->'note_id'=n->'note_id' THEN RETURN NULL; END IF;
    FOR item IN SELECT DISTINCT id FROM unnest(ARRAY[(o->>'node_id')::bigint,(n->>'node_id')::bigint]) id WHERE id IS NOT NULL ORDER BY id LOOP
      PERFORM ai_search_touch('NODE',item.id,ARRAY['RELATION']);
    END LOOP;
    FOR item IN SELECT DISTINCT id FROM unnest(ARRAY[(o->>'note_id')::bigint,(n->>'note_id')::bigint]) id WHERE id IS NOT NULL ORDER BY id LOOP
      PERFORM ai_search_touch('NOTE',item.id,ARRAY['SCOPE']);
    END LOOP;
    RETURN NULL;
END $$;

CREATE FUNCTION ai_search_container_changed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE item record;
BEGIN
    IF TG_TABLE_NAME='page' THEN
      IF OLD.directory_id IS NOT DISTINCT FROM NEW.directory_id AND OLD.user_id IS NOT DISTINCT FROM NEW.user_id THEN RETURN NULL; END IF;
    ELSE
      IF OLD.team_id IS NOT DISTINCT FROM NEW.team_id AND OLD.user_id IS NOT DISTINCT FROM NEW.user_id THEN RETURN NULL; END IF;
    END IF;
    FOR item IN SELECT n.id FROM node n JOIN page p ON p.id=n.page_id
      WHERE (TG_TABLE_NAME='page' AND p.id=NEW.id) OR (TG_TABLE_NAME='directory' AND p.directory_id=NEW.id) ORDER BY n.id LOOP
      PERFORM ai_search_touch('NODE',item.id,ARRAY['SCOPE']);
    END LOOP;
    FOR item IN SELECT e.id FROM edge e JOIN page p ON p.id=e.page_id
      WHERE (TG_TABLE_NAME='page' AND p.id=NEW.id) OR (TG_TABLE_NAME='directory' AND p.directory_id=NEW.id) ORDER BY e.id LOOP
      PERFORM ai_search_touch('EDGE',item.id,ARRAY['SCOPE']);
    END LOOP;
    FOR item IN SELECT DISTINCT l.note_id id FROM node_note_link l JOIN node n ON n.id=l.node_id JOIN page p ON p.id=n.page_id
      WHERE (TG_TABLE_NAME='page' AND p.id=NEW.id) OR (TG_TABLE_NAME='directory' AND p.directory_id=NEW.id) ORDER BY l.note_id LOOP
      PERFORM ai_search_touch('NOTE',item.id,ARRAY['SCOPE']);
    END LOOP;
    RETURN NULL;
END $$;

-- 원본 상태 backfill과 trigger 설치는 Flyway의 단일 DDL 트랜잭션으로 실행한다.
LOCK TABLE note,node,edge,node_note_link,page,directory IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO search_source_state(source_type,source_id,revision,deleted,last_txid)
SELECT 'NOTE',id,1,false,txid_current() FROM note UNION ALL
SELECT 'NODE',id,1,false,txid_current() FROM node UNION ALL
SELECT 'EDGE',id,1,false,txid_current() FROM edge;
CREATE TRIGGER ai_note_changed AFTER INSERT OR UPDATE OR DELETE ON note FOR EACH ROW EXECUTE FUNCTION ai_search_source_changed();
CREATE TRIGGER ai_node_changed AFTER INSERT OR UPDATE OR DELETE ON node FOR EACH ROW EXECUTE FUNCTION ai_search_source_changed();
CREATE TRIGGER ai_edge_changed AFTER INSERT OR UPDATE OR DELETE ON edge FOR EACH ROW EXECUTE FUNCTION ai_search_source_changed();
CREATE TRIGGER ai_link_changed AFTER INSERT OR UPDATE OR DELETE ON node_note_link FOR EACH ROW EXECUTE FUNCTION ai_search_link_changed();
CREATE TRIGGER ai_page_changed AFTER UPDATE ON page FOR EACH ROW EXECUTE FUNCTION ai_search_container_changed();
CREATE TRIGGER ai_directory_changed AFTER UPDATE ON directory FOR EACH ROW EXECUTE FUNCTION ai_search_container_changed();

-- 기존 결측값을 오늘 날짜 등으로 조작하지 않는다. 신규/수정 행부터 방어하고 기존 행은 별도 점검 후 VALIDATE.
ALTER TABLE note ADD CONSTRAINT ck_ai_note_document CHECK (subject IS NOT NULL AND length(btrim(subject))>0 AND content IS NOT NULL AND note_date IS NOT NULL) NOT VALID;
ALTER TABLE node ADD CONSTRAINT ck_ai_node_document CHECK (subject IS NOT NULL AND length(btrim(subject))>0 AND content IS NOT NULL AND record_date IS NOT NULL) NOT VALID;
