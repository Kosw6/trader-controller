-- 실패한 원 작업을 초기화하지 않고 동일 세대에 최대 한 번의 복구 작업을 연결한다.
ALTER TABLE ai.index_job ADD COLUMN recovery_of uuid UNIQUE REFERENCES ai.index_job(id);
DO $$ DECLARE c text; BEGIN
 SELECT conname INTO c FROM pg_constraint WHERE conrelid='ai.index_job'::regclass AND contype='u'
   AND array_length(conkey,1)=4;
 IF c IS NOT NULL THEN EXECUTE format('ALTER TABLE ai.index_job DROP CONSTRAINT %I',c); END IF;
END $$;
CREATE UNIQUE INDEX ai_index_logical_job ON ai.index_job(source_type,source_id,source_revision,policy_version)
 WHERE recovery_of IS NULL;
CREATE TABLE ai.dependency_circuit (
 name varchar(32) PRIMARY KEY,
 failures integer NOT NULL DEFAULT 0,
 open_until timestamptz,
 last_checked_at timestamptz
);
