-- AI 직접 이웃 조회는 source_id=? OR target_id=?로 양쪽 끝점을 찾는다.
-- 기존 (page_id,source_id,target_id) 인덱스만으로는 페이지 조건 없는 조회를 지원하지 못한다.
-- 권한/같은 페이지 검증은 조회에서 그대로 유지한다. 관계 예산이나 타임아웃을 늘리지 않는다.
-- 일반 CREATE INDEX는 쓰기를 대기시킬 수 있으므로 운영 반영 전 실행 창을 확보한다.
CREATE INDEX IF NOT EXISTS ix_edge_source_id ON public.edge(source_id);
CREATE INDEX IF NOT EXISTS ix_edge_target_id ON public.edge(target_id);
