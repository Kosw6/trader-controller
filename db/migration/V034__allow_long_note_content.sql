-- 긴 노트의 문단 분할/검색 계약. 레거시 기본 varchar(255) 때문에 본문이 저장되지 않는 문제 보완.
-- 문자열 표현을 유지하며 LOB/OID로 변경하지 않는다. 제목 길이와 API 읽기 바이트 제한은 유지한다.
ALTER TABLE public.note ALTER COLUMN content TYPE text;
