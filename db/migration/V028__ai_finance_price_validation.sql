-- 수정주가 의미를 데이터 공급원별로 검증한 종목만 조건 검색에 넣는다.
-- adjusted_close가 NOT NULL이라는 이유만으로 분할 조정 검증을 대신하지 않는다.
CREATE TABLE ai.price_validation (
 asset_id bigint NOT NULL REFERENCES public.asset(id),
 source varchar(30) NOT NULL,
 verified boolean NOT NULL DEFAULT false,
 basis varchar(32) NOT NULL CHECK(basis='SPLIT_ADJUSTED_CLOSE'),
 evidence_reference text NOT NULL,
 validated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(asset_id,source)
);
