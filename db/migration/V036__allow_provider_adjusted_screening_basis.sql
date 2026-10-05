-- MVP 조건 검색은 공급자 수정주가 기준이다. 분할 전용이라는 의미를 부여하지 않는다.
-- 기존 승인 행을 자동 전환하지 않으며, 실제 수정주가 적재/검증은 별도 수행한다.
ALTER TABLE ai.price_validation DROP CONSTRAINT price_validation_basis_check;
ALTER TABLE ai.price_validation ADD CONSTRAINT price_validation_basis_check
 CHECK (basis='SPLIT_ADJUSTED_CLOSE'
        OR (basis='KIS_PROVIDER_ADJUSTED' AND source='KIS'));
