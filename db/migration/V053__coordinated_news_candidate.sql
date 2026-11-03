-- Additive opt-in news discovery stages. Existing rows and data remain untouched.
ALTER TABLE pipeline_recovery_candidate DROP CONSTRAINT pipeline_recovery_candidate_kind_check;
ALTER TABLE pipeline_recovery_candidate ADD CONSTRAINT pipeline_recovery_candidate_kind_check
 CHECK(kind IN ('PRICE','FINANCIAL','FINANCIAL_REVISION','NEWS'));
