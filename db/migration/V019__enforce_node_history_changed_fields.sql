-- NodeHistory 한 건은 실제 변경된 필드를 하나 이상 설명해야 한다.
-- 기존 개발 데이터에 잘못된 값이 있다면 관련 Node와 History를 함께 초기화한 뒤 적용한다.
ALTER TABLE node_history
    ALTER COLUMN changed_fields SET NOT NULL,
    ADD CONSTRAINT chk_node_history_changed_fields_non_empty_array
    CHECK (
        jsonb_typeof(changed_fields::jsonb) = 'array'
        AND jsonb_array_length(changed_fields::jsonb) > 0
    );

COMMENT ON CONSTRAINT chk_node_history_changed_fields_non_empty_array ON node_history IS
    'changed_fields는 하나 이상의 변경 필드를 가진 JSON 배열이어야 한다';
