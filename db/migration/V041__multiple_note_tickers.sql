-- Additive note-only schema. Existing note.stock_symb remains the legacy primary tag.
-- Existing rows are read via fallback; no backfill or price tables are touched.
SET lock_timeout = '3s';
CREATE TABLE note_stock_tag (
    note_id bigint NOT NULL REFERENCES note(id) ON DELETE CASCADE,
    tag_order integer NOT NULL CHECK (tag_order >= 0 AND tag_order < 20),
    symbol varchar(64) NOT NULL CHECK (length(symbol) > 0),
    PRIMARY KEY (note_id, tag_order)
);
CREATE INDEX note_stock_tag_symbol_idx ON note_stock_tag(symbol, note_id);
RESET lock_timeout;
