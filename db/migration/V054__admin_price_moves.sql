-- Threshold preferences only; existing prices, raw receipts and financial rows are untouched.
CREATE TABLE admin_price_move_rule (
 id smallint PRIMARY KEY CHECK(id=1),
 rise_percent numeric(12,4) NOT NULL CHECK(rise_percent BETWEEN 0.01 AND 10000),
 fall_percent numeric(12,4) NOT NULL CHECK(fall_percent BETWEEN 0.01 AND 100),
 version bigint NOT NULL DEFAULT 1 CHECK(version>0),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO admin_price_move_rule(id,rise_percent,fall_percent) VALUES(1,5,5);
CREATE TABLE admin_price_move_rule_history (
 version bigint PRIMARY KEY,
 rise_percent numeric(12,4) NOT NULL,
 fall_percent numeric(12,4) NOT NULL,
 updated_by text NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
