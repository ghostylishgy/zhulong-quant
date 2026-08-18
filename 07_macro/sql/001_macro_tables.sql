-- 07_macro/sql/001_macro_tables.sql
-- Macro Resonance Engine V1 schema bootstrap (idempotent)

CREATE TABLE IF NOT EXISTS dim_macro_topic_member (
    topic_type      VARCHAR NOT NULL,
    topic_id        VARCHAR NOT NULL,
    topic_name      VARCHAR NOT NULL,
    symbol          VARCHAR NOT NULL,
    ts_code         VARCHAR DEFAULT '',
    source          VARCHAR DEFAULT 'bootstrap',
    is_active       INTEGER DEFAULT 1,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic_type, topic_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_macro_member_symbol
ON dim_macro_topic_member(symbol);

CREATE INDEX IF NOT EXISTS idx_macro_member_topic
ON dim_macro_topic_member(topic_type, topic_id);


CREATE TABLE IF NOT EXISTS fact_macro_topic_daily (
    trade_date              DATE NOT NULL,
    topic_type              VARCHAR NOT NULL,
    topic_id                VARCHAR NOT NULL,
    topic_name              VARCHAR NOT NULL,
    member_cnt              INTEGER DEFAULT 0,
    up_cnt                  INTEGER DEFAULT 0,
    limit_up_cnt            INTEGER DEFAULT 0,
    open_board_cnt          INTEGER DEFAULT 0,
    net_mf_amount           DOUBLE DEFAULT 0,
    avg_pct_chg             DOUBLE DEFAULT 0,
    heat_hits               INTEGER DEFAULT 0,
    freshness_days          INTEGER DEFAULT 0,
    anti_fake_score         DOUBLE DEFAULT 0,
    anti_fake_flag          INTEGER DEFAULT 0,
    anti_fake_reason        VARCHAR DEFAULT '',
    resonance_score         DOUBLE DEFAULT 0,
    resonance_level         VARCHAR DEFAULT 'LOW',
    llm_resonance_score     INTEGER DEFAULT NULL,
    is_event_driven_trap    BOOLEAN DEFAULT FALSE,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, topic_type, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_macro_topic_daily_date
ON fact_macro_topic_daily(trade_date);

CREATE INDEX IF NOT EXISTS idx_macro_topic_daily_topic
ON fact_macro_topic_daily(topic_type, topic_id, trade_date);


CREATE TABLE IF NOT EXISTS fact_macro_top5_daily (
    trade_date              DATE NOT NULL,
    rank                    INTEGER NOT NULL,
    topic_type              VARCHAR NOT NULL,
    topic_id                VARCHAR NOT NULL,
    topic_name              VARCHAR NOT NULL,
    resonance_score         DOUBLE DEFAULT 0,
    anti_fake_flag          INTEGER DEFAULT 0,
    reason_tags             VARCHAR DEFAULT '',
    llm_resonance_score     INTEGER DEFAULT NULL,
    is_event_driven_trap    BOOLEAN DEFAULT FALSE,
    generated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, rank),
    UNIQUE (trade_date, topic_type, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_macro_top5_daily_date
ON fact_macro_top5_daily(trade_date);


CREATE TABLE IF NOT EXISTS fact_macro_micro_overlay (
    trade_date              DATE NOT NULL,
    symbol                  VARCHAR NOT NULL,
    topic_type              VARCHAR NOT NULL,
    topic_id                VARCHAR NOT NULL,
    micro_verdict           VARCHAR DEFAULT '',
    micro_score             DOUBLE DEFAULT 0,
    macro_score             DOUBLE DEFAULT 0,
    anti_fake_flag          INTEGER DEFAULT 0,
    composite_score         DOUBLE DEFAULT 0,
    signal_label            VARCHAR DEFAULT '',
    pushed                  INTEGER DEFAULT 0,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, symbol, topic_type, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_macro_overlay_trade_date
ON fact_macro_micro_overlay(trade_date);

CREATE INDEX IF NOT EXISTS idx_macro_overlay_symbol
ON fact_macro_micro_overlay(symbol, trade_date);
