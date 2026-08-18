from __future__ import annotations

from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path(__file__).resolve().parents[3] / "data" / "spark.duckdb"
EVENT_TYPES = ("DCA_BUY", "DCA_SKIP", "HARVEST_SELL", "STATE_TRANSITION", "MACRO_SENTIMENT")
EVENT_TYPES_SQL = ", ".join(f"'{item}'" for item in EVENT_TYPES)

SNAPSHOT_UPSERT_SQL = """
INSERT INTO spark_state_snapshot (
    etf_code,
    state,
    avg_cost,
    total_invested,
    current_shares,
    principal_recovered,
    peak_return,
    updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, now())
ON CONFLICT (etf_code) DO UPDATE SET
    state = EXCLUDED.state,
    avg_cost = EXCLUDED.avg_cost,
    total_invested = EXCLUDED.total_invested,
    current_shares = EXCLUDED.current_shares,
    principal_recovered = EXCLUDED.principal_recovered,
    peak_return = EXCLUDED.peak_return,
    updated_at = now();
"""

EVENT_INSERT_SQL = """
INSERT INTO spark_event_log (
    event_id,
    timestamp,
    etf_code,
    event_type,
    tushare_snapshot,
    note,
    created_at
) VALUES (?, now(), ?, ?, ?, ?, now());
"""


def _event_log_sql(conn: duckdb.DuckDBPyConnection) -> str | None:
    row = conn.execute(
        """
        SELECT sql
        FROM duckdb_tables()
        WHERE table_name = 'spark_event_log'
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return row[0]


def _rebuild_event_log_with_macro(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE spark_event_log_new (
            event_id VARCHAR PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            etf_code VARCHAR NOT NULL,
            event_type VARCHAR NOT NULL CHECK (event_type IN ({EVENT_TYPES_SQL})),
            tushare_snapshot JSON NOT NULL,
            note VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        INSERT INTO spark_event_log_new (
            event_id,
            timestamp,
            etf_code,
            event_type,
            tushare_snapshot,
            note,
            created_at
        )
        SELECT
            event_id,
            timestamp,
            etf_code,
            event_type,
            tushare_snapshot,
            note,
            created_at
        FROM spark_event_log;
        """
    )
    conn.execute("DROP TABLE spark_event_log;")
    conn.execute("ALTER TABLE spark_event_log_new RENAME TO spark_event_log;")


def _ensure_event_log_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS spark_event_log (
            event_id VARCHAR PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            etf_code VARCHAR NOT NULL,
            event_type VARCHAR NOT NULL CHECK (event_type IN ({EVENT_TYPES_SQL})),
            tushare_snapshot JSON NOT NULL,
            note VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    sql = _event_log_sql(conn)
    if sql and "MACRO_SENTIMENT" not in sql:
        _rebuild_event_log_with_macro(conn)


def _ensure_legacy_snapshot_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_state_snapshot (
            etf_code VARCHAR PRIMARY KEY,
            state VARCHAR NOT NULL CHECK (
                state IN ('WATCHLIST', 'ACCUMULATING', 'PAUSED_OVERVALUED', 'FREE_RIDE')
            ),
            avg_cost DOUBLE NOT NULL DEFAULT 0,
            total_invested DOUBLE NOT NULL DEFAULT 0,
            current_shares DOUBLE NOT NULL DEFAULT 0,
            principal_recovered BOOLEAN NOT NULL DEFAULT FALSE,
            peak_return DOUBLE NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def _ensure_advisor_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_import_batches (
            batch_id VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            source_path VARCHAR,
            input_text_hash VARCHAR NOT NULL,
            cost_basis_type VARCHAR NOT NULL,
            note VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_position_weekly_snapshot (
            snapshot_id VARCHAR PRIMARY KEY,
            snapshot_date DATE NOT NULL,
            fund_code VARCHAR NOT NULL,
            fund_name VARCHAR NOT NULL,
            shares DOUBLE NOT NULL CHECK (shares >= 0),
            unit_cost DOUBLE NOT NULL,
            cost_basis_type VARCHAR NOT NULL,
            source VARCHAR NOT NULL DEFAULT 'manual',
            source_batch_id VARCHAR,
            source_hash VARCHAR NOT NULL,
            note VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_nav_daily (
            fund_code VARCHAR NOT NULL,
            nav_date DATE NOT NULL,
            unit_nav DOUBLE,
            adj_nav DOUBLE,
            source VARCHAR NOT NULL,
            data_quality VARCHAR NOT NULL,
            raw_json JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (fund_code, nav_date, source)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_valuation_signal (
            signal_id VARCHAR PRIMARY KEY,
            fund_code VARCHAR NOT NULL,
            signal_date DATE NOT NULL,
            bucket VARCHAR,
            valuation_percentile DOUBLE,
            valuation_level VARCHAR,
            trend_level VARCHAR,
            data_quality VARCHAR NOT NULL,
            source VARCHAR,
            risk_flags_json JSON,
            rationale VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_holding_signal (
            signal_id VARCHAR PRIMARY KEY,
            snapshot_date DATE NOT NULL,
            fund_code VARCHAR NOT NULL,
            fund_name VARCHAR NOT NULL,
            shares DOUBLE NOT NULL,
            unit_cost DOUBLE NOT NULL,
            latest_nav DOUBLE,
            nav_date DATE,
            market_value DOUBLE,
            cost_value DOUBLE,
            return_ratio DOUBLE,
            holding_mode VARCHAR,
            action VARCHAR NOT NULL,
            action_level VARCHAR NOT NULL,
            data_quality VARCHAR NOT NULL,
            valuation_quality VARCHAR,
            risk_flags_json JSON,
            rationale VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute("ALTER TABLE spark_holding_signal ADD COLUMN IF NOT EXISTS holding_mode VARCHAR;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_action_review (
            review_id VARCHAR PRIMARY KEY,
            signal_id VARCHAR,
            fund_code VARCHAR NOT NULL,
            signal_date DATE NOT NULL,
            recommendation VARCHAR NOT NULL,
            adoption_status VARCHAR NOT NULL DEFAULT 'PENDING',
            outcome_weeks INTEGER,
            outcome_json JSON,
            note VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def _ensure_backtest_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_backtest_run (
            run_id VARCHAR PRIMARY KEY,
            params_json JSON NOT NULL,
            universe_json JSON NOT NULL,
            start_dates_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_backtest_result (
            run_id VARCHAR NOT NULL,
            fund_code VARCHAR NOT NULL,
            bucket VARCHAR,
            is_holding BOOLEAN NOT NULL,
            start_date DATE NOT NULL,
            strategy VARCHAR NOT NULL,
            xirr DOUBLE,
            total_return DOUBLE,
            max_dd DOUBLE,
            dd_recovery_days INTEGER,
            n_sells INTEGER NOT NULL DEFAULT 0,
            n_wrong_sells INTEGER NOT NULL DEFAULT 0,
            beats_naive BOOLEAN,
            terminal_value DOUBLE,
            terminal_cash DOUBLE,
            total_contributed DOUBLE,
            wrong_sell_opportunity_cost DOUBLE,
            drawdown_saved DOUBLE,
            data_quality_flags_json JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_backtest_verdict (
            run_id VARCHAR NOT NULL,
            bucket VARCHAR NOT NULL,
            strategy VARCHAR NOT NULL,
            verdict VARCHAR NOT NULL CHECK (verdict IN ('DISCIPLINE', 'ADVISORY_ONLY', 'SKIPPED')),
            rationale VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def _ensure_candidate_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_fund_universe (
            fund_code VARCHAR PRIMARY KEY,
            fund_name VARCHAR NOT NULL,
            asset_bucket VARCHAR NOT NULL,
            theme VARCHAR NOT NULL,
            source VARCHAR NOT NULL,
            tags_json JSON,
            valuation_proxy_json JSON,
            note VARCHAR,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_fund_candidates (
            fund_code VARCHAR PRIMARY KEY,
            candidate_id VARCHAR NOT NULL,
            fund_name VARCHAR NOT NULL,
            theme VARCHAR NOT NULL,
            asset_bucket VARCHAR NOT NULL,
            candidate_status VARCHAR NOT NULL CHECK (
                candidate_status IN (
                    'WATCHING',
                    'LOW_ZONE',
                    'TREND_CONFIRM',
                    'DATA_INSUFFICIENT',
                    'PAUSED'
                )
            ),
            priority INTEGER NOT NULL DEFAULT 3,
            thesis VARCHAR,
            source VARCHAR NOT NULL,
            config_hash VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_candidate_signal (
            signal_id VARCHAR PRIMARY KEY,
            signal_date DATE NOT NULL,
            fund_code VARCHAR NOT NULL,
            fund_name VARCHAR NOT NULL,
            theme VARCHAR NOT NULL,
            asset_bucket VARCHAR NOT NULL,
            candidate_action VARCHAR NOT NULL CHECK (
                candidate_action IN (
                    'OBSERVE',
                    'LOW_ZONE_WATCH',
                    'LOW_VALUATION_WATCH',
                    'TREND_TURN_WATCH',
                    'LOW_ZONE_REBOUND_WATCH',
                    'DATA_INSUFFICIENT'
                )
            ),
            candidate_status VARCHAR NOT NULL CHECK (
                candidate_status IN (
                    'WATCHING',
                    'LOW_ZONE',
                    'TREND_CONFIRM',
                    'DATA_INSUFFICIENT',
                    'PAUSED'
                )
            ),
            action_level VARCHAR NOT NULL,
            latest_nav DOUBLE,
            nav_date DATE,
            drawdown_60d DOUBLE,
            return_20d DOUBLE,
            return_60d DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            valuation_percentile DOUBLE,
            data_quality VARCHAR NOT NULL,
            valuation_quality VARCHAR,
            risk_flags_json JSON,
            rationale VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute("ALTER TABLE spark_candidate_signal ADD COLUMN IF NOT EXISTS candidate_status VARCHAR DEFAULT 'WATCHING';")
    conn.execute(
        """
        UPDATE spark_candidate_signal
        SET candidate_status = CASE
            WHEN candidate_action = 'DATA_INSUFFICIENT' THEN 'DATA_INSUFFICIENT'
            WHEN candidate_action IN (
                'LOW_ZONE_WATCH',
                'LOW_VALUATION_WATCH'
            ) THEN 'LOW_ZONE'
            WHEN candidate_action IN (
                'TREND_TURN_WATCH',
                'LOW_ZONE_REBOUND_WATCH'
            ) THEN 'TREND_CONFIRM'
            ELSE 'WATCHING'
        END
        WHERE candidate_status IS NULL;
        """
    )


def _ensure_market_universe_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_market_fund_universe (
            fund_code VARCHAR PRIMARY KEY,
            fund_name VARCHAR NOT NULL,
            market VARCHAR NOT NULL,
            management VARCHAR,
            fund_type VARCHAR,
            found_date DATE,
            status VARCHAR,
            source VARCHAR NOT NULL,
            raw_json JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_market_theme_tag (
            fund_code VARCHAR NOT NULL,
            theme VARCHAR NOT NULL,
            classification_confidence VARCHAR NOT NULL CHECK (
                classification_confidence IN (
                    'INDEX_VERIFIED',
                    'HOLDING_VERIFIED',
                    'NAME_ONLY',
                    'UNKNOWN'
                )
            ),
            match_method VARCHAR NOT NULL,
            matched_terms_json JSON,
            evidence_json JSON,
            is_current_holding BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (fund_code, theme, match_method)
        );
        """
    )


def _ensure_theme_observation_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_theme_observation_bucket (
            theme VARCHAR PRIMARY KEY,
            bucket_status VARCHAR NOT NULL CHECK (
                bucket_status IN (
                    'DISCOVERED',
                    'WATCHING',
                    'FOCUS',
                    'REJECTED',
                    'PAUSED'
                )
            ),
            first_seen_date DATE NOT NULL,
            last_seen_date DATE NOT NULL,
            observation_count INTEGER NOT NULL DEFAULT 1,
            total_fund_count INTEGER NOT NULL DEFAULT 0,
            non_holding_count INTEGER NOT NULL DEFAULT 0,
            current_holding_count INTEGER NOT NULL DEFAULT 0,
            name_only_count INTEGER NOT NULL DEFAULT 0,
            index_verified_count INTEGER NOT NULL DEFAULT 0,
            holding_verified_count INTEGER NOT NULL DEFAULT 0,
            unknown_count INTEGER NOT NULL DEFAULT 0,
            evidence_json JSON,
            note VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_theme_observation_daily (
            observation_date DATE NOT NULL,
            theme VARCHAR NOT NULL,
            bucket_status VARCHAR NOT NULL,
            total_fund_count INTEGER NOT NULL DEFAULT 0,
            non_holding_count INTEGER NOT NULL DEFAULT 0,
            current_holding_count INTEGER NOT NULL DEFAULT 0,
            name_only_count INTEGER NOT NULL DEFAULT 0,
            index_verified_count INTEGER NOT NULL DEFAULT 0,
            holding_verified_count INTEGER NOT NULL DEFAULT 0,
            unknown_count INTEGER NOT NULL DEFAULT 0,
            sample_funds_json JSON,
            evidence_json JSON,
            source VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (observation_date, theme)
        );
        """
    )


def _ensure_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_spark_event_log_etf_timestamp ON spark_event_log (etf_code, timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_spark_event_log_event_type ON spark_event_log (event_type, timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_spark_state_snapshot_state ON spark_state_snapshot (state);",
        "CREATE INDEX IF NOT EXISTS idx_spark_weekly_snapshot_date_code ON spark_position_weekly_snapshot (snapshot_date, fund_code);",
        "CREATE INDEX IF NOT EXISTS idx_spark_nav_daily_code_date ON spark_nav_daily (fund_code, nav_date);",
        "CREATE INDEX IF NOT EXISTS idx_spark_holding_signal_date_code ON spark_holding_signal (snapshot_date, fund_code);",
        "CREATE INDEX IF NOT EXISTS idx_spark_backtest_result_run ON spark_backtest_result (run_id, bucket, strategy);",
        "CREATE INDEX IF NOT EXISTS idx_spark_candidates_theme_status ON spark_fund_candidates (theme, candidate_status);",
        "CREATE INDEX IF NOT EXISTS idx_spark_candidate_signal_date_code ON spark_candidate_signal (signal_date, fund_code);",
        "CREATE INDEX IF NOT EXISTS idx_spark_market_universe_market_status ON spark_market_fund_universe (market, status);",
        "CREATE INDEX IF NOT EXISTS idx_spark_market_theme_tag_theme_conf ON spark_market_theme_tag (theme, classification_confidence);",
        "CREATE INDEX IF NOT EXISTS idx_spark_market_theme_tag_holding ON spark_market_theme_tag (is_current_holding);",
        "CREATE INDEX IF NOT EXISTS idx_spark_theme_bucket_status ON spark_theme_observation_bucket (bucket_status, last_seen_date);",
        "CREATE INDEX IF NOT EXISTS idx_spark_theme_daily_date_status ON spark_theme_observation_daily (observation_date, bucket_status);",
    ]
    for sql in statements:
        conn.execute(sql)


def init_db(db_path: Path | str | None = None) -> Path:
    target = Path(db_path).expanduser().resolve() if db_path else DEFAULT_DB_PATH.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(target))
    try:
        _ensure_event_log_schema(conn)
        _ensure_legacy_snapshot_schema(conn)
        _ensure_advisor_schema(conn)
        _ensure_backtest_schema(conn)
        _ensure_candidate_schema(conn)
        _ensure_market_universe_schema(conn)
        _ensure_theme_observation_schema(conn)
        _ensure_indexes(conn)
    finally:
        conn.close()

    return target
