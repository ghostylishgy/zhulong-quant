"""Independent sidecar storage for US radar events."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import sqlite3

from .config import Settings
from .evidence import EvidenceSignal
from .form4 import Form4Transaction
from .industry import SignalTarget
from .quality import EventQuality
from .schema import NormalizedEvent
from .validation import (
    RETRYABLE_DATA_QUALITIES,
    ValidationTask,
    ValidationUpdate,
    build_event_chain_summary,
)


class EventStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_settings(cls, settings: Settings) -> "EventStore":
        return cls(settings.database_path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS mvp_runs (
                    run_id TEXT PRIMARY KEY,
                    data_version TEXT NOT NULL,
                    purpose TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS us_events (
                    event_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    ticker TEXT,
                    company TEXT,
                    cik TEXT,
                    accession TEXT,
                    event_time TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT,
                    summary TEXT,
                    raw_payload TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_us_events_time ON us_events(event_time);
                CREATE INDEX IF NOT EXISTS idx_us_events_type ON us_events(event_type);
                CREATE INDEX IF NOT EXISTS idx_us_events_ticker ON us_events(ticker);

                CREATE TABLE IF NOT EXISTS us_prompt_versions (
                    prompt_version TEXT PRIMARY KEY,
                    purpose TEXT NOT NULL,
                    model_name TEXT,
                    prompt_text TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS us_llm_signals (
                    signal_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    prompt_version TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    input_hash TEXT NOT NULL,
                    output_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES us_events(event_id)
                );

                CREATE TABLE IF NOT EXISTS form4_transactions (
                    tx_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    issuer_ticker TEXT,
                    owner_name TEXT,
                    owner_relationship TEXT,
                    transaction_date TEXT,
                    transaction_code TEXT,
                    acquired_disposed TEXT,
                    shares REAL,
                    price REAL,
                    value REAL,
                    is_open_market INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES us_events(event_id)
                );
                CREATE INDEX IF NOT EXISTS idx_form4_event ON form4_transactions(event_id);
                CREATE INDEX IF NOT EXISTS idx_form4_ticker ON form4_transactions(issuer_ticker);

                CREATE TABLE IF NOT EXISTS event_quality (
                    event_id TEXT PRIMARY KEY,
                    quality_class TEXT NOT NULL,
                    quality_score REAL NOT NULL,
                    form_category TEXT NOT NULL,
                    transmission_window TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES us_events(event_id)
                );

                CREATE TABLE IF NOT EXISTS signal_targets (
                    target_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    target_market TEXT NOT NULL,
                    target_ticker TEXT NOT NULL,
                    target_name TEXT,
                    target_role TEXT NOT NULL,
                    theme TEXT,
                    link_reason TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    transmission_type TEXT,
                    source_event_signals TEXT,
                    enabled_for_research INTEGER NOT NULL DEFAULT 1,
                    enabled_for_trading INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES us_events(event_id)
                );
                CREATE INDEX IF NOT EXISTS idx_signal_targets_event ON signal_targets(event_id);
                CREATE INDEX IF NOT EXISTS idx_signal_targets_symbol ON signal_targets(target_market, target_ticker);
                CREATE INDEX IF NOT EXISTS idx_signal_targets_role ON signal_targets(target_role);
                CREATE INDEX IF NOT EXISTS idx_signal_targets_transmission ON signal_targets(transmission_type);

                CREATE TABLE IF NOT EXISTS signal_validation_results (
                    validation_id TEXT PRIMARY KEY,
                    target_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    target_market TEXT NOT NULL,
                    target_ticker TEXT NOT NULL,
                    horizon_days INTEGER NOT NULL,
                    benchmark TEXT,
                    target_return REAL,
                    benchmark_return REAL,
                    excess_return REAL,
                    data_quality TEXT NOT NULL,
                    measured_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    base_date TEXT,
                    horizon_date TEXT,
                    base_close REAL,
                    horizon_close REAL,
                    price_provider TEXT,
                    excess_vs_csi300 REAL,
                    excess_vs_industry REAL,
                    excess_vs_chain_median REAL,
                    excess_vs_qqq REAL,
                    excess_vs_spy REAL,
                    excess_vs_soxx REAL,
                    primary_excess REAL,
                    signal_label TEXT,
                    direction_label TEXT,
                    event_direction TEXT,
                    direction_source TEXT,
                    window_label TEXT,
                    transmission_type TEXT DEFAULT 'unknown',
                    benchmark_data_quality TEXT,
                    quality_gate TEXT,
                    is_effective_sample INTEGER NOT NULL DEFAULT 0,
                    validation_rule_version TEXT,
                    price_rule_version TEXT,
                    updated_at TEXT,
                    FOREIGN KEY(target_id) REFERENCES signal_targets(target_id)
                );
                CREATE INDEX IF NOT EXISTS idx_validation_event ON signal_validation_results(event_id);
                CREATE INDEX IF NOT EXISTS idx_validation_symbol ON signal_validation_results(target_market, target_ticker);

                CREATE TABLE IF NOT EXISTS event_chain_validation_results (
                    chain_validation_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    theme TEXT NOT NULL,
                    target_market TEXT NOT NULL,
                    transmission_type TEXT NOT NULL,
                    horizon_days INTEGER NOT NULL,
                    total_targets INTEGER NOT NULL,
                    effective_targets INTEGER NOT NULL,
                    significant_targets INTEGER NOT NULL,
                    significant_share REAL,
                    median_primary_excess REAL,
                    signal_label TEXT NOT NULL,
                    event_direction TEXT NOT NULL,
                    direction_source TEXT NOT NULL,
                    direction_confidence REAL NOT NULL,
                    direction_label TEXT NOT NULL,
                    data_quality TEXT NOT NULL,
                    source_validation_versions TEXT NOT NULL,
                    chain_rule_version TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES us_events(event_id)
                );
                CREATE INDEX IF NOT EXISTS idx_chain_validation_event
                    ON event_chain_validation_results(event_id);
                CREATE INDEX IF NOT EXISTS idx_chain_validation_group
                    ON event_chain_validation_results(theme, target_market, transmission_type, horizon_days);

                CREATE TABLE IF NOT EXISTS research_actions (
                    action_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    target_id TEXT,
                    action_type TEXT NOT NULL,
                    action_status TEXT NOT NULL,
                    rationale TEXT NOT NULL,
                    owner TEXT,
                    due_at TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES us_events(event_id),
                    FOREIGN KEY(target_id) REFERENCES signal_targets(target_id)
                );

                CREATE TABLE IF NOT EXISTS us_reports (
                    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_path TEXT NOT NULL,
                    report_type TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    event_count INTEGER NOT NULL
                );
                """
            )
            self._ensure_column(conn, "signal_targets", "transmission_type", "TEXT")
            self._ensure_column(conn, "signal_targets", "source_event_signals", "TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_targets_transmission ON signal_targets(transmission_type)")
            for column, column_type in {
                "base_date": "TEXT",
                "horizon_date": "TEXT",
                "base_close": "REAL",
                "horizon_close": "REAL",
                "price_provider": "TEXT",
                "excess_vs_csi300": "REAL",
                "excess_vs_industry": "REAL",
                "excess_vs_chain_median": "REAL",
                "excess_vs_qqq": "REAL",
                "excess_vs_spy": "REAL",
                "excess_vs_soxx": "REAL",
                "primary_excess": "REAL",
                "signal_label": "TEXT",
                "direction_label": "TEXT",
                "event_direction": "TEXT",
                "direction_source": "TEXT",
                "direction_confidence": "REAL",
                "window_label": "TEXT",
                "transmission_type": "TEXT DEFAULT 'unknown'",
                "benchmark_data_quality": "TEXT",
                "quality_gate": "TEXT",
                "is_effective_sample": "INTEGER NOT NULL DEFAULT 0",
                "validation_rule_version": "TEXT",
                "price_rule_version": "TEXT",
                "updated_at": "TEXT",
            }.items():
                self._ensure_column(conn, "signal_validation_results", column, column_type)
            self._backfill_validation_metadata(conn)

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def _backfill_validation_metadata(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            UPDATE signal_validation_results
            SET transmission_type = COALESCE(
                (
                    SELECT st.transmission_type
                    FROM signal_targets st
                    WHERE st.target_id = signal_validation_results.target_id
                ),
                'unknown'
            )
            WHERE transmission_type IS NULL OR transmission_type = 'unknown'
            """
        )
        conn.execute(
            """
            UPDATE signal_validation_results
            SET direction_confidence = CASE direction_source
                WHEN 'form4_transaction' THEN 0.95
                WHEN 'evidence_heuristic' THEN 0.55
                WHEN '8k_text_heuristic' THEN 0.40
                WHEN 'evidence_neutral' THEN 0.35
                ELSE 0.0
            END
            WHERE direction_confidence IS NULL
            """
        )

    def create_mvp_run(self, data_version: str, purpose: str) -> str:
        now = datetime.now(timezone.utc).isoformat()
        run_id = hashlib.sha256(f"{data_version}|{purpose}|{now}".encode("utf-8")).hexdigest()[:16]
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO mvp_runs (run_id, data_version, purpose, status, created_at) VALUES (?, ?, ?, ?, ?)",
                (run_id, data_version, purpose, "ACTIVE", now),
            )
        return run_id

    def reset_derived_data(self) -> dict[str, int]:
        tables = ["research_actions", "event_chain_validation_results", "signal_validation_results", "signal_targets", "event_quality", "form4_transactions", "us_llm_signals", "us_reports"]
        deleted: dict[str, int] = {}
        with self.connect() as conn:
            for table in tables:
                before = conn.total_changes
                conn.execute(f"DELETE FROM {table}")
                deleted[table] = conn.total_changes - before
        return deleted

    def upsert_events(self, events: list[NormalizedEvent]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        with self.connect() as conn:
            for event in events:
                row = asdict(event)
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT OR IGNORE INTO us_events (
                        event_id, source, event_type, ticker, company, cik,
                        accession, event_time, title, url, summary, raw_payload,
                        created_at
                    ) VALUES (
                        :event_id, :source, :event_type, :ticker, :company, :cik,
                        :accession, :event_time, :title, :url, :summary,
                        :raw_payload, :created_at
                    )
                    """,
                    {**row, "created_at": now},
                )
                if conn.total_changes > before:
                    inserted += 1
        return inserted

    def known_event_accessions(self) -> set[str]:
        """Return accessions already stored by this sidecar for completeness audits."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT accession
                FROM us_events
                WHERE accession IS NOT NULL AND TRIM(accession) != ''
                """
            ).fetchall()
        return {str(row[0]).strip() for row in rows}

    def recent_events(self, since: datetime, event_type: str | None = None) -> list[NormalizedEvent]:
        since_text = since.astimezone(timezone.utc).isoformat()
        params: list[str] = [since_text]
        clause = "WHERE event_time >= ?"
        if event_type:
            clause += " AND event_type = ?"
            params.append(event_type)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT event_id, source, event_type, ticker, company, cik,
                       accession, event_time, title, url, summary, raw_payload
                FROM us_events
                {clause}
                ORDER BY event_time DESC, created_at DESC
                """,
                tuple(params),
            ).fetchall()
        return [_event_from_row(row) for row in rows]

    def all_events_without_signals(self, since: datetime) -> list[NormalizedEvent]:
        since_text = since.astimezone(timezone.utc).isoformat()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT e.event_id, e.source, e.event_type, e.ticker, e.company,
                       e.cik, e.accession, e.event_time, e.title, e.url,
                       e.summary, e.raw_payload
                FROM us_events e
                LEFT JOIN us_llm_signals s ON s.event_id = e.event_id
                WHERE e.event_time >= ? AND s.signal_id IS NULL
                ORDER BY e.event_time DESC, e.created_at DESC
                """,
                (since_text,),
            ).fetchall()
        return [_event_from_row(row) for row in rows]

    def upsert_prompt_version(self, record: dict) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            before = conn.total_changes
            conn.execute(
                """
                INSERT OR IGNORE INTO us_prompt_versions (
                    prompt_version, purpose, model_name, prompt_text, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (record["prompt_version"], record["purpose"], record.get("model_name"), record["prompt_text"], now),
            )
            return 1 if conn.total_changes > before else 0

    def insert_signal(self, signal: EvidenceSignal) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            before = conn.total_changes
            conn.execute(
                """
                INSERT OR IGNORE INTO us_llm_signals (
                    signal_id, event_id, prompt_version, model_name,
                    input_hash, output_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (signal.signal_id, signal.event_id, signal.prompt_version, signal.model_name, signal.input_hash, signal.output_json, now),
            )
            return 1 if conn.total_changes > before else 0

    def insert_form4_transactions(self, txs: list[Form4Transaction]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        with self.connect() as conn:
            for tx in txs:
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT OR IGNORE INTO form4_transactions (
                        tx_id, event_id, issuer_ticker, owner_name, owner_relationship,
                        transaction_date, transaction_code, acquired_disposed, shares,
                        price, value, is_open_market, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tx.tx_id, tx.event_id, tx.issuer_ticker, tx.owner_name,
                        tx.owner_relationship, tx.transaction_date, tx.transaction_code,
                        tx.acquired_disposed, tx.shares, tx.price, tx.value,
                        1 if tx.is_open_market else 0, now,
                    ),
                )
                if conn.total_changes > before:
                    inserted += 1
        return inserted

    def form4_open_market_count(self, event_id: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM form4_transactions WHERE event_id = ? AND is_open_market = 1",
                (event_id,),
            ).fetchone()
        return int(row[0])

    def form4_transaction_count(self, event_id: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM form4_transactions WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        return int(row[0])

    def upsert_event_quality(self, quality: EventQuality) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            before = conn.total_changes
            conn.execute(
                """
                INSERT OR REPLACE INTO event_quality (
                    event_id, quality_class, quality_score, form_category,
                    transmission_window, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    quality.event_id, quality.quality_class, quality.quality_score,
                    quality.form_category, quality.transmission_window, quality.reason, now,
                ),
            )
            return 1 if conn.total_changes > before else 0

    def upsert_signal_targets(self, targets: list[SignalTarget]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        with self.connect() as conn:
            for target in targets:
                target_id = _target_id(target)
                before = conn.total_changes
                conn.execute(
                    """
                    INSERT OR IGNORE INTO signal_targets (
                        target_id, event_id, target_market, target_ticker,
                        target_name, target_role, theme, link_reason, confidence,
                        transmission_type, source_event_signals,
                        enabled_for_research, enabled_for_trading, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?)
                    """,
                    (
                        target_id, target.event_id, target.target_market, target.target_ticker,
                        target.target_name, target.target_role, target.theme, target.link_reason,
                        target.confidence, target.transmission_type, target.source_event_signals, now,
                    ),
                )
                if conn.total_changes > before:
                    inserted += 1
        return inserted

    def pending_validation_targets(self, horizons: list[int], limit: int) -> list[dict]:
        horizons = list(dict.fromkeys(horizons))
        if not horizons:
            return []
        placeholders = ",".join("?" for _ in horizons)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT st.target_id, st.event_id, st.target_market, st.target_ticker
                FROM signal_targets st
                WHERE st.enabled_for_research = 1
                AND EXISTS (
                    SELECT 1 FROM us_events e WHERE e.event_id = st.event_id
                )
                AND (
                    SELECT COUNT(DISTINCT vr.horizon_days)
                    FROM signal_validation_results vr
                    WHERE vr.target_id = st.target_id
                      AND vr.horizon_days IN ({placeholders})
                ) < ?
                ORDER BY st.created_at DESC
                LIMIT ?
                """,
                (*horizons, len(horizons), limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def insert_validation_task(self, task: ValidationTask) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            before = conn.total_changes
            conn.execute(
                """
                INSERT OR IGNORE INTO signal_validation_results (
                    validation_id, target_id, event_id, target_market, target_ticker,
                    horizon_days, benchmark, target_return, benchmark_return,
                    excess_return, data_quality, measured_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?)
                """,
                (
                    task.validation_id, task.target_id, task.event_id, task.target_market,
                    task.target_ticker, task.horizon_days, task.benchmark,
                    task.data_quality, task.measured_at, now,
                ),
            )
            return 1 if conn.total_changes > before else 0

    def validation_work_items(self, limit: int, include_complete: bool = False, market: str | None = None) -> list[dict]:
        if include_complete:
            quality_clause = "1 = 1"
            quality_params: tuple[object, ...] = ()
        else:
            placeholders = ",".join("?" for _ in RETRYABLE_DATA_QUALITIES)
            quality_clause = f"vr.data_quality IN ({placeholders})"
            quality_params = tuple(RETRYABLE_DATA_QUALITIES)
        market_clause = "AND vr.target_market = ?" if market else ""
        params: tuple[object, ...] = (*quality_params, market, limit) if market else (*quality_params, limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT vr.validation_id, vr.target_id, vr.event_id,
                       vr.target_market, vr.target_ticker, vr.horizon_days,
                       vr.benchmark, vr.data_quality,
                       e.event_time, e.event_type, e.ticker AS source_ticker,
                       e.title AS event_title, e.summary AS event_summary,
                       q.quality_class, q.quality_score, q.reason AS quality_reason,
                       st.theme, st.transmission_type AS target_transmission_type,
                       (
                           SELECT s.output_json
                           FROM us_llm_signals s
                           WHERE s.event_id = vr.event_id
                           ORDER BY s.created_at DESC
                           LIMIT 1
                       ) AS evidence_output_json,
                       (
                           SELECT CASE
                               WHEN SUM(CASE WHEN acquired_disposed = 'A' THEN value ELSE 0 END) >
                                    SUM(CASE WHEN acquired_disposed = 'D' THEN value ELSE 0 END)
                               THEN 'bullish'
                               WHEN SUM(CASE WHEN acquired_disposed = 'D' THEN value ELSE 0 END) >
                                    SUM(CASE WHEN acquired_disposed = 'A' THEN value ELSE 0 END)
                               THEN 'bearish'
                               ELSE NULL
                           END
                           FROM form4_transactions ft
                           WHERE ft.event_id = vr.event_id AND ft.is_open_market = 1
                       ) AS net_form4_direction
                FROM signal_validation_results vr
                JOIN us_events e ON e.event_id = vr.event_id
                JOIN signal_targets st ON st.target_id = vr.target_id
                LEFT JOIN event_quality q ON q.event_id = vr.event_id
                WHERE {quality_clause}
                {market_clause}
                ORDER BY e.event_time DESC, vr.horizon_days, vr.target_market, vr.target_ticker
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def cn_chain_targets(self, event_id: str, theme: str | None, exclude_ticker: str | None = None) -> list[dict]:
        exclude_clause = "AND target_ticker != ?" if exclude_ticker else ""
        params: tuple[object, ...] = (event_id, theme, exclude_ticker) if exclude_ticker else (event_id, theme)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT target_ticker
                FROM signal_targets
                WHERE event_id = ?
                  AND target_market = 'CN'
                  AND COALESCE(theme, '') = COALESCE(?, '')
                  AND enabled_for_research = 1
                  {exclude_clause}
                ORDER BY target_ticker
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def update_validation_result(self, update: ValidationUpdate) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            before = conn.total_changes
            conn.execute(
                """
                UPDATE signal_validation_results
                SET benchmark = ?,
                    target_return = ?,
                    benchmark_return = ?,
                    excess_return = ?,
                    data_quality = ?,
                    measured_at = ?,
                    base_date = ?,
                    horizon_date = ?,
                    base_close = ?,
                    horizon_close = ?,
                    price_provider = ?,
                    excess_vs_csi300 = ?,
                    excess_vs_industry = ?,
                    excess_vs_chain_median = ?,
                    excess_vs_qqq = ?,
                    excess_vs_spy = ?,
                    excess_vs_soxx = ?,
                    primary_excess = ?,
                    signal_label = ?,
                    direction_label = ?,
                    event_direction = ?,
                    direction_source = ?,
                    direction_confidence = ?,
                    window_label = ?,
                    transmission_type = ?,
                    benchmark_data_quality = ?,
                    quality_gate = ?,
                    is_effective_sample = ?,
                    validation_rule_version = ?,
                    price_rule_version = ?,
                    updated_at = ?
                WHERE validation_id = ?
                """,
                (
                    update.benchmark,
                    update.target_return,
                    update.benchmark_return,
                    update.excess_return,
                    update.data_quality,
                    update.measured_at,
                    update.base_date,
                    update.horizon_date,
                    update.base_close,
                    update.horizon_close,
                    update.price_provider,
                    update.excess_vs_csi300,
                    update.excess_vs_industry,
                    update.excess_vs_chain_median,
                    update.excess_vs_qqq,
                    update.excess_vs_spy,
                    update.excess_vs_soxx,
                    update.primary_excess,
                    update.signal_label,
                    update.direction_label,
                    update.event_direction,
                    update.direction_source,
                    update.direction_confidence,
                    update.window_label,
                    update.transmission_type,
                    update.benchmark_data_quality,
                    update.quality_gate,
                    update.is_effective_sample,
                    update.validation_rule_version,
                    update.price_rule_version,
                    now,
                    update.validation_id,
                ),
            )
            return conn.total_changes - before

    def refresh_event_chain_summaries(self) -> int:
        with self.connect() as conn:
            source_rows = conn.execute(
                """
                SELECT vr.event_id, vr.target_id, vr.target_market,
                       vr.horizon_days, vr.primary_excess, vr.signal_label,
                       vr.event_direction, vr.direction_source,
                       vr.direction_confidence, vr.data_quality,
                       vr.is_effective_sample, vr.validation_rule_version,
                       st.theme,
                       st.transmission_type AS target_transmission_type
                FROM signal_validation_results vr
                JOIN signal_targets st ON st.target_id = vr.target_id
                WHERE st.enabled_for_research = 1
                ORDER BY vr.event_id, st.theme, vr.target_market,
                         st.transmission_type, vr.horizon_days, vr.target_id
                """
            ).fetchall()

        groups: dict[tuple[str, str, str, str, int], list[dict]] = {}
        for row in source_rows:
            item = dict(row)
            key = (
                str(item["event_id"]),
                str(item.get("theme") or "unknown"),
                str(item["target_market"]),
                str(item.get("target_transmission_type") or "unknown"),
                int(item["horizon_days"]),
            )
            groups.setdefault(key, []).append(item)

        summaries = [build_event_chain_summary(rows) for rows in groups.values()]
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            for summary in summaries:
                conn.execute(
                    """
                    INSERT INTO event_chain_validation_results (
                        chain_validation_id, event_id, theme, target_market,
                        transmission_type, horizon_days, total_targets,
                        effective_targets, significant_targets, significant_share,
                        median_primary_excess, signal_label, event_direction,
                        direction_source, direction_confidence, direction_label,
                        data_quality, source_validation_versions,
                        chain_rule_version, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(chain_validation_id) DO UPDATE SET
                        total_targets = excluded.total_targets,
                        effective_targets = excluded.effective_targets,
                        significant_targets = excluded.significant_targets,
                        significant_share = excluded.significant_share,
                        median_primary_excess = excluded.median_primary_excess,
                        signal_label = excluded.signal_label,
                        event_direction = excluded.event_direction,
                        direction_source = excluded.direction_source,
                        direction_confidence = excluded.direction_confidence,
                        direction_label = excluded.direction_label,
                        data_quality = excluded.data_quality,
                        source_validation_versions = excluded.source_validation_versions,
                        chain_rule_version = excluded.chain_rule_version,
                        updated_at = excluded.updated_at
                    """,
                    (
                        summary.chain_validation_id,
                        summary.event_id,
                        summary.theme,
                        summary.target_market,
                        summary.transmission_type,
                        summary.horizon_days,
                        summary.total_targets,
                        summary.effective_targets,
                        summary.significant_targets,
                        summary.significant_share,
                        summary.median_primary_excess,
                        summary.signal_label,
                        summary.event_direction,
                        summary.direction_source,
                        summary.direction_confidence,
                        summary.direction_label,
                        summary.data_quality,
                        summary.source_validation_versions,
                        summary.chain_rule_version,
                        now,
                    ),
                )
        return len(summaries)

    def event_chain_report_rows(self, since: datetime, limit: int = 160) -> list[sqlite3.Row]:
        since_text = since.astimezone(timezone.utc).isoformat()
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT e.event_time, e.ticker AS source_ticker, e.event_type,
                       cv.theme, cv.target_market, cv.transmission_type,
                       cv.horizon_days, cv.total_targets, cv.effective_targets,
                       cv.significant_targets, cv.significant_share,
                       cv.median_primary_excess, cv.signal_label,
                       cv.event_direction, cv.direction_source,
                       cv.direction_confidence, cv.direction_label,
                       cv.data_quality, cv.source_validation_versions,
                       cv.chain_rule_version
                FROM event_chain_validation_results cv
                JOIN us_events e ON e.event_id = cv.event_id
                WHERE e.event_time >= ?
                ORDER BY e.event_time DESC, cv.horizon_days,
                         cv.target_market, cv.theme, cv.transmission_type
                LIMIT ?
                """,
                (since_text, limit),
            ).fetchall()

    def report_rows(self, since: datetime, limit: int = 120) -> list[sqlite3.Row]:
        since_text = since.astimezone(timezone.utc).isoformat()
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT e.event_time, e.ticker AS source_ticker, e.event_type,
                       e.company, e.title, e.url,
                       q.quality_class, q.quality_score, q.transmission_window,
                       st.theme, st.target_market, st.target_ticker, st.target_name,
                       st.target_role, st.transmission_type, st.link_reason, st.confidence,
                       vr.data_quality AS validation_quality,
                       vr.primary_excess, vr.signal_label, vr.direction_label, vr.window_label,
                       vr.event_direction, vr.direction_source,
                       vr.direction_confidence,
                       vr.quality_gate, vr.is_effective_sample
                FROM us_events e
                LEFT JOIN event_quality q ON q.event_id = e.event_id
                LEFT JOIN signal_targets st ON st.event_id = e.event_id
                LEFT JOIN signal_validation_results vr
                  ON vr.target_id = st.target_id AND vr.horizon_days = 1
                WHERE e.event_time >= ?
                ORDER BY e.event_time DESC, st.target_market, st.target_ticker
                LIMIT ?
                """,
                (since_text, limit),
            ).fetchall()

    def insert_report(self, report_path: str, report_type: str, generated_at: str, event_count: int) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO us_reports (report_path, report_type, generated_at, event_count) VALUES (?, ?, ?, ?)",
                (report_path, report_type, generated_at, event_count),
            )


def _event_from_row(row: sqlite3.Row) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=row["event_id"], source=row["source"], event_type=row["event_type"],
        ticker=row["ticker"], company=row["company"], cik=row["cik"],
        accession=row["accession"], event_time=row["event_time"], title=row["title"],
        url=row["url"], summary=row["summary"], raw_payload=row["raw_payload"],
    )


def _target_id(target: SignalTarget) -> str:
    material = "|".join([
        target.event_id, target.target_market, target.target_ticker,
        target.target_role, target.theme or "", target.transmission_type or "",
    ])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()
