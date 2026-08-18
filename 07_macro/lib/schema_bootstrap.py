#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
07_macro/lib/schema_bootstrap.py
Bootstrap Macro Resonance schema in DuckDB.
"""

from __future__ import annotations

import argparse
import runpy
import logging
import os
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger('zhulong.macro.schema_bootstrap')

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / '.git').exists() or (p / 'storage').exists()),
    _current.parents[2],
)

SQL_PATH = PROJECT_ROOT / '07_macro' / 'sql' / '001_macro_tables.sql'
DEFAULT_DB_PATH = PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'

_loader_ns = runpy.run_path(str(PROJECT_ROOT / '04_governance' / 'lib' / 'core' / 'module_loader.py'))
load_attr_from_path = _loader_ns["load_attr_from_path"]

INDEX_DDL: Dict[str, List[tuple[str, str]]] = {
    'fact_macro_topic_daily': [
        (
            'idx_macro_topic_daily_date',
            'CREATE INDEX IF NOT EXISTS idx_macro_topic_daily_date ON fact_macro_topic_daily(trade_date)',
        ),
        (
            'idx_macro_topic_daily_topic',
            'CREATE INDEX IF NOT EXISTS idx_macro_topic_daily_topic ON fact_macro_topic_daily(topic_type, topic_id, trade_date)',
        ),
    ],
    'fact_macro_top5_daily': [
        (
            'idx_macro_top5_daily_date',
            'CREATE INDEX IF NOT EXISTS idx_macro_top5_daily_date ON fact_macro_top5_daily(trade_date)',
        ),
    ],
}


def _load_dbgateway():
    return load_attr_from_path(
        'db_gateway_01_macro',
        PROJECT_ROOT / '01_engine' / 'lib' / 'db_gateway.py',
        'DBGateway',
    )


def _resolve_db_path(db_path: str | None = None) -> str:
    if db_path:
        return str(db_path)
    env_db = str(os.getenv('DB_PATH', '') or '').strip()
    if env_db:
        return env_db
    return str(DEFAULT_DB_PATH)


def _split_sql_statements(sql_text: str) -> List[str]:
    statements = []
    for part in sql_text.split(';'):
        stmt = part.strip()
        if stmt:
            statements.append(stmt)
    return statements


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE lower(table_name) = lower(?)
          AND lower(column_name) = lower(?)
        LIMIT 1
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row)


def _drop_indexes_for_table(conn, table_name: str) -> int:
    changed = 0
    for index_name, _ in INDEX_DDL.get(table_name, []):
        conn.execute(f'DROP INDEX IF EXISTS {index_name}')
        changed += 1
    return changed


def _rebuild_indexes_for_table(conn, table_name: str) -> int:
    changed = 0
    for _, ddl in INDEX_DDL.get(table_name, []):
        conn.execute(ddl)
        changed += 1
    return changed


def _migrate_drop_prob_column(conn, table_name: str) -> int:
    changed = 0
    has_prob = _column_exists(conn, table_name, 'one_day_trip_prob')
    has_trap = _column_exists(conn, table_name, 'is_event_driven_trap')

    if not has_trap:
        conn.execute(
            f'ALTER TABLE {table_name} ADD COLUMN is_event_driven_trap BOOLEAN DEFAULT FALSE'
        )
        changed += 1
        has_trap = True
        logger.info('Added is_event_driven_trap column on %s', table_name)

    if not has_prob:
        return changed

    if has_trap:
        conn.execute(
            f"""
            UPDATE {table_name}
            SET is_event_driven_trap = CASE
                WHEN COALESCE(CAST(one_day_trip_prob AS DOUBLE), 0) > 0 THEN TRUE
                ELSE COALESCE(is_event_driven_trap, FALSE)
            END
            """
        )
        changed += 1

    changed += _drop_indexes_for_table(conn, table_name)
    conn.execute(f'ALTER TABLE {table_name} DROP COLUMN one_day_trip_prob')
    changed += 1
    logger.info('Dropped legacy one_day_trip_prob column on %s', table_name)
    changed += _rebuild_indexes_for_table(conn, table_name)

    return changed


def bootstrap_schema(db_path: str | None = None) -> int:
    if not SQL_PATH.exists():
        raise FileNotFoundError(f'SQL bootstrap file not found: {SQL_PATH}')

    DBGateway = _load_dbgateway()
    target_db = _resolve_db_path(db_path)

    sql_text = SQL_PATH.read_text(encoding='utf-8')
    statements = _split_sql_statements(sql_text)

    migration_steps = 0
    with DBGateway(target_db, read_only=False, logger=logger) as conn:
        for stmt in statements:
            conn.execute(stmt)

        migration_steps += _migrate_drop_prob_column(conn, 'fact_macro_topic_daily')
        migration_steps += _migrate_drop_prob_column(conn, 'fact_macro_top5_daily')

        try:
            conn.commit()
        except Exception as exc:
            logger.error("Non-fatal: fallback resolution failed: %s", exc, exc_info=True)

    total = len(statements) + migration_steps
    logger.info(
        'Macro schema bootstrap complete | db=%s | ddl=%d | migration_steps=%d',
        target_db,
        len(statements),
        migration_steps,
    )
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description='Bootstrap Macro Resonance schema')
    parser.add_argument('--db-path', type=str, default='', help='Optional DuckDB path override')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    count = bootstrap_schema(db_path=args.db_path or None)
    print(f'Macro schema bootstrap statements executed: {count}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
