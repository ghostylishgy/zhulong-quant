#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/run_zeta_collect.py
ZetaCollect 独立脚本 — 从 daemon inline 剥离
拉取全市场 Zeta 筹码数据 (龙虎榜 + 融资盘 + 大宗交易)
分批 500 只执行，带节流 sleep
"""

import logging
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = PROJECT_ROOT / '01_engine'
SCRIPTS_DIR = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(ENGINE_ROOT) not in sys.path:
    sys.path.append(str(ENGINE_ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from config.settings import Config
from harvest_state import ensure_ops_pipeline_state_table, sync_harvest_state
from lib.db_gateway import DBGateway
from zeta.zeta_collector_v240 import ZetaCollector, get_last_trade_date

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-5s | %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('zhulong.zeta_run')


def _ensure_fact_zeta_signals_table() -> None:
    """Initialize zeta sink table if absent."""
    with DBGateway(Config.DB_PATH, read_only=False, logger=logger) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_zeta_signals (
                ts_code TEXT NOT NULL,
                trade_date DATE NOT NULL,
                lhb_net DOUBLE DEFAULT 0,
                lhb_buy DOUBLE DEFAULT 0,
                lhb_sell DOUBLE DEFAULT 0,
                seat_count INTEGER DEFAULT 0,
                inst_buy INTEGER DEFAULT 0,
                hot_money INTEGER DEFAULT 0,
                rzye DOUBLE DEFAULT 0,
                rzmre DOUBLE DEFAULT 0,
                margin_delta DOUBLE DEFAULT 0,
                block_trade_vol DOUBLE DEFAULT 0,
                block_trade_premium DOUBLE DEFAULT 0,
                data_source TEXT DEFAULT 'tushare',
                collected_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, trade_date)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fact_zeta_trade_date ON fact_zeta_signals(trade_date)"
        )


def _norm_ts_code(symbol: str) -> str:
    s = str(symbol or '').strip()
    if not s:
        return s
    if '.' in s:
        return s
    return f"{s}.SH" if s.startswith('6') else f"{s}.SZ"


def _load_symbols(trade_date_iso: str) -> list[str]:
    with DBGateway(Config.DB_PATH, read_only=True, logger=logger) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM fact_daily
            WHERE trade_date = CAST(? AS DATE)
            ORDER BY symbol
            """,
            [trade_date_iso],
        ).fetchall()
    return [_norm_ts_code(r[0]) for r in rows if r and r[0]]


def _load_close_map(trade_date_iso: str) -> dict[str, float]:
    close_map: dict[str, float] = {}
    try:
        with DBGateway(Config.DB_PATH, read_only=True, logger=logger) as conn:
            rows = conn.execute(
                """
                SELECT symbol, close
                FROM fact_daily
                WHERE trade_date = CAST(? AS DATE)
                """,
                [trade_date_iso],
            ).fetchall()
        for symbol, close in rows:
            ts_code = _norm_ts_code(symbol)
            if not ts_code:
                continue
            close_map[ts_code] = _to_float(close)
    except Exception as exc:
        logger.warning('fact_daily close map load failed: %s', exc)
    return close_map


def _resolve_target_trade_date() -> date:
    """Resolve target trade date from fact_daily first, then fallback to calendar helper."""
    try:
        with DBGateway(Config.DB_PATH, read_only=True, logger=logger) as conn:
            row = conn.execute("SELECT MAX(trade_date) FROM fact_daily").fetchone()
        val = row[0] if row else None
        if val:
            if isinstance(val, datetime):
                td = val.date()
            elif isinstance(val, date):
                td = val
            else:
                td = datetime.strptime(str(val)[:10], '%Y-%m-%d').date()
            logger.info(f'Zeta target trade date from fact_daily: {td}')
            return td
    except Exception as exc:
        logger.warning(f'Zeta target-date probe from fact_daily failed: {exc}')

    td = get_last_trade_date()
    logger.info(f'Zeta target trade date fallback(calendar): {td}')
    return td


def _to_float(value) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _pick_column(columns: set[str], preferred: str, legacy: str) -> str | None:
    if preferred in columns:
        return preferred
    if legacy in columns:
        return legacy
    return None




def _require_columns(df, source: str, required: list[str]) -> set[str]:
    if df is None:
        raise ValueError(f'[ZetaCollect] {source} DataFrame is None')
    cols = set(df.columns)
    missing = [col for col in required if col not in cols]
    if missing:
        raise ValueError(
            f'[ZetaCollect] {source} ???????: {missing}; available={sorted(cols)}'
        )
    return cols

def _aggregate_top(top_df) -> dict[str, dict]:
    cols = _require_columns(top_df, 'top_list', ['ts_code', 'net_amount'])
    buy_col = _pick_column(cols, 'l_buy', 'buy_amount')
    sell_col = _pick_column(cols, 'l_sell', 'sell_amount')
    if buy_col is None or sell_col is None:
        raise ValueError(
            f'[ZetaCollect] top_list ???????: buy/sell required (l_buy|buy_amount, l_sell|sell_amount); '
            f'available={sorted(cols)}'
        )
    if top_df.empty:
        return {}

    agg: dict[str, dict] = {}
    for row in top_df.to_dict('records'):
        ts_code = _norm_ts_code(row.get('ts_code'))
        if not ts_code:
            continue
        item = agg.setdefault(
            ts_code,
            {
                'lhb_net': 0.0,
                'lhb_buy': 0.0,
                'lhb_sell': 0.0,
                'seat_count': 0,
                'inst_buy': 0,
                'hot_money': 0,
            },
        )
        item['lhb_net'] += _to_float(row.get('net_amount'))
        item['lhb_buy'] += _to_float(row.get(buy_col))
        item['lhb_sell'] += _to_float(row.get(sell_col))
        item['seat_count'] += 1
        if '??' in str(row.get('reason', '')):
            item['inst_buy'] += 1

    for item in agg.values():
        item['hot_money'] = max(int(item['seat_count']) - int(item['inst_buy']), 0)
    return agg


def _aggregate_margin(margin_df) -> dict[str, dict]:
    _require_columns(margin_df, 'margin', ['ts_code', 'rzye', 'rzmre', 'rzche'])
    if margin_df.empty:
        return {}
    agg: dict[str, dict] = {}
    for row in margin_df.to_dict('records'):
        ts_code = _norm_ts_code(row.get('ts_code'))
        if not ts_code:
            continue
        rzye = _to_float(row.get('rzye'))
        rzmre = _to_float(row.get('rzmre'))
        rzche = _to_float(row.get('rzche'))
        agg[ts_code] = {
            'rzye': rzye,
            'rzmre': rzmre,
            'margin_delta': rzmre - rzche,
        }
    return agg


def _aggregate_block(block_df, close_map: dict[str, float]) -> dict[str, dict]:
    if block_df is None:
        return {}
    if block_df.empty:
        return {}

    cols = set(block_df.columns)
    if 'ts_code' not in cols or 'vol' not in cols:
        logger.warning(
            '[ZetaCollect] block_trade missing columns ts_code/vol, skip premium aggregation; available=%s',
            sorted(cols),
        )
        return {}

    price_col = None
    for col in ('price', 'avg_price', 'deal_price'):
        if col in cols:
            price_col = col
            break

    agg: dict[str, dict] = {}
    for row in block_df.to_dict('records'):
        ts_code = _norm_ts_code(row.get('ts_code'))
        if not ts_code:
            continue

        vol = _to_float(row.get('vol'))
        if vol <= 0:
            continue

        price = _to_float(row.get(price_col)) if price_col else 0.0
        if price <= 0 and 'amount' in cols:
            amount = _to_float(row.get('amount'))
            if amount > 0:
                price = amount / vol

        close_px = _to_float(close_map.get(ts_code))
        item = agg.setdefault(
            ts_code,
            {'block_trade_vol': 0.0, 'vwap_num': 0.0, 'vwap_den': 0.0},
        )
        item['block_trade_vol'] += vol
        if price > 0:
            item['vwap_num'] += vol * price
        if close_px > 0:
            item['vwap_den'] += vol * close_px

    out: dict[str, dict] = {}
    for ts_code, item in agg.items():
        numerator = float(item['vwap_num'])
        denominator = float(item['vwap_den'])
        premium = (numerator / denominator - 1.0) if denominator > 0 else 0.0
        out[ts_code] = {
            'block_trade_vol': float(item['block_trade_vol']),
            'block_trade_premium': premium,
        }
    return out


def _persist_rows(trade_date_iso: str, rows: list[tuple]) -> int:
    if not rows:
        return 0

    columns = [
        'ts_code', 'trade_date', 'lhb_net', 'lhb_buy', 'lhb_sell',
        'seat_count', 'inst_buy', 'hot_money', 'rzye', 'rzmre',
        'margin_delta', 'block_trade_vol', 'block_trade_premium',
        'data_source', 'collected_at',
    ]
    frame = pd.DataFrame.from_records(rows, columns=columns)
    duplicate_symbols = frame.loc[frame['ts_code'].duplicated(), 'ts_code'].tolist()
    if duplicate_symbols:
        raise ValueError(f'duplicate Zeta symbols in batch: {duplicate_symbols[:5]}')

    with DBGateway(Config.DB_PATH, read_only=False, logger=logger) as conn:
        relation_name = '_zhulong_zeta_batch'
        conn.register(relation_name, frame)
        conn.execute('BEGIN TRANSACTION')
        try:
            conn.execute(
                'DELETE FROM fact_zeta_signals WHERE trade_date = CAST(? AS DATE)',
                [trade_date_iso],
            )
            conn.execute(
                """
                INSERT INTO fact_zeta_signals (
                    ts_code, trade_date, lhb_net, lhb_buy, lhb_sell,
                    seat_count, inst_buy, hot_money, rzye, rzmre,
                    margin_delta, block_trade_vol, block_trade_premium,
                    data_source, collected_at
                )
                SELECT ts_code, CAST(trade_date AS DATE), lhb_net, lhb_buy, lhb_sell,
                       seat_count, inst_buy, hot_money, rzye, rzmre, margin_delta,
                       block_trade_vol, block_trade_premium, data_source,
                       CAST(collected_at AS TIMESTAMP)
                FROM _zhulong_zeta_batch
                """
            )
            conn.execute('COMMIT')
        except Exception:
            try:
                conn.execute('ROLLBACK')
            except Exception as rollback_exc:
                logger.error('Zeta persist rollback failed: %s', rollback_exc)
            raise
        finally:
            conn.unregister(relation_name)
    return len(rows)


def run_batch_collect(trade_date) -> int:
    trade_date_iso = trade_date.strftime('%Y-%m-%d')
    _ensure_fact_zeta_signals_table()
    ensure_ops_pipeline_state_table(logger=logger)

    symbols = _load_symbols(trade_date_iso)
    total = len(symbols)
    logger.info(f'全市场标的: {total} 只')
    if total == 0:
        sync_harvest_state(trade_date_iso, 'IN_PROGRESS', 'Zeta_EMPTY', logger=logger)
        logger.warning('无标的可采集')
        return 0

    try:
        zc = ZetaCollector()
        if not zc.pro:
            raise RuntimeError('ZetaCollector: TuShare API unavailable')

        top_df, margin_df, block_df = zc.fetch_daily_frames(trade_date)

        top_map = _aggregate_top(top_df)
        margin_map = _aggregate_margin(margin_df)
        close_map = _load_close_map(trade_date_iso)
        block_map = _aggregate_block(block_df, close_map)

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows = []
        for ts_code in symbols:
            top = top_map.get(ts_code, {})
            margin = margin_map.get(ts_code, {})
            block = block_map.get(ts_code, {})
            rows.append(
                (
                    ts_code,
                    trade_date_iso,
                    _to_float(top.get('lhb_net')),
                    _to_float(top.get('lhb_buy')),
                    _to_float(top.get('lhb_sell')),
                    int(top.get('seat_count', 0) or 0),
                    int(top.get('inst_buy', 0) or 0),
                    int(top.get('hot_money', 0) or 0),
                    _to_float(margin.get('rzye')),
                    _to_float(margin.get('rzmre')),
                    _to_float(margin.get('margin_delta')),
                    _to_float(block.get('block_trade_vol')),
                    _to_float(block.get('block_trade_premium')),
                    'tushare',
                    now,
                )
            )

        persisted = _persist_rows(trade_date_iso, rows)
        sync_harvest_state(
            trade_date_iso,
            'IN_PROGRESS',
            'Zeta_DONE',
            logger=logger,
        )
        logger.info(f'Zeta Batch 完成: {trade_date_iso}, persisted={persisted}')
        return persisted
    except Exception as exc:
        sync_harvest_state(
            trade_date_iso,
            'FAILED',
            'Zeta_FAILED',
            logger=logger,
        )
        raise


def main() -> None:
    td = _resolve_target_trade_date()
    logger.info(f'Zeta 目标交易日: {td}')
    run_batch_collect(td)


if __name__ == '__main__':
    main()
