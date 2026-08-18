#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_governance/lib/core/safe_writer.py
═══════════════════════════════════════════════════════
DuckDB 防御性写入器 — 全域数据治理核心

骨架来源: 03_tactics/echo_manager.py::duckdb_safe()
重试引擎: 02_brain/decision_engine.py::with_duckdb_retry()
非阻塞范式: 05_shadow/lib/engine.py::persist_fill()

功能:
    1. sanitize_value/row: "" → None, numpy → 原生, NaN → None
    2. normalize_date: YYYYMMDD / YYYY-MM-DD 统一归一化
    3. duckdb_safe: 上下文管理器 (auto-close + 重试)
    4. insert_or_update: 原子化 INSERT ... ON CONFLICT
    5. write_nonblocking: 非阻塞写入 (失败仅 WARN)
═══════════════════════════════════════════════════════
"""

import math
import time
import logging
import random
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from datetime import datetime, date

logger = logging.getLogger('zhulong.safe_writer')

# ==================== Root Detection ====================

_current = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _current.parents if (p / ".git").exists() or (p / "storage").exists()),
    _current.parents[3]
)
DEFAULT_DB_PATH = str(PROJECT_ROOT / "storage" / "database" / "zhulong.duckdb")

# DBGateway contract import (01_engine)
try:
    from .module_loader import load_attr_from_path
except Exception:
    import sys
    _core_dir = Path(__file__).resolve().parent
    if str(_core_dir) not in sys.path:
        sys.path.append(str(_core_dir))
    from module_loader import load_attr_from_path

DBGateway = load_attr_from_path(
    "db_gateway_01",
    PROJECT_ROOT / "01_engine" / "lib" / "db_gateway.py",
    "DBGateway",
)

# date utility contract import (04_governance)
normalize_date = load_attr_from_path(
    "date_util_04",
    PROJECT_ROOT / "04_governance" / "lib" / "core" / "date_util.py",
    "normalize_date",
)


# ==================== Type Sanitization ====================

def sanitize_value(val: Any, col_name: str = "") -> Any:
    """
    DuckDB 强类型归一化:
    - 空字符串 "" → None (消除 TIMESTAMP ConversionError)
    - numpy int64/float64 → Python int/float
    - NaN/Inf → None
    - bool → int (DuckDB 兼容)
    - datetime/date 对象 → ISO-8601 字符串
    """
    if val is None:
        return None

    # 空字符串 → None (根因修复: DuckDB 拒绝 "" 作为 TIMESTAMP)
    if isinstance(val, str):
        if val.strip() == "":
            return None
        return val

    # Numpy 类型强转 (避免 DuckDB 类型不匹配)
    type_name = type(val).__name__
    if type_name in ('int64', 'int32', 'int16', 'int8', 'uint64', 'uint32', 'uint16', 'uint8'):
        return int(val)
    if type_name in ('float64', 'float32', 'float16'):
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f

    # Python float NaN/Inf 检查
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return val

    # bool → int (DuckDB 不支持 Python bool 直接写入某些列)
    if isinstance(val, bool):
        return 1 if val else 0

    # datetime/date → ISO-8601 字符串
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")

    return val


def sanitize_row(data: Dict[str, Any]) -> Dict[str, Any]:
    """对整个 dict 执行 sanitize_value"""
    return {k: sanitize_value(v, k) for k, v in data.items()}



# ==================== Connection Management ====================

@contextmanager
def duckdb_safe(path=None, read_only=False, retries=5, base_delay=0.5):
    """
    DuckDB ??????????:
    - auto-close (finally ??)
    - locked/busy ??????
    - ?? read_only ??
    """
    if path is None:
        path = DEFAULT_DB_PATH
    last_err = None
    for attempt in range(retries):
        try:
            with DBGateway(str(path), read_only=read_only, logger=logger) as conn:
                yield conn
            return
        except GeneratorExit:
            return
        except Exception as e:
            err_msg = str(e).lower()
            if 'locked' in err_msg or 'busy' in err_msg or 'conflict' in err_msg:
                last_err = e
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.3)
                logger.warning(f"DuckDB locked (attempt {attempt+1}/{retries}), wait {delay:.1f}s")
                time.sleep(delay)
            else:
                raise
    if last_err:
        raise last_err
# ==================== Safe Write Operations ====================

def insert_or_update(table: str, data: Dict[str, Any],
                     conflict_col: str = "task_id",
                     db_path: str = None) -> bool:
    """
    原子化 INSERT ... ON CONFLICT DO UPDATE
    自动执行 sanitize_row + retry
    """
    clean = sanitize_row(data)

    columns = ", ".join(clean.keys())
    placeholders = ", ".join(["?" for _ in clean])
    update_cols = [k for k in clean.keys() if k != conflict_col]
    update_clause = ", ".join([f"{k}=?" for k in update_cols])
    update_vals = [clean[k] for k in update_cols]

    sql = f"""
        INSERT INTO {table} ({columns}) VALUES ({placeholders})
        ON CONFLICT({conflict_col}) DO UPDATE SET {update_clause}
    """
    params = list(clean.values()) + update_vals

    try:
        with DBGateway(db_path or DEFAULT_DB_PATH, read_only=False, logger=logger) as conn:
            conn.execute(sql, params)
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"SafeWriter INSERT failed [{table}]: {e}")
        return False


def write_nonblocking(table: str, data: Dict[str, Any],
                      conflict_col: str = "task_id",
                      db_path: str = None) -> bool:
    """非阻塞写入: 失败仅 WARN, 不抛异常"""
    try:
        return insert_or_update(table, data, conflict_col, db_path)
    except Exception as e:
        logger.warning(f"SafeWriter nonblocking write failed [{table}]: {e}")
        return False


def save_audit_safe(packet_data: Dict[str, Any],
                    thinking_data: Dict[str, Any] = None,
                    db_path: str = None) -> bool:
    """审计数据安全落盘 (替代 NexusDB._save_audit_inner)"""
    db = db_path or DEFAULT_DB_PATH
    ok = insert_or_update("nexus_audits", packet_data,
                          conflict_col="task_id", db_path=db)
    if not ok:
        return False
    if thinking_data and thinking_data.get("task_id"):
        write_nonblocking("nexus_thinking_traces", thinking_data,
                          conflict_col="task_id", db_path=db)
    return True


def get_reader(db_path: str = None):
    """获取只读连接上下文管理器"""
    return duckdb_safe(db_path or DEFAULT_DB_PATH, read_only=True)


# ==================== Self-Test ====================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    print("=" * 60)
    print("  SafeWriter Self-Test")
    print("=" * 60)

    print("\n--- sanitize_value ---")
    tests = [
        ("", "empty_str"), ("  ", "whitespace"), (None, "none"),
        (42, "int"), (3.14, "float"), (float('nan'), "nan"),
        (float('inf'), "inf"), (True, "bool_true"), (False, "bool_false"),
    ]
    for val, label in tests:
        result = sanitize_value(val, label)
        print(f"  {label:15s} {str(val):15s} -> {result}")

    print("\n--- normalize_date ---")
    for d in ["20260303", "2026-03-03", "", None]:
        print(f"  {str(d):25s} -> {normalize_date(d)}")

    print("\n--- duckdb_safe connection ---")
    try:
        with duckdb_safe(read_only=True) as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM nexus_audits").fetchone()[0]
            print(f"  nexus_audits rows: {cnt}")
        print("  OK")
    except Exception as e:
        logger.error(f'Critical Logical Gap: {str(e)}', exc_info=True)
    print("\nSafeWriter self-test complete")
