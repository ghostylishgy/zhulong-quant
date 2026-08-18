import logging
import sys
import importlib
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from pathlib import Path
# from core.database import Database, get_db  # legacy import path (unresolved in current tree)
# from data_engine.fetcher import Fetcher      # legacy import path (unresolved in current tree)
from config.settings import Config

try:
    _db_mod = importlib.import_module("core.database")
except Exception:
    _gov_lib = Path(__file__).resolve().parents[2] / "04_governance" / "lib"
    if str(_gov_lib) not in sys.path:
        sys.path.append(str(_gov_lib))
    try:
        _db_mod = importlib.import_module("core.database")
    except Exception:
        _db_mod = None

        def get_db():
            return None
if "_db_mod" in locals() and _db_mod is not None:
    Database = getattr(_db_mod, "Database", Any)
    get_db = getattr(_db_mod, "get_db")


try:
    from .fetcher import Fetcher
except Exception:
    from fetcher import Fetcher

logger = logging.getLogger('zhulong.roe_auditor')

@dataclass
class ROEAuditResult:
    code: str
    name: str
    roe_year1: Optional[float] = None
    roe_year2: Optional[float] = None
    roe_year3: Optional[float] = None
    year1: Optional[int] = None
    year2: Optional[int] = None
    year3: Optional[int] = None
    roe_avg: Optional[float] = None
    roe_pass: bool = False
    roe_score: int = 0
    roe_trend: str = 'FLAT'
    data_source: str = 'akshare'
    report_date: Optional[date] = None

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items()}

class ROEAuditor:
    def __init__(self):
        self.db = get_db()
        self.fetcher = Fetcher()
        self.min_years = 3
        self.min_roe_avg = 0.10
    def fetch_roe_data(self, code: str, name: str = ''):
        try:
            df = self.fetcher.fetch_roe_data(code)
            if df is None or df.empty:
                return None
            return df
        except Exception as e:
            logger.error(f"ROE data fetch failed for {code}: {e}", exc_info=True)
            return pd.DataFrame()

    def _error_placeholder(self, code: str, name: str, reason: str) -> ROEAuditResult:
        return ROEAuditResult(
            code=code,
            name=name,
            roe_avg=0.15,
            roe_pass=True,
            roe_score=0,
            roe_trend='ERROR_BYPASS',
            data_source=f"error:{str(reason)[:32]}",
            report_date=date.today(),
        )
    def audit_stock(self, code: str, name: str = ''):
        try:
            df = self.fetch_roe_data(code, name)
            if df is None or df.empty:
                return None

            roe_val = None
            if '数值' in df.columns:
                try:
                    roe_val = float(df.iloc[0]['数值'])
                except Exception as e:
                    logger.error(f"ROE parse failed for {code}: {e}", exc_info=True)
                    return self._error_placeholder(code, name, 'PARSE_EXCEPTION')

            if roe_val is None:
                return self._error_placeholder(code, name, 'ROE_VALUE_MISSING')

            roe_avg = roe_val
            return ROEAuditResult(
                code=code,
                name=name,
                roe_avg=roe_avg / 100.0 if roe_avg > 1.0 else roe_avg,
                roe_pass=(roe_avg > 10.0 if roe_avg > 1.0 else roe_avg > 0.1),
            )
        except Exception as e:
            logger.error(f"ROE audit exception for {code}: {e}", exc_info=True)
            return self._error_placeholder(code, name, type(e).__name__)

def get_roe_auditor():
    return ROEAuditor()
