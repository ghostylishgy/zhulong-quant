# Macro Resonance Engine V1 Runbook

## 1. Purpose
07_macro is the V1 macro resonance sidecar for topic heat ranking and macro-micro signal overlay.

V1 pipeline phases:
1. Phase 3: topic mapping plus low-frequency cache (dim_macro_topic_member)
2. Phase 4: factor aggregation plus scoring plus top5
3. Phase 5: anti-fake rules
4. Phase 6: macro-micro overlay labeling

## 2. Single Trigger Command
Run one-shot full pipeline:

```bash
python3 scripts/run_macro_resonance.py --trade_date 2026-04-07
```

Optional focus symbol output:

```bash
python3 scripts/run_macro_resonance.py --trade_date 2026-04-07 --focus_symbol 688256.SH
```

## 3. Data Dependencies

### 3.1 External Tushare APIs
V1 calls these endpoints:
- concept
- concept_detail
- moneyflow
- limit_list_d

### 3.2 Local DuckDB Tables
- fact_stock_basic
- fact_daily
- nexus_audits
- dim_macro_topic_member
- fact_macro_topic_daily
- fact_macro_top5_daily
- fact_macro_micro_overlay

## 4. Low-Frequency Cache Rule
concept_detail is high-cost and low-frequency data.

Refresh mapping only when either condition is true:
1. dim_macro_topic_member is empty
2. latest updated_at is older than 7 days

Otherwise, mapping is loaded from local cache.

## 5. Force Clean Cache and Rerun

### 5.1 Clear mapping cache
```bash
python3 - <<'PY'
import sys
from pathlib import Path
PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT / '01_engine' / 'lib') not in sys.path:
    sys.path.append(str(PROJECT_ROOT / '01_engine' / 'lib'))
from db_gateway import DBGateway
with DBGateway(str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'), read_only=False) as conn:
    conn.execute('DELETE FROM dim_macro_topic_member')
    conn.commit()
print('dim_macro_topic_member cleared')
PY
```

### 5.2 Optional: clear one trade date output
```bash
python3 - <<'PY'
import sys
from pathlib import Path
trade_date = '2026-04-07'
PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT / '01_engine' / 'lib') not in sys.path:
    sys.path.append(str(PROJECT_ROOT / '01_engine' / 'lib'))
from db_gateway import DBGateway
with DBGateway(str(PROJECT_ROOT / 'storage' / 'database' / 'zhulong.duckdb'), read_only=False) as conn:
    for table in ['fact_macro_topic_daily', 'fact_macro_top5_daily', 'fact_macro_micro_overlay']:
        conn.execute(f"DELETE FROM {table} WHERE trade_date = CAST(? AS DATE)", [trade_date])
    conn.commit()
print('macro outputs cleared for', trade_date)
PY
```

### 5.3 Rerun
```bash
python3 scripts/run_macro_resonance.py --trade_date 2026-04-07
```

## 6. Lightweight Health Probe

```bash
python3 scripts/check_macro_resonance.py --trade_date 2026-04-07
```

Probe checks:
- top5 rows exist in fact_macro_top5_daily
- anti-fake ratio is not abnormally high
- anti-fake flagged count in top5 is not abnormally high

## 7. Troubleshooting Notes
- If Tushare call fails, verify token and network/DNS.
- If DuckDB lock conflict appears, rerun after lock release.
- If top5 is empty, verify upstream fact_daily and Tushare daily data availability.
