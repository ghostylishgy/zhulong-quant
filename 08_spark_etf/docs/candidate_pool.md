# Spark ETF Candidate Pool

## Positioning

The candidate pool is a watch-only opportunity radar. It is separate from the holding discipline line.

The pool is for funds not currently held. During every refresh, the service reads the latest
`spark_position_weekly_snapshot` and excludes any configured candidate whose `fund_code` already
appears in the current holding snapshot with positive shares. Excluded rows are not written as
candidate signals.

It may emit:

- observe
- low-zone watch
- low-valuation watch
- trend-turn watch
- low-zone rebound watch
- data-insufficient

It must not emit:

- buy amount
- sell instruction for existing holdings
- change to weekly holding advice
- automatic push to the user

## Tables

- `spark_fund_universe`: configured watchable fund universe.
- `spark_fund_candidates`: active manual candidate list and thesis.
- `spark_candidate_signal`: daily/weekly candidate watch signals.

These tables do not update `spark_position_weekly_snapshot`, `spark_holding_signal`, or `spark_action_review`.

## Config

Candidate seeds live in:

```text
08_spark_etf/config/candidates.yaml
```

The first version is manual-seed only. There is no full-market scan yet.

Manual seeds should be selected from public fund metadata such as Tushare `fund_basic`, then verified
against the latest holding snapshot. If a configured seed becomes a real holding later, it is
automatically removed from candidate signals on the next refresh.

## Signal Rules

The first scaffold uses public NAV history and optional valuation proxies:

- less than `min_history_points`: `DATA_INSUFFICIENT`
- low valuation percentile: `LOW_VALUATION_WATCH`
- deep 60-day drawdown plus 20-day rebound: `LOW_ZONE_REBOUND_WATCH`
- 60-day drawdown below threshold: `LOW_ZONE_WATCH`
- positive 20-day return plus MA20 above MA60: `TREND_TURN_WATCH`
- otherwise: `OBSERVE`

If valuation proxy is missing, the service still uses NAV trend and drawdown, but it must not claim a strong low-valuation signal.

## Manual Run

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_candidates.py --no-refresh-nav
```

Refresh candidate NAV history and signals:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_candidates.py --lookback-days 180
```

Use cached NAV history only:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_candidates.py --no-refresh-nav
```

## Cron Template

Run after the nightly NAV history refresh. This job is silent and does not push:

```cron
45 21 * * * cd /root/quant_project/08_spark_etf && PYTHONPATH=src python3 scripts/refresh_candidates.py --no-refresh-nav >> logs/candidate_pool_cron.log 2>&1
```

Mock smoke test:

```bash
cd /root/quant_project/08_spark_etf
MOCK_TUSHARE=1 PYTHONPATH=src python3 scripts/refresh_candidates.py --db /tmp/spark_candidates_mock.duckdb --lookback-days 60
```

## Next Steps

After this scaffold is stable:

1. Review the non-held seeds in `candidates.yaml` and remove weak duplicates after observing signal quality.
2. Add trusted external valuation proxies for Hong Kong innovation drug, QDII, gold, and overseas tech.
3. Add a candidate-only weekly report after signal noise is understood.
4. Only then consider a full-market fund screener.
