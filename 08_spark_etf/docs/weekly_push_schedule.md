# Spark ETF Weekly Push Schedule

## Goal

Spark ETF uses a two-step weekly loop:

1. Saturday evening: remind the user to fill the weekly Tiantian Fund holding snapshot.
2. Sunday evening: analyze the latest snapshot and push holding actions.

The Sunday review must not use an old snapshot. If the latest imported snapshot is not in the current ISO week, the system pushes a skip notice instead of buy/sell suggestions.

## Cron Template

Install on node 121 after `08_spark_etf` is tracked and deployed:

```cron
0 20 * * 6 cd /root/quant_project/08_spark_etf && PYTHONPATH=src python3 scripts/push_weekly_snapshot_reminder.py >> logs/weekly_push_cron.log 2>&1
0 20 * * 0 cd /root/quant_project/08_spark_etf && PYTHONPATH=src python3 scripts/run_weekly_holding_review.py --push >> logs/weekly_push_cron.log 2>&1
```

## Manual Smoke Tests

Saturday reminder without push:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/push_weekly_snapshot_reminder.py --no-push
```

Sunday analysis without push and without NAV refresh:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/run_weekly_holding_review.py --no-refresh-nav
```

Sunday analysis with push:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/run_weekly_holding_review.py --push
```

## Push Channel

Both scripts reuse the main project `utils/pusher.py` and require `PUSHPLUS_TOKEN` in the project environment.

## Safety Rules

- Saturday script only reminds; it does not analyze holdings.
- Sunday script checks the latest snapshot date before generating advice.
- If the latest snapshot is stale, Sunday pushes a skip notice.
- `sell_only` holdings, such as `481001.OF`, do not participate in DCA suggestions.
- All output remains `ADVISORY_ONLY`.
