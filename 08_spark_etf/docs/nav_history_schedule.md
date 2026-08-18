# Spark ETF NAV History Refresh

## Goal

Keep `spark_nav_daily` populated with public fund NAV data from Tushare, so weekly holding signals can use trend and drawdown evidence.

This job is data-only:

- It does not import user holdings.
- It does not generate buy/sell advice.
- It does not send pushplus messages.
- It only upserts public NAV rows into the independent `08_spark_etf/data/spark.duckdb`.

## Manual Commands

Refresh all configured holdings for the latest 180 days:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_nav_history.py --lookback-days 180
```

Refresh selected funds:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_nav_history.py --codes 481001.OF 020255.OF --lookback-days 180
```

Mock smoke test:

```bash
cd /root/quant_project/08_spark_etf
MOCK_TUSHARE=1 PYTHONPATH=src python3 scripts/refresh_nav_history.py --db /tmp/spark_nav_history_mock.duckdb --codes 481001.OF --lookback-days 10
```

## Cron Template

```cron
30 21 * * * cd /root/quant_project/08_spark_etf && PYTHONPATH=src python3 scripts/refresh_nav_history.py --lookback-days 180 --json-log logs/nav_history_latest.json >> logs/nav_history_cron.log 2>&1
```

## Notes

- Live holding valuation uses `unit_nav`.
- `adj_nav` is stored for future backtests, not for current account market value.
- QDII funds may lag by one trading day; this is expected and reflected by the latest NAV date.
