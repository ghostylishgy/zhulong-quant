# Spark ETF Market Universe

## Positioning

The market universe is the raw discovery layer for Spark ETF candidate V2.

It does not emit buy advice, does not modify holdings, and does not promote funds into
`spark_fund_candidates` automatically. It only stores public fund metadata and weak theme evidence.

## Tables

- `spark_market_fund_universe`: full-market fund metadata from Tushare `fund_basic`.
- `spark_market_theme_tag`: theme recall evidence for each fund.
- `spark_theme_observation_bucket`: current theme observation bucket state.
- `spark_theme_observation_daily`: daily theme observation snapshots for later review.

These tables are separate from:

- `spark_position_weekly_snapshot`
- `spark_holding_signal`
- `spark_action_review`
- `spark_fund_candidates`
- `spark_candidate_signal`

## Classification Policy

The first classifier is intentionally conservative:

- fund-name keyword matches are marked `NAME_ONLY`
- no single direction score is exposed
- `NAME_ONLY` is discovery evidence, not a verified theme
- current holdings are flagged with `is_current_holding`

Future upgrades should add:

- tracked-index verification: `INDEX_VERIFIED`
- holdings/industry verification: `HOLDING_VERIFIED`
- theme signal review with separate valuation, drawdown, trend, breadth, and data-quality columns

## Manual Run

Refresh open-end funds, which best match Tiantian Fund workflows:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_market_universe.py --market O
```

Smoke test with a row limit:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_market_universe.py --market O --limit 200
```

Mock smoke test:

```bash
cd /root/quant_project/08_spark_etf
MOCK_TUSHARE=1 PYTHONPATH=src python3 scripts/refresh_market_universe.py --db /tmp/spark_market_universe_mock.duckdb
```

## Operating Rule

This layer supplies evidence to the user's judgment. It must not become a hidden scorer that replaces
human review.

## Observation Bucket

The observation bucket is the next layer after market theme tags. It aggregates themes across the
full-market universe and stores repeatable evidence over time.

It currently records:

- total tagged funds
- non-held tagged funds
- current-holding overlap
- classification confidence counts
- sample non-held funds as evidence

It deliberately does not record a single direction score. It also does not promote themes or funds
into `spark_fund_candidates` automatically.

Refresh observations:

```bash
cd /root/quant_project/08_spark_etf
PYTHONPATH=src python3 scripts/refresh_theme_observations.py
```

Suggested weekly schedule:

```cron
10 21 * * 0 cd /root/quant_project/08_spark_etf && PYTHONPATH=src python3 scripts/refresh_market_universe.py --market O >> logs/market_universe_cron.log 2>&1
20 21 * * 0 cd /root/quant_project/08_spark_etf && PYTHONPATH=src python3 scripts/refresh_theme_observations.py >> logs/theme_observation_cron.log 2>&1
```

The first version uses `NAME_ONLY` tags from `spark_market_theme_tag`. Later versions should upgrade
theme evidence through tracked-index and holdings verification before any theme can become `FOCUS`.
