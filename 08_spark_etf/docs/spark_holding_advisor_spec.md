# Spark ETF Holding Advisor Development Spec

Scope: `08_spark_etf/`, independent `spark.duckdb`, no writes to the main Zhulong audit database, no account login, no automatic trading.

## 1. Positioning

Spark ETF is a lightweight, auditable holding advisor. It only emits states and recommendations:

- continue holding
- buy less
- pause buying
- profit-protection watch
- suggest partial sell
- suggest cleanup

It never touches the user's fund account and never sends orders.

## 2. Weekly Snapshot Input

The current MVP truth source is the weekly position snapshot supplied manually by the user:

```text
fund_code / fund_name / shares / unit_cost
```

`unit_cost` must be the position cost shown by Tiantian Fund, meaning the already-adjusted holding cost price after platform accounting. It is not the raw original average buy price. All 20/30/50 percent protection lines are calculated from this value.

The import CLI must record the cost basis type:

- `tiantian_position_cost`
- `diluted_cost`
- `unknown`

Do not mix cost basis types silently.

## 3. Public Data Sources And Data Quality

NAV and valuation data come from public sources only, never from screenshots.

Preferred sources:

- Tushare `fund_nav` for NAV
- Tushare `index_dailybasic` for A-share index valuation when available
- China Securities Index / Eastmoney / AkShare as future fallbacks

Data quality states:

- `DATA_OK`
- `NAV_ONLY`
- `VALUATION_PROXY_MISSING`
- `NO_DATA`
- `STALE_DATA`

Supplement: multi-source disagreement should be represented as a risk flag such as `DATA_CONFLICT`. If this later becomes common enough, promote it to a first-class data-quality state. Do not silently choose one source when sources materially disagree.

When no reliable valuation proxy exists, the system may emit profit-protection and trend-watch guidance only. It must not emit strong low-valuation buy guidance.

## 4. Sell Discipline Priority

1. Data quality: missing or stale data blocks strong buy/sell advice.
2. Asset bucket: different fund families use different signals.
3. Valuation is the primary anchor:
   - extreme low valuation: hold or add, no profit taking
   - low valuation: hold, no active sell
   - neutral: protection hints allowed
   - high valuation: profit-watch
   - extreme high valuation: pause buying and consider selling if trend weakens
4. Trend confirmation:
   - high valuation plus strong trend: protect, do not rush
   - high valuation plus weakening trend: raise sell priority
5. Return protection lines:
   - 20 percent: profit-protection watch
   - 30 percent: principal-recovery reminder, not automatic sell
   - 50 percent: layered take-profit reminder

`FREE_RIDE` must be explicitly selected by the user and is not the default behavior.

## 5. Asset Buckets

| Bucket | Main Signal | Rule |
| --- | --- | --- |
| A-share broad/dividend/medical/consumer | PE/PB percentile | eligible for DATA_OK discipline |
| Hong Kong / HK medical / China internet | valuation percentile when available | often NAV_ONLY; degrade honestly |
| QDII | overseas valuation plus premium/discount | no strong low-valuation call without premium data |
| gold | gold trend plus real-rate proxy | not a PE/PB percentile game |
| AI infra / CPO / semiconductor | trend, cycle, crowding | never label as low valuation; trend confirmation only |

## 6. Sentinel And Review Loop

Weekly share changes are checked against the previous snapshot:

- abnormal decrease: likely sell or data entry issue; ask for confirmation
- abnormal jump: likely buy, dividend reinvestment, or data entry issue; ask for confirmation

`spark_action_review` records the closed loop:

- system recommendation
- user adoption status
- N-week outcome

A recommendation cannot be considered proven unless it is reviewable later.

## 7. Backtest Methodology

Backtests measure rules, not the user's real trading history. They use synthetic DCA schedules and historical adjusted NAV, never the real weekly position snapshots.

Rules:

1. Use adjusted unit NAV for backtests. Dividend days must not become fake drawdowns.
2. Do not tune thresholds by historical returns. Thresholds are discipline settings and the backtest validates them.
3. Use multiple deterministic start dates. Single-start conclusions are invalid.
4. Dynamic-amount strategies must be compared with XIRR or normalized capital-weighted metrics, not absolute return alone.
5. Confirmation day and QDII delays are simplified as next-NAV execution assumptions and must be documented as assumptions.

## 8. Backtest Strategies

- `dca_naive`: fixed weekly amount, never sell
- `hold_only`: one-time buy for approximately the full-period contribution amount, never sell
- `dca_dynamic_amount`: valuation-adjusted amount, never sell
- `dca_dynamic_plus_valuation_tp`: dynamic DCA plus valuation take-profit rules
- `dca_dynamic_plus_free_ride`: dynamic DCA plus explicit principal recovery / free ride

Strategies depending on valuation are `SKIPPED` when valuation history is missing.

## 9. Backtest Gate

The main rule strategy must beat `dca_naive` on the multi-start distribution before it can become `DISCIPLINE`.

For each bucket and strategy:

1. median XIRR >= baseline median XIRR
2. max drawdown <= baseline max drawdown OR drawdown recovery time is shorter
3. wrong-sell opportunity cost < drawdown saved by take-profit

All pass: `DISCIPLINE`.
Otherwise: `ADVISORY_ONLY`.
Missing valuation series for valuation-dependent rules: `SKIPPED`.

Supplement: evaluate by bucket. Growth buckets such as AI infra/CPO should not be forced through valuation take-profit gates as strong discipline.

## 10. Wrong Sell Definition

Default definition:

- K = 60 trading days
- X = 10 percent

A sell is counted as wrong if adjusted NAV rises more than X percent above the sell price within K trading days and the strategy did not emit a valid buy-back signal during that window.

Default K/X is used for the primary gate. Sensitivity reports may include K=90 or X=12/15 percent for high-volatility buckets, but those are not optimized thresholds.

## 11. XIRR Alignment

For fair comparison, buys and sells are internal strategy cash movements. XIRR should use external contributions and terminal value:

- weekly contribution: external negative cash flow
- final liquidation value: positive cash flow = fund market value + strategy cash

Internal buy/sell cash movement should not be treated as user cash-out unless the tested strategy explicitly defines it that way.

## 12. Deterministic Start Dates

Use deterministic rolling start dates for auditability. Recommended:

- monthly or quarterly rolling starts when enough history exists
- always include approximately T-1y, T-2y, T-3y, T-5y where available

Random sampling is allowed only as an optional secondary report with fixed seed.

## 13. Implementation Order

1. Weekly snapshot schema and manual import CLI
2. Public NAV cache
3. Holding signal generation
4. PushPlus report integration
5. Backtest schema and rule scaffold
6. Full backtest strategies and verdict gate
7. Watchlist and low-valuation candidate discovery as a later track
