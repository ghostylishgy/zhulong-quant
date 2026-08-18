# BL-018 Corporate Action and Price Semantics Contract v0.1

Status: Phase B1 isolated FIFO-tax contract frozen; production mutation not authorized

## 1. Purpose

This contract separates three price and position domains so that a corporate
action cannot silently change execution PnL, analytical continuity, or Shadow
cash and share balances.

1. `raw_execution_price`: exchange-observed prices used for fills, stops, fees,
   limit checks, and realized PnL.
2. `continuous_analytical_price`: adjusted history used only for cross-period
   RPS, moving averages, and research comparisons.
3. `corporate_action_position_state`: rights, receivables, payable cash, pending
   bonus shares, available shares, and later tax adjustments for a held lot.

The three domains must never overwrite one another. In particular, an adjusted
price must not be used as a fill price, and a dividend must not be represented
by rewriting the historical entry price.

## 2. Current production facts

- `fact_daily` is the raw market record. It does not contain an authoritative
  adjusted-factor series.
- TrendHunter and RPS currently consume raw close for cross-period calculations.
- Shadow stores position and execution state but has no unified corporate-action
  event ledger.
- The first historical canary is `688809.SH`: 500 shares were bought, 200 were
  sold before the 2026-06-18 record date, so the rights quantity is 300 rather
  than the original 500.

These facts justify a staged implementation. This document does not authorize a
production Shadow mutation.

## 3. Authoritative event and evidence hierarchy

The primary corporate-action fact is the Tushare `dividend` row whose
`div_proc` is `实施`. Proposal and shareholder-meeting rows are context only.

`fact_daily.pre_close` and a future adjusted-factor snapshot are cross-checks.
They must not be treated as independent corporate-action facts and must not
outvote the implementation row.

Every normalized event must retain:

```text
symbol
report_period
announcement_date
record_date
ex_date
pay_date
div_listdate
event_type
cash_div_per_share
stock_div_per_share
source
source_as_of
event_identity
event_sha256
```

The audit artifact must also retain canonical `position_evidence` and
`sale_evidence` arrays plus an independent SHA for each array. A downstream tax
preview may not reopen the live tables and silently replace the reviewed sale
history.

`event_identity` is stable across revisions and binds symbol, report period,
and the implementation family. Dates, rates, source provenance, and fetch time
are versioned attributes rather than identity attributes. `event_sha256` binds
the normalized business content but excludes fetch-time provenance. Two
different content hashes for one identity are an event revision, not a second
benefit; fetching unchanged content on another day is not a revision.

## 4. Time contract

### 4.1 Cash dividend

1. At record-date close, freeze the actually held quantity after all sells on
   that date.
2. On ex-date, create a gross cash receivable and reset price-dependent position
   anchors only after the execution implementation is separately approved.
3. Do not increase Shadow cash until `pay_date`.
4. Dividend tax remains pending until the relevant lot is sold and the holding
   period is known. v0.1 must not pretend that one static tax rate is final.
5. The audit artifact may expose gross cash and `PENDING_FIFO_SALE_LEDGER`, but
   must leave net cash null until a separately reviewed lot-sale ledger can
   prove acquisition date, entitled quantity, sale date, and FIFO allocation.

### 4.2 Bonus shares or transfer shares

1. At record-date close, freeze the entitled quantity.
2. On ex-date, create pending shares.
3. Pending shares are not sellable.
4. Move them to available quantity only on `div_listdate`.

### 4.3 Same-day actions

A sale on record date reduces the rights quantity because entitlement is based
on the closing position. A buy filled on record date may qualify only if the
fill is a valid exchange execution before close.

## 5. Position quantity contract

The following quantities are distinct:

```text
initial_qty
current_available_qty
rights_frozen_qty
pending_stock_qty
realized_qty
```

`initial_qty` is immutable. Corporate actions must not rewrite it. Historical
rights reconstruction uses the original buy plus actual sell events through the
record date. The current `fact_paper_positions.qty` is not sufficient because it
describes the latest state rather than a past record-date snapshot.

## 6. Future application ledger

Production implementation, if separately approved, requires an independent
append-only ledger. A JSON list on the position row is not an audit trail.

Minimum proposed fields:

```text
application_id
record_sha256
event_identity
event_sha256
symbol
position_trade_date
signal_task_id
stage
effective_date
rights_frozen_qty
cash_receivable_delta_gross
cash_balance_delta_gross
pending_share_delta
available_share_delta
tax_delta
source_as_of
source_audit_sha256
review_manifest_sha256
status
created_at
```

The uniqueness boundary is `(event_sha256, position_trade_date, stage)`. A
revised event must create a reviewed correction path; it must not silently
overwrite the prior application.

The read-only audit must generate this stage key before production integration.
For multiple position lots in the same symbol, each lot keeps a separate stage
key and the report may additionally provide an event-level quantity total. The
aggregate is a review convenience, not a mutable position balance.

### 6.1 Isolated dividend-tax recovery ledger

`tools/shadow_dividend_tax_recovery.py` is a separate, unwired contract for
pure cash dividends only. It consumes the position and sale evidence already
bound into a `DATA_OK` audit artifact and applies FIFO at the Shadow account and
symbol level, not according to a strategy position's declared sell allocation.

The frozen rate buckets are:

```text
holding period <= 1 calendar month: 20%
holding period > 1 month and <= 1 calendar year: 10%
holding period > 1 calendar year: 0%
```

The one-month and one-year boundary dates are inclusive. Month-end acquisition
dates use the last valid day of the target month. A tax record binds the event,
acquisition lot, sale idempotency key, evidence SHAs, preview SHA, and human
review manifest SHA. Exact replay is idempotent; changed content and event
revisions require a future reviewed correction path.

Policy references checked for this contract are the current listed-company
20%/10%/0% treatment in
[Finance and Taxation 2015 No. 101](https://www.chinatax.gov.cn/n810341/n810755/c1797427/content.html)
and the official natural-month/year and account FIFO explanation in the
[Ministry of Finance FAQ](https://m.mof.gov.cn/zcjd/201211/t20121116_697339.htm).

The B1 contract deliberately fails closed for:

- bonus shares, capital conversions, or mixed cash/share events;
- missing or internally inconsistent position/sale evidence;
- a sale dated after the artifact's `source_as_of`;
- same-symbol buy and sell activity on the same day, because the regulatory
  account-level daily-net rule needs a separate netting contract.

These cases are not estimated. No tax row is created in the production database.

## 7. Read-only audit artifact

The first implementation is `tools/audit_shadow_corporate_actions.py`.

It may:

- open DuckDB read-only;
- read Shadow positions and realized intraday sell events;
- read a supplied event fixture or call Tushare `dividend`;
- reconstruct the quantity held at record-date close;
- estimate gross receivables and pending share quantities;
- retain multiple position lots separately and render event-level totals;
- generate deterministic stage-application keys for idempotency review;
- compare ex-date `pre_close` with a theoretical reference derived from the
  previous raw close and normalized cash/share terms;
- render JSON and Markdown evidence reports.

It must not:

- write DuckDB;
- change Shadow cash, positions, metrics, or fills;
- change RPS, TrendHunter, L1, L3, L4, RAG, or news;
- register a daemon job;
- claim that retrospective source data was available at an earlier decision
  time.
- treat the price-anchor comparison as an execution price or as a second
  authoritative corporate-action source;
- estimate final dividend tax without a complete FIFO lot-sale ledger.

## 8. Quality states

```text
DATA_OK
VALID_EMPTY
PARTIAL_HISTORY
SOURCE_UNAVAILABLE
SCHEMA_DRIFT
EVENT_REVISION_DETECTED
```

A mismatch between reconstructed sell quantity and the position's realized
quantity is `PARTIAL_HISTORY`. It must not be repaired by guessing.

## 9. Acceptance cases before Shadow integration

The read-only layer must cover:

- partial sale before record date;
- sale on record date;
- `pay_date != ex_date`;
- bonus shares unavailable before `div_listdate`;
- multiple entry lots;
- duplicate application attempts;
- event revision with a changed content hash;
- missing sell history;
- cash and share events in the same implementation plan.

Phase A.2 covers multi-lot entitlement, combined cash/share events, and
deterministic application-key generation in the read-only artifact. Phase B0 adds the unwired
`tools/shadow_corporate_action_ledger.py` contract and verifies exact replay,
changed-content conflict rejection, event-revision rejection, and atomic batch
rollback in temporary DuckDB files. It does not create the table in production,
or mutate Shadow positions. Phase B1 upgrades the audit artifact to v0.3 with
SHA-bound position/sale evidence and adds pure-cash account-level FIFO tax
preview and an isolated append-only recovery ledger. Multi-lot 20%/10%/0%
allocation, calendar boundaries, partial settlement, exact replay, changed
content, event revision, same-day netting rejection, and mixed-event rejection
are covered in temporary tests. Production application, correction entries,
same-day daily-net handling, and stock-dividend tax basis remain blockers. Until
a separately approved production path applies both stage and tax records without
rewriting historical fills, the production `cash_receivable_net` remains null.

Only after these cases pass and the historical report is reviewed may a separate
proposal authorize new production tables and Shadow state transitions.

## 10. Deferred analytical-price work

RPS and TrendHunter adjusted-history migration is a separate phase. It requires
a versioned point-in-time adjustment-factor snapshot and a before/after L1
comparison. It must not be bundled with the Shadow corporate-action MVP.
