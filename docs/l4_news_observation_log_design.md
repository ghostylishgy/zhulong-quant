# L4 News Observation Log Design

Status: implemented for observation. This document does not authorize changes to prompts, scores, verdicts, Shadow, or execution.

## 1. Objective

Measure whether the L4 news verifier is available, reproducible, accurate, and potentially useful before any production policy moves beyond `OBSERVE_ONLY`.

The log must answer four questions:

1. Did the side-channel worker process every eligible L4 candidate?
2. Which providers succeeded, failed, or returned relevant evidence?
3. What would the deterministic news gate have changed, without changing the actual verdict?
4. After outcomes mature, did the counterfactual warning identify genuinely bad PASS decisions?

## 2. Safety Invariants

- `OBSERVE_ONLY` must keep `l4_news_prompt_injected=false` and `l4_news_gate_applied=false`.
- The observation logger must never update `l4_final_verdict`, scores, Shadow signals, or execution state.
- Every fetch must reuse the persisted `l4_news_as_of`; worker wall-clock time must not become the evidence cutoff.
- Missing or partial provider coverage is `UNAVAILABLE/PARTIAL`, never negative proof.
- Positive news is measurable context only and must never create a counterfactual upgrade.
- Full article bodies and credentials are not written to operational logs.

## 3. Existing Evidence Surface

Per-candidate evidence already lives in `nexus_audits.l4_news_*`:

- queue and completion status
- fixed `l4_news_as_of`
- risk level and risk score
- counterfactual `l4_news_gate`
- source/evidence payload
- prompt-injection and gate-application flags

The new design should reuse those fields instead of creating a second candidate-level source of truth.

## 4. Proposed Run-Level Record

One append-only `ops_l4_news_observation_runs` record is written per worker invocation.

| Field | Purpose |
| --- | --- |
| `worker_run_id` | Unique id for one queue-drain invocation |
| `started_at`, `ended_at`, `elapsed_ms` | Worker health and cost |
| `scheduled_window` | `22:35`, `23:35`, `00:35`, `01:35`, or manual |
| `queued_before`, `attempted`, `completed`, `unavailable`, `pending_after` | Queue accounting |
| `cninfo_ok`, `cls_ok`, `eastmoney_ok` | Provider success counts |
| `cninfo_failed`, `cls_failed`, `eastmoney_failed` | Provider failure counts |
| `evidence_items`, `official_items`, `media_items` | Coverage volume |
| `gate_none`, `would_cap_hold`, `would_veto` | Counterfactual distribution |
| `counterfactual_changes` | Rows where the hypothetical gate differs from the actual L4 verdict |
| `latency_p50_ms`, `latency_p95_ms`, `latency_max_ms` | Per-candidate fetch cost |
| `status`, `error_summary` | `DONE/PARTIAL/FAILED/SKIPPED_AUDIT_ACTIVE` |

The record is operational evidence only. It must not be consumed by L4 or RAG weighting.

## 5. Candidate-Level Log Line

Keep one concise structured line per processed candidate:

```text
[L4-NEWS-OBS] worker=<id> task=<task_id> symbol=<symbol> as_of=<timestamp> status=<status> sources_ok=<list> sources_failed=<list> evidence=<n> official=<n> media=<n> hypothetical=<NONE|WOULD_CAP_HOLD|WOULD_VETO> elapsed_ms=<n>
```

Do not print article bodies. Titles remain bounded inside the persisted evidence payload for later manual review.

## 6. Run Summary Log Line

```text
[L4-NEWS-SUMMARY] worker=<id> queued=<n> completed=<n> unavailable=<n> pending=<n> cninfo=<ok>/<attempted> cls=<ok>/<attempted> eastmoney=<ok>/<attempted> none=<n> cap_hold=<n> veto=<n> counterfactual_changes=<n> p50_ms=<n> p95_ms=<n> status=<status>
```

Normal summaries stay in logs and DuckDB. Push notifications are reserved for actionable failures.

## 7. Alert Rules

Push one aggregated warning when any of the following occurs:

- pending queue remains after the `01:35` window
- worker invocation crashes or cannot persist its run record
- all providers fail for every attempted candidate in one run
- CNINFO rolling five-trading-day availability falls below 95%
- at least one row violates the safety invariant (`prompt_injected` or `gate_applied` under `OBSERVE_ONLY`)

Single-provider transient failures should be recorded but should not create per-symbol push noise.

## 8. Counterfactual Effect Metrics

At collection time, derive without mutating the verdict:

- `would_change_verdict`: whether the hypothetical gate differs from `l4_final_verdict`
- `counterfactual_verdict`: original verdict after applying only the deterministic risk-only mapping
- `counterfactual_reason`: official critical, official caution, or two independent media providers

After T+1/T+5 outcomes mature, a separate read-only review may calculate:

- warning precision: warned candidates that later underperformed or hit a risk exit
- false-positive rate: warned candidates that remained healthy or became winners
- missed-risk rate: bad PASS outcomes with no prior news warning
- incremental value over L4/RAG: whether news identified risk not already captured by Bear, tide, or RAG

Outcome attribution must not be written back into the original news evidence.

## 9. Graduation Gate

The first review point requires all of the following:

- at least 15 trading days
- at least 100 eligible L4 candidates
- at least 20 `WOULD_CAP_HOLD/WOULD_VETO` events; otherwise observation continues
- queue completion near 100%, with no persistent backlog
- CNINFO availability at least 95%
- at least one media provider availability at least 90%
- manual review of every risk event
- zero material false VETO caused by negation, wrong entity, or multi-subject text
- sentence-level negation and target-entity binding completed before `ENFORCED`

Passing the gate triggers a design review of counterfactual `COURT_CONTEXT`; it does not automatically change `L4_NEWS_POLICY`.

## 10. Validation Plan

- Empty queue: one `DONE` run record with zeros and no alert.
- Partial provider failure: candidate persists as partial; summary counts the failed source; no verdict mutation.
- Official critical evidence: logs `WOULD_VETO`; actual verdict remains unchanged.
- Single media risk: cannot log `WOULD_VETO` independently.
- Historical rerun: persisted `news_as_of` is used even when the worker runs later.
- Audit overlap: worker logs `SKIPPED_AUDIT_ACTIVE`; later window drains the same queue.
- Final-window backlog: one aggregated warning with count and oldest queued task.
- Safety regression: test that all observed rows keep prompt/gate flags false and retain their original L4 verdict.

## 11. Delivery Boundary

The implementation should be one observability-only commit. Prompt changes, sentence/entity disambiguation, `COURT_CONTEXT`, and `ENFORCED` activation belong to later reviewed commits.

## 12. Implemented Components

- `tools/process_l4_news_observations.py`: candidate logs, run summaries, source counters, latency, counterfactual fields, safety checks, and aggregated alerts.
- `ops_l4_news_observation_runs`: append-only worker invocation records.
- `ops_l4_news_manual_reviews`: operator labels for every counterfactual risk event.
- `ops_l4_news_control_state`: explicit prerequisite state; semantic hardening defaults to incomplete.
- `tools/review_l4_news_observation.py`: read-only graduation evaluator. It can only return `NOT_READY` or `READY_FOR_DESIGN_REVIEW` and never edits `L4_NEWS_POLICY`.
- `ops_l4_news_graduation_reviews`: append-only weekly evaluation results and full condition snapshots.
- Daemon schedule: queue drain at `22:35/23:35/00:35/01:35`; graduation review Sunday at `20:30`.
