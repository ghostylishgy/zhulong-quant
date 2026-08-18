# Compass Discovery Constraints Protocol

Status: MVP dry-run contract
Version: compass_discovery_constraints_v0.1
Last updated: 2026-07-08

## Purpose

This protocol defines how Compass/ima direction-level output may constrain a future Zhulong discovery preview.
It does not authorize automatic theme-to-stock mapping, validation task creation, Shadow writes, RAG writes, or trading actions.

Compass/ima may provide:

- explicit historical stock seeds (`seed_anchor`)
- direction-level themes (`theme_candidate`)
- observation-only objects (`watch_only`)
- exclusions (`excluded`)

Only Zhulong may later validate tradable A-share symbols, and only after the required gates are satisfied.

## Object Roles

| object_class | Meaning | Default task behavior |
| --- | --- | --- |
| `seed_anchor` | Explicit stock-like seed from prior human/Compass work. It is not an ima-generated buy list. | `record_only`; exact ticker resolve allowed; no task unless `--enable-seed-validation` is set. |
| `theme_candidate` | Direction-level hint such as industry, business function, material layer, or policy-driven demand. | `hint_only`; never auto-maps to listed companies. |
| `watch_only` | Object worth observing but not suitable for validation task generation. | no task. |
| `excluded` | Explicitly excluded direction or object. | no discovery and no task. |

## Hard Gates Before Any Stock Validation Task

A stock validation task may only be emitted when all applicable gates pass:

1. `market_is_a_share`
2. `ticker_exact_or_manual_resolved`
3. `business_relevance_evidence_present`
4. `human_review_for_theme_mapping` when the source object is a theme
5. `dry_run_preview_reviewed`
6. `no_trade_signal=true`

If any gate is missing, the object remains `record_only`, `hint_only`, `watch_only`, or `excluded`.

## Business Relevance Evidence

`business_relevance_evidence_present` is not satisfied by a loose name or keyword match alone.

| evidence_level | Examples | Can it independently unlock a validation task? |
| --- | --- | --- |
| `strong` | main business disclosure, annual report, exchange announcement, official filing, or explicit company statement tied to the theme | yes, after all other gates pass |
| `medium` | concept board membership, industry component table, broker/industry report, or curated theme membership table | only with human review and at least one supporting metric |
| `weak` | company name keyword, industry keyword, fuzzy/LIKE match, or broad semantic similarity | no |

Weak evidence may appear in a discovery preview, but it cannot independently generate a validation task.

## Preview Review Gate

`dry_run_preview_reviewed` is enforced by a separate offline review tool. Discovery itself still cannot approve its own rows.

Implemented review mechanism:

- `tools/compass_discovery_review.py` initializes an editable `review_manifest.json` from a discovery preview.
- The manifest binds the whole preview SHA256 and each reviewed row SHA256.
- Applied decisions require a reviewer id, timezone-aware review timestamp, and review notes.
- Approval requires `medium` or `strong` evidence, explicit business-relevance confirmation, and at least one non-null supporting metric from the immutable preview row.
- `weak` evidence cannot be approved by the manifest.

The review tool emits reviewed candidates only. It does not generate validation tasks; task generation remains a later, separate dry-run gate.

## Allowed Actions

The protocol allows only dry-run and read-only actions:

- preserve the source record
- render normalized and ingest previews
- exact ticker resolve for `seed_anchor`
- manual review
- future read-only discovery preview constrained by this protocol

## Blocked Actions

The protocol blocks:

- trade
- write_shadow
- write_rag_memory
- write_nexus_audits
- write_duckdb
- trigger_daemon
- call_decision_engine
- call_nexus_run
- auto_theme_to_stock_mapping
- auto_fuzzy_ticker_resolve
- generate_stock_validation_from_theme
- write_validation_tasks_from_theme
- unbounded_tushare_discovery

## Required Normalized Fields

Each candidate should preserve:

- `object_class`
- `origin_role`
- `validation_mode`
- `discovery_status`
- `discovery_constraints`
- `source_section`
- `source_row_index`
- `raw_fields`
- `generated_by_ima`
- `used_as_discovery_hint`
- `default_generate_validation_task`
- `no_trade_signal`

`discovery_constraints` must include:

- `protocol_version`
- `role`
- `status`
- `market_scope`
- `query_terms`
- `allowed_actions`
- `blocked_actions`
- `readonly_modules`
- `required_before_stock_validation_task`
- `fail_closed_on`
- `auto_generate_stock_validation_task=false`
- `auto_theme_to_stock_mapping=false`
- `auto_fuzzy_ticker_resolve=false`
- `manual_review_required_before_task=true`

## Future Discovery Preview Contract

A future discovery tool may read `theme_candidate.discovery_constraints`, but it must only output a preview such as:

- source theme
- read-only search constraints
- candidate names found from allowed data sources
- evidence snippets or table references
- unresolved or ambiguous ticker status
- manual review requirements

It must not directly write validation tasks. A separate reviewed step is required to turn a preview row into a seed or validation task.

The only allowed path is:

```text
theme_candidate -> discovery_preview -> human review -> reviewed seed/reviewed candidate -> dry-run validation task
```

The forbidden path is:

```text
theme_candidate -> validation_task
```

## `--enable-seed-validation` Scope

`--enable-seed-validation` is not a force flag. It only opens the final gate for otherwise eligible `seed_anchor` rows.

It must not bypass:

- `market_is_a_share`
- `ticker_exact_or_manual_resolved`
- `no_trade_signal=true`
- `not watch_only`
- `not excluded`
- `not theme_candidate`
- `manual_review_required=false`

## Fail-Closed Rules

Fail closed when:

- the market is not A-share
- exact ticker resolution fails
- fuzzy or LIKE matching is the only evidence
- theme relevance is inferred without source evidence
- business exposure evidence is absent
- the source object is watch-only or excluded
- the preview has not been manually reviewed

## Current Implementation

- `tools/compass_report_normalizer.py` emits `discovery_protocol` and per-candidate `discovery_constraints`.
- `tools/compass_ingest.py` carries the constraints into the ingest preview and task payload.
- `tools/compass_theme_discovery.py` reads only `theme_candidate` discovery hints and renders a read-only `discovery_preview`; every preview row remains `manual_review_required=true`, `generate_task=false`, and `ticker_status=preview_only_not_task_resolved`.
- `tools/compass_discovery_review.py` initializes and applies SHA-bound manual review manifests. Approved rows become reviewed candidate artifacts with `generate_task=false`; no validation task is created in the review phase.
- Theme candidates remain discovery hints only and generate no validation tasks.
- Seed anchors remain record-only by default; `--enable-seed-validation` is required for exact-resolved A-share seeds to emit dry-run validation tasks.
