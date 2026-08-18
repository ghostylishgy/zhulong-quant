# Zhulong Quant

[中文说明](README.zh-CN.md)

Zhulong is an explainable A-share research and paper-trading audit system. It combines deterministic market-data processing, bounded local-model assistance, mandatory cloud-model review, news and market-context risk checks, lifecycle-aware paper trading, and fact-first strategy memory.

This is a sanitized public source snapshot. It contains no production database, market-data cache, vector store, API credential, private environment file, runtime log, real portfolio configuration, or private infrastructure address.

> Zhulong does not connect to a broker and does not place real-money orders. It is research software, not investment advice or a public trading-signal service.

## Design Goal

The system asks one operational question:

> Which candidates deserve a complete, evidence-bound audit before they are allowed into a T+1 paper-trading plan?

Candidate discovery, qualification, entry planning, position review, exit, and post-trade learning are treated as one lifecycle. A strong daily move is only the beginning of the investigation.

## Current Audit Flow

```text
Market-data harvest
  Tushare and local data contracts -> DuckDB snapshots

Candidate recall
  Legacy L1 strength filter
  + isolated path observers
  + Eagle Active Path intraday discovery (observation only)

L1.5 structure gate
  TrendHunter checks trend alignment, volume structure, and pattern quality

L2 evidence preparation
  Deterministic facts and risk fields are authoritative
  Local model may inspect a closed set of missing-evidence IDs only

L3 thesis audit
  Deterministic thesis, score, and invalidation cards are authoritative
  Local model may select from pre-generated invalidation IDs only

L4 cloud court
  Bull / Bear / Judge / Notary evidence-bound review
  Mandatory provider failure pauses the audit instead of silently degrading

Pre-entry risk gate
  News evidence, account eligibility, market context, and plan validity
  Risk can block a paper entry; positive news does not inflate the score

Shadow lifecycle
  T+1 paper fill, position monitoring, lifecycle archetype, exit, and posthoc review

RAG memory
  Fact-first records, deterministic templates, optional enrichment,
  provenance checks, quarantine, and outcome linkage
```

## Authority Boundaries

| Component | Current authority |
| --- | --- |
| L1 / L1.5 | Production candidate entry and structural qualification |
| L2 deterministic layer | Authoritative evidence and risk fields |
| L2 local model | Non-authoritative closed-set observer; disabled by default |
| L3 deterministic layer | Authoritative thesis and invalidation cards |
| L3 local model | Non-authoritative invalidation-ID selector |
| L4 cloud court | Final audited verdict |
| News entry gate | Risk-only block before paper entry; does not rewrite L4 history |
| Eagle Active Path | Observation-only candidate discovery |
| Compass / ima bridge | Offline, dry-run research input only |
| US Radar | Isolated external-evidence sidecar |
| Shadow | Paper trading only |
| RAG / L5 research | Evidence memory and posthoc research; no autonomous authority |

## Fail-Closed Behavior

- A missing or unavailable mandatory L4 provider stops the current audit request.
- PushPlus sends a concise incident notification.
- The same request is retried at a configurable interval, currently 30 minutes by default.
- The audit resumes only after the provider returns a valid response.
- Model observers cannot overwrite deterministic L2/L3 fields.
- Unavailable news evidence can block a new paper entry.
- Quarantined RAG records cannot return through enrichment or embedding updates.

## Research Sidecars

### Eagle Active Path

Eagle records persistent intraday activity using fresh quote windows and real counter deltas. It looks for information-rich candidates that may not appear in a simple daily gain ranking. It remains isolated from L1, L4, Shadow, and trading authority until forward evidence is sufficient.

### Compass / ima

Compass reports are normalized into traceable dry-run artifacts. Themes do not become stock tasks automatically; ticker resolution is exact-match or manually reviewed, and unsupported markets remain watch-only.

### US Radar

US Radar uses separate storage and reporting to study overseas events and supply-chain evidence. It does not write the Zhulong primary database or create trading tasks.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `01_engine/` | Data gateway, Tushare bridge, RAG refresh, backup and contracts |
| `02_brain/` | Main L1-L4 engine, TrendHunter, RPS and evidence guards |
| `03_tactics/` | Eagle, Owl and intraday observation bridges |
| `04_governance/` | Governance, notification, scoring and risk utilities |
| `05_shadow/` | T+1 paper fills, position lifecycle and post-trade memory |
| `06_watchtower/` | Operational API and web terminal |
| `07_macro/` | Macro and market-regime context |
| `08_spark_etf/` | Isolated ETF holdings-discipline tool |
| `10_us_radar/` | Isolated US event research sidecar |
| `config/` | Public examples; private `.env` is excluded |
| `docs/` | Public architecture and protocol documents |
| `tests/` | Contract, failure-path and regression tests |
| `tools/`, `scripts/`, `utils/` | Dry-run, diagnostics and maintenance tools |

## Quick Start

Requirements:

- Linux and Python 3.11
- DuckDB-compatible local storage
- A Tushare token for market-data collection
- Optional Ollama service for local observers
- Cloud-model credentials for the mandatory L4 court

```bash
git clone https://github.com/ghostylishgy/zhulong-quant.git
cd zhulong-quant

python3 -m venv .venv
. .venv/bin/activate
pip install -r config/requirements.txt

cp config/.env.example config/.env
ln -s config/.env .env
chmod 600 config/.env
# Fill private values locally. Never commit this file.
```

The repository does not include a production database. Restore or construct the required schema and data through your own authorized data pipeline before starting the daemon.

Run the regression suite:

```bash
export PYTHONPATH="$PWD/03_tactics:$PWD"
python3 -m unittest discover -s tests -p 'test_*.py' -q
```

Compile critical modules:

```bash
python3 -m py_compile   zhulong_daemon.py   01_engine/lib/db_gateway.py   02_brain/decision_engine.py
```

## Deployment Safety

- Keep `.env`, databases, Chroma stores, reports, logs, credentials, backups, and real portfolio files outside Git.
- Addresses in this snapshot use RFC 5737 documentation ranges. Configure real service locations privately.
- Watchtower protects research endpoints with `X-Watchtower-Key` and an explicit CORS allowlist. Keep a second authentication layer at the reverse proxy for Internet-facing deployments.
- Keep Shadow disconnected from broker APIs.
- Treat web evidence as untrusted until manually approved.
- Review [SECURITY.md](SECURITY.md) before deploying any API surface.

## Public Snapshot Policy

The public branch intentionally excludes private engineering logs, production incidents, prompt-history snapshots, machine names, private addresses, proxy details, recovery evidence, runtime data, real holdings, account data, and personal watchlists.

The complete operational history is retained separately in encrypted private backups.

## License

Copyright (c) 2026 ghostylishgy. All rights reserved.

This repository is source-available for inspection and discussion only. No open-source license is granted. See [LICENSE](LICENSE).

## Disclaimer

Historical observations, backtests, paper trades, model outputs, and candidate reports do not predict future returns. Nothing in this repository is a recommendation to buy or sell securities.
