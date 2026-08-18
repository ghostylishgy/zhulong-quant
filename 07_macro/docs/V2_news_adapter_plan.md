# Macro V2 News Adapter Migration Plan (Minimal Invasion)

## Scope
- Build `07_macro/lib/news_adapter.py` only (sidecar style, no hard-coupled changes to V1 factor path).
- Keep Macro V1 scoring path runnable without any news source.

## Module split
1. `news_provider` (future)
- Responsibility: fetch raw topic news from external source.
- Interface target: `fetch_news(topic_type, topic_id, topic_name) -> List[dict]`.

2. `news_adapter` (this delivery)
- Responsibility: brutal token-juicing and prompt corpus generation.
- Hard rules:
  - Drop body length < 50 chars.
  - Dedupe by title similarity via `SequenceMatcher`.
  - Keep max 3 items and hard-cut merged text to 1500 chars.

3. `llm_macro_client` (future)
- Responsibility: call node-102 Fin-R1 with strict JSON schema.
- Must return classified extraction only; no probability output.

4. `macro_enricher` (future)
- Responsibility: map extraction tags to Python score table and persist `llm_resonance_score` / `is_event_driven_trap`.

## Migration checklist
1. Schema stage
- Replace legacy `one_day_trip_prob` with `is_event_driven_trap`.
- Keep bootstrap idempotent and add in-place migration for old tables.

2. Adapter stage
- Land `news_adapter.py` + logs + unit tests.

3. Integration stage
- Inject after V1 persist as best-effort sidecar.
- On timeout/parse failure: silent downgrade, no impact on V1 ranking.

4. Daemon stage
- Add optional `phase_macro_v2` between harvest and audit.
