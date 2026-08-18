# Macro Resonance Engine V1 Schema

This document records the V1 DuckDB table contract and provides a stable interface for future V2 cloud adjudication.

## 1. dim_macro_topic_member
Purpose: topic-to-stock mapping cache.

Primary key: (topic_type, topic_id, symbol)

| Field | Type | Meaning |
|---|---|---|
| topic_type | VARCHAR | Topic namespace, such as industry or concept |
| topic_id | VARCHAR | Topic unique id in namespace |
| topic_name | VARCHAR | Human-readable topic name |
| symbol | VARCHAR | Stock symbol |
| ts_code | VARCHAR | Raw symbol from source |
| source | VARCHAR | Mapping source identifier |
| is_active | INTEGER | Active flag, 1 means active |
| updated_at | TIMESTAMP | Mapping refresh timestamp |

## 2. fact_macro_topic_daily
Purpose: daily topic-level factor and score facts.

Primary key: (trade_date, topic_type, topic_id)

| Field | Type | Meaning |
|---|---|---|
| trade_date | DATE | Trade date |
| topic_type | VARCHAR | Topic namespace |
| topic_id | VARCHAR | Topic id |
| topic_name | VARCHAR | Topic name snapshot |
| member_cnt | INTEGER | Number of mapped members |
| up_cnt | INTEGER | Members with positive pct_chg |
| limit_up_cnt | INTEGER | Members in limit-up set |
| open_board_cnt | INTEGER | Members with open-board events |
| net_mf_amount | DOUBLE | Aggregated main net inflow amount |
| avg_pct_chg | DOUBLE | Average pct_chg in topic |
| heat_hits | INTEGER | Aggregated heat metric from limit list |
| freshness_days | INTEGER | Cache freshness distance in days |
| anti_fake_score | DOUBLE | Anti-fake risk score |
| anti_fake_flag | INTEGER | Anti-fake risk flag, 1 means risky |
| anti_fake_reason | VARCHAR | Anti-fake trigger reason tags |
| resonance_score | DOUBLE | Final base score |
| resonance_level | VARCHAR | LOW, MID, HIGH |
| llm_resonance_score | INTEGER | Reserved for V2 cloud score |
| is_event_driven_trap | BOOLEAN | Event-driven trap flag for Macro V2 extraction |
| updated_at | TIMESTAMP | Last write timestamp |

## 3. fact_macro_top5_daily
Purpose: ranked top-k topic output per day.

Primary key: (trade_date, rank)

Unique: (trade_date, topic_type, topic_id)

| Field | Type | Meaning |
|---|---|---|
| trade_date | DATE | Trade date |
| rank | INTEGER | Rank in daily top-k |
| topic_type | VARCHAR | Topic namespace |
| topic_id | VARCHAR | Topic id |
| topic_name | VARCHAR | Topic name snapshot |
| resonance_score | DOUBLE | Ranking score |
| anti_fake_flag | INTEGER | Anti-fake flag |
| reason_tags | VARCHAR | Compact reason summary |
| llm_resonance_score | INTEGER | Reserved for V2 cloud score |
| is_event_driven_trap | BOOLEAN | Event-driven trap flag for Macro V2 extraction |
| generated_at | TIMESTAMP | Generation timestamp |

## 4. fact_macro_micro_overlay
Purpose: macro-micro merged signal table.

Primary key: (trade_date, symbol, topic_type, topic_id)

| Field | Type | Meaning |
|---|---|---|
| trade_date | DATE | Trade date |
| symbol | VARCHAR | Stock symbol |
| topic_type | VARCHAR | Selected best topic namespace |
| topic_id | VARCHAR | Selected best topic id |
| micro_verdict | VARCHAR | Micro verdict, PASS or WATCH |
| micro_score | DOUBLE | Micro confidence score |
| macro_score | DOUBLE | Macro topic score |
| anti_fake_flag | INTEGER | Selected topic anti-fake flag |
| composite_score | DOUBLE | Blended macro-micro score |
| signal_label | VARCHAR | Final action label |
| pushed | INTEGER | Push status flag |
| updated_at | TIMESTAMP | Last write timestamp |
