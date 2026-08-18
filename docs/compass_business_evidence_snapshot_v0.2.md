# Compass Business Evidence Snapshot v0.2

## 1. 角色

该组件位于 Theme Discovery 与人工 review manifest 之间，用于回答一个窄问题：候选公司的公开主营描述或主营产品项，是否直接命中 Compass 已冻结的产业链节点。

它不是新的选股器，不改变 `anchor_reference`、`review_shortlist`、`broad_universe` 或 `diagnostic_sample`，也不会自动证明某个标的值得验证或交易。

```text
ima normalized report
  -> SW/THS discovery source snapshot
  -> theme discovery three-pool preview
  -> business evidence snapshot (this component)
  -> future human review integration, separately approved
```

## 2. 输入

- `compass_theme_discovery_preview_v0.2` JSON；
- `config/compass_business_nodes_v0.2.json`；
- 显式 `evidence_as_of`；
- 当前可用的 Tushare `stock_company` 与 `fina_mainbz` API。

纳入证据复核：

- `review_shortlist`；
- `broad_universe`；
- exact-resolved 的 `anchor_reference`。

`diagnostic_sample` 永不查询。v0.2 默认最多接收 500 个唯一股票；超过上限整批输出 `INPUT_LIMIT_EXCEEDED`，且不发 API 请求，不做截断或假 Top-N。

公司描述按输入股票所需交易所批量读取，避免逐股调用。只有 `main_business` 命中节点的股票才有资格继续查询 `fina_mainbz`，默认上限为 30 个唯一股票。`business_scope` 和 `introduction` 命中只作辅助，不单独形成 B1。若主营描述命中数超过上限，保留全部 B1 行并输出 `PARTIAL_MAINBZ_LIMIT`，但不调用任何 `fina_mainbz`，也不截断候选。

## 3. 节点词典与审阅资格

v0.2 只覆盖：

- A 线：变压器、分接开关、取向硅钢、输变电设备、数据中心供配电；
- I 线：封装基板、先进封装、HBM 相关材料、半导体封装材料。

词典文件的原始 SHA 写入每份快照。v0.2 在每个节点内显式声明 `review_qualifying_terms`。节点的 `terms` 负责保留上下文命中；只有 `review_qualifying_terms` 命中才具备进入未来人工晋级审阅的资格。

例如 A 线中的 `电力设备`、`电网设备` 过于宽泛，只保留为 `generic_node_context`；`特高压`、`输变电`、`高压开关` 才是该节点的审阅资格词。这个区分不会删除原始证据，也不会改变 discovery pool。

词典变更必须走 Git diff；LLM 可以提出候选词，但不得直接修改股票、池、证据等级、审阅资格或任务状态。

## 4. 证据等级

| 等级 | 含义 | 能否自动生成 task |
|---|---|---:|
| B0_NO_DIRECT_MATCH | 没有主营描述或主营产品节点命中 | 否 |
| B0_CONTEXT_ONLY_GENERIC | 主营仅命中宽泛上下文词，不具备人工晋级审阅资格 | 否 |
| B1_COMPANY_DESCRIPTOR | 当前公司 `main_business` 直接命中节点 | 否 |
| B2_REPORTED_BUSINESS_ITEM | 最新可见报告期的主营产品项命中节点 | 否 |

`business_evidence_level` 描述证据来源层级，`business_review_eligibility` 描述该证据是否足够具体，二者不能互相替代。具体节点证据仍然只获得 `eligible_specific_node`，不等于已确认业务相关，也不等于可生成 task。

`fina_mainbz` 的产品行不保证存在可靠且不重复的收入分母，因此 v0.2 不自动计算营收占比。`revenue_share=null` 是诚实缺省，不是数据错误。

## 5. 时间语义

`stock_company` 是当前公司描述，`fina_mainbz.end_date` 是报告期而非公告时间。快照记录当前 API 获取时间和报告期，但不能重建“某个历史交易日当时已经公开了什么”。

报告期距离 `evidence_as_of` 超过 550 天时标记 `report_period_stale=true`。该标签只提醒人工复核业务是否仍然存在，不自动降级、排除或生成任务。

因此：

- 该证据可供当前批次人工核验；
- 不得用于历史回测中的 point-in-time 强证据；
- 不得把报告期等同于公告日；
- 不得回填进过去的 L4、Shadow 或 RAG 决策上下文。

## 6. CLI

```bash
python3 tools/compass_business_evidence_snapshot.py \
  --input storage/reports/compass_ingest/compass_discovery_preview_2026W30.json \
  --evidence-as-of 2026-07-30 \
  --max-input-symbols 500 \
  --max-mainbz-symbols 30
```

默认只打印 JSON。只有显式增加 `--write-artifacts` 才会写入：

```text
storage/reports/compass_ingest/compass_business_evidence_snapshot_<batch>.json
storage/reports/compass_ingest/zhulong_compass_business_evidence_snapshot_<batch>.md
```

## 7. 冻结边界

v0.2 禁止：

- 修改 discovery pool；
- 自动批准 business relevance；
- 初始化或覆盖人工 review manifest；
- 生成 validation task；
- 写 DuckDB、Shadow、RAG 或 `nexus_audits`；
- 调用 decision engine、Nexus 或 daemon。

真实 W30 语义复核发现，`电力设备` 这类宽泛主营词会把上下文误写成可审阅证据，因此 v0.2 增加了独立审阅资格。下一步仍不是自动接线，而是单独设计“证据快照 SHA 如何被人工晋级 manifest 引用”。
