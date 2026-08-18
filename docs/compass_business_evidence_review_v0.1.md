# Compass Business Evidence Review v0.1

## 1. 角色

该工具位于 Business Evidence Snapshot v0.2 之后，专门处理原 `broad_universe` 中已经命中具体主营节点的对象。

它把“机器发现了具体业务证据”和“人同意把该对象交给后续 discovery 适配器”分成两步。人工 `promote` 不直接改变原 discovery pool，不初始化既有 BL-011 manifest，也不生成 validation task。

```text
business evidence snapshot v0.2
  -> editable business-evidence review manifest
  -> reviewed-promotion artifact
  -> future separately reviewed discovery adapter
```

## 2. 可审阅范围

只有同时满足以下条件的行会进入 manifest：

- 来源为 `broad_universe_recheck`；
- 原池仍为 `broad_universe`；
- `business_review_eligibility=eligible_specific_node`；
- 至少有一个 `review_qualifying_evidence_item_id`；
- 上游未取得 review 资格；
- `generate_task=false` 且 `no_trade_signal=true`。

`B0_CONTEXT_ONLY_GENERIC`、原 `review_shortlist`、锚点和辅助描述命中不会进入该 manifest。北交所对象可以保留为研究审阅样本，但本工具同样不会为其生成任务。

## 3. SHA 绑定

manifest 同时绑定：

- 业务证据快照文件 SHA；
- 业务快照 payload SHA；
- 上游 discovery preview SHA；
- 节点词典 SHA；
- 每一条业务证据行的 canonical SHA；
- `evidence_as_of`。

任一上游文件、词典、证据行或时间口径发生变化，旧 manifest 都会 fail-closed。

## 4. 人工决策

允许值：

- `pending`：尚未复核；
- `promote`：允许进入未来独立 discovery adapter；
- `keep_broad`：证据存在，但仍留在宽池；
- `reject_mapping`：人工认为该映射不成立。

`promote` 必须选择至少一个已冻结的 qualifying evidence item，并填写审核人、带时区审核时间和审核说明。其他决策不得携带 selected evidence ID。

## 5. CLI

初始化 manifest：

```bash
python3 tools/compass_business_evidence_review.py \
  --snapshot /tmp/compass_business_w30_v02.json \
  --init-manifest-output /tmp/compass_business_review_manifest_w30.json
```

人工编辑 manifest 后应用：

```bash
python3 tools/compass_business_evidence_review.py \
  --snapshot /tmp/compass_business_w30_v02.json \
  --review-manifest /tmp/compass_business_review_manifest_w30.json \
  --json-output /tmp/compass_business_evidence_review_w30.json \
  --preview-output /tmp/zhulong_compass_business_evidence_review_w30.md
```

## 6. 冻结边界

本工具不会：

- 改写 `broad_universe` 或 `review_shortlist`；
- 让对象直接进入既有 BL-011 review manifest；
- 生成 validation task；
- 写 DuckDB、Shadow、RAG 或 `nexus_audits`；
- 调用 decision engine、Nexus 或 daemon；
- 形成交易信号。

后续若开发 discovery adapter，必须另行评审其输入 SHA、市场资格与任务生成边界。本工具的 `promote` 只是人工认可业务映射，绝不是买入判断。
