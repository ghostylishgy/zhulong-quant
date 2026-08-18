# BL-030 L2/L3 与 RAG Structured Reasoning Contract v0.1

## 1. 冻结结论

烛龙先冻结模型无关的 L2/L3 输出协议，再进行本地模型选型，最后才开发 RAG 推理痕迹写入。本协议不授权任何模型取得裁决权，也不改变当前 L1-L4、Shadow、新闻或交易链路。

冻结顺序：

1. 固定 L2/L3 职责、证据边界和输出字段。
2. 使用同一协议和同一 point-in-time 样本比较候选模型。
3. 冻结胜出模型、后备模型、模型 digest、Prompt SHA、协议版本和 Ollama 版本。
4. 只把通过守卫的结构化推理痕迹接入 RAG。
5. 先观察同标的历史检索，再评审跨标的相似案例和 L5 使用。

禁止为了迁就某个模型的输出习惯反向修改事实语义或 RAG 数据结构。模型可以替换，协议必须稳定。

## 2. 三层职责

### 2.1 L2

确定性 L2 继续是事实标签、风险分和结构事实的权威来源。

L2 observer 只回答：

- 当前结构是健康、混合、衰竭还是无法判断；
- 哪些输入证据相互支持或冲突；
- 缺少什么证据；
- 哪些问题需要交给 L3。

L2 observer 不得输出或覆盖权威 risk score、fact tags、PASS/HOLD/VETO、仓位或交易动作。现有 `L2_OBSERVER_V5` 字段保持冻结。

### 2.2 L3

确定性 L3 继续是当前生产 verdict、score、reasoning 和证伪条件的权威来源。

L3 observer 只回答：

- 候选处于启动、延续、衰竭还是不明确阶段；
- 投资论点是支持、混合、破坏还是未知；
- 支持与风险证据分别是什么；
- 缺失证据和证伪条件是什么；
- 在没有执行权的反事实下建议 PASS、HOLD 或 VETO。

现有 `L3_OBSERVER_V5` 字段保持冻结。`SUGGESTED_GATE` 永远是旁路判断，不是生产 verdict。

### 2.3 RAG

RAG 保存可追溯的事实、决策背景和事后结果，不保存模型不可验证的自由思维过程。

必须区分：

- **允许**：经 schema、证据 ID、方向一致性和指标语义守卫验证后的结构化 reasoning trace。
- **禁止**：原始 `<think>`、隐藏思维链、未经校验的 raw response、模板残留、模型自造阈值和证据外推。

不保存原始思维链不等于放弃推理。烛龙需要的是可审计的决策痕迹，而不是无法复核的模型内心独白。

## 3. RAG_REASONING_TRACE_V1 最小结构

```json
{
  "trace_version": "RAG_REASONING_TRACE_V1",
  "task_id": "",
  "symbol": "",
  "trade_date": "",
  "as_of": "",
  "evidence_ledger_sha256": "",
  "authoritative": {
    "source": "deterministic_l3",
    "verdict": "PASS | HOLD | VETO",
    "score": 0,
    "summary": "",
    "logic_hash": "",
    "invalidation_conditions": []
  },
  "observer": {
    "status": "ACCEPTED | REJECTED | NOT_RUN",
    "contract_version": "L3_OBSERVER_V5",
    "model_id": "",
    "model_digest": "",
    "prompt_sha256": "",
    "ollama_version": "",
    "lifecycle_stage": "",
    "thesis_state": "",
    "suggested_gate": "",
    "quality_score": null,
    "confidence": "",
    "supporting_evidence_ids": [],
    "risk_evidence_ids": [],
    "missing_evidence": [],
    "invalidation_conditions": [],
    "summary": ""
  }
}
```

这是准入协议，不是本阶段的数据库迁移授权。优先复用现有 `facts_json`、`evidence_json`、`signal_task_id` 和 thinking trace 血缘；只有证明现有容器无法可靠承载时，才评审新增字段或表。

## 4. RAG 准入规则

只有同时满足以下条件的 observer trace 才可进入未来的结构化 RAG 记忆：

1. `status=ACCEPTED`；
2. schema 与 contract version 完整；
3. 所有 evidence ID 都来自本次输入 ledger；
4. 不存在模板残留、交易指令、指标误义或证据方向冲突；
5. `as_of`、task identity 和 evidence ledger SHA 完整；
6. 模型、Prompt 和运行时身份可重现。

`REJECTED` 输出只保留为模型质量观测，不进入 RAG 检索。observer 与 authoritative 结论不一致时，两者并列保存，不允许把 observer 覆盖成权威事实。

历史 L3 文本只允许标记为 `LEGACY_SUMMARY_ONLY`。不得从旧叙事反推或补造 evidence ID、生命周期、阈值和证伪条件。

## 5. 检索边界

第一阶段继续保持当前同标的、point-in-time 检索，不因协议冻结立即扩展检索面。

跨标的相似案例属于后续独立评审，最多按以下结构字段召回：

- trade archetype；
- lifecycle stage；
- tide/regime；
- L2 fact tags；
- outcome horizon。

跨标的检索不得迁移公司专属财务事实、公告事实、ticker 身份或事后收益到当前候选。任何 T+1/T+3/T+5/T+10 结果只能用于成熟后的离线后验和 L5 研究，不能泄漏到原审计时点。

## 6. 模型选型门

L2 与 L3 候选必须分别测试，不以参数规模或单次演示决定：

### L2 指标

- 严格 JSON/schema 通过率；
- evidence ID 合法率；
- 冲突和缺失识别率；
- 结构分类区分度，禁止长期塌缩到同一标签；
- 模板残留、指标误义和交易指令为零；
- p50/p95 延迟、超时率和内存占用。

### L3 指标

- 证据引用准确率；
- 生命周期、论点状态与建议门控的一致性；
- 可验证证伪条件质量；
- 无证据断言、指标误义、方向颠倒和自造阈值为零；
- p50/p95 延迟、超时率和内存占用。

更快或更大的模型都不会自动晋级。胜出模型必须在同一冻结样本、同一 evidence ledger 和同一契约上证明增量。

## 7. 实施顺序

1. 本协议冻结，不改生产 RAG 写入。
2. L2 候选模型离线 bake-off。
3. L3 候选模型离线 bake-off。
4. 冻结 champion/fallback 及完整运行指纹。
5. 开发 `RAG_REASONING_TRACE_V1` 的最小写入与读取旁路。
6. 观察自然前向样本，验证可追溯性、检索精度和延迟。
7. 达到样本门后，单独评审跨标的检索与 L5 消费。

## 8. 当前不做

- 不升级 Ollama；
- 不下载或激活新模型；
- 不修改当前 L2/L3 observer 配置；
- 不让 observer 进入 L4 prompt 或裁决；
- 不把 raw chain-of-thought 写入 RAG；
- 不扩展 `agg_tag_performance` 权重权限；
- 不回填或重写历史 L3/L4 verdict；
- 不修改 Shadow、新闻、新旧 L1 联赛或交易参数。

## 9. 状态定义

本协议状态为：**Contract Frozen / Implementation Not Authorized**。

它授权下一步开展离线模型比较，不授权 RAG schema、daemon、数据库或生产链路改动。
