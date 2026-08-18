# BL-029 Local Model Audit Contract v0.1

## 1. 目标

恢复本地模型在烛龙审计中的有效工作，但不恢复旧版“模型直接覆盖事实、评分和裁决”的权力。当前阶段的目标是形成可追溯的本地语义观察结果，并用前向样本证明其增量价值。

## 2. 最小模型编制

| 模型 | 当前职责 | 是否进入生产审计 | 是否有裁决权 |
|---|---|---:|---:|
| `mxbai-embed-large` | RAG embedding | 是 | 否 |
| `lfm2.5-thinking:1.2b` | L2 结构、冲突、缺失证据观察 | 是，observer | 否 |
| `qwen2.5:1.5b` | 仅复核确定性 L2 灰区或风险标签 | 是，review sidecar | 否 |
| `fin-auditor-observer:v0.1` | 基于 fin-auditor 7B 权重的 L3 生命周期、论点和证伪条件审计 | 是，observer | 否 |
| `llama3.2:3b` | 离线基准 | 否 | 否 |
| `deepseek-r1:1.5b` | 离线基准 | 否 | 否 |

3B 与 1.2B/1.5B 的能力并不相同，但在当前硬件和任务分工下，3B 没有形成独立生产角色：它比 L2 主模型更重，又明显弱于 7B 的 L3 审计能力。保留已安装模型便于离线比较，不为“模型数量完整”恢复调用。

## 3. 权限三态

- `off`：不调用模型，确定性 L2/L3 正常运行。
- `observer`：调用、解析、校验并持久化模型输出；确定性事实、标签、分数、L3 verdict 和 L4 输入保持权威。
- `authority`：未来状态。本阶段禁止启用，必须先通过回放、前向观察和单独设计评审。

兼容原则：旧 `L2_MODEL_ENABLED=1` 只等价于 L2 observer；旧 `L3_MODEL_ENABLED=1` 不再隐式取得 authority。

## 4. L2 Observer 契约

输入只包含审计时点已存在的字段，并为每项证据分配稳定 ID：

- `P.PCT_CHG`、`P.CLOSE`、`P.AMOUNT`、`P.TURNOVER`、`P.VOL_RATIO`、`P.RPS10`
- `T.PATTERN_SCORE`、`T.MA_ALIGNMENT`、`T.PATTERN_NAME`
- `Z.LHB_NET`、`Z.INST_FLOW`、`Z.HOT_MONEY`、`Z.MARGIN_DELTA`、`Z.BLOCK_VOL`、`Z.BLOCK_PREMIUM`

输出为严格 JSON：

```json
{
  "contract_version": "L2_OBSERVER_V5",
  "structure_state": "HEALTHY | MIXED | EXHAUSTED | UNKNOWN",
  "supporting_evidence_ids": [],
  "risk_evidence_ids": [],
  "evidence_conflicts": [],
  "missing_evidence": [],
  "questions_for_l3": [],
  "confidence": "LOW | MEDIUM | HIGH",
  "summary": ""
}
```

模型不得输出 risk score、股票标签、PASS/HOLD/VETO 或买卖动作。弱证据只能提出问题，不能升级为事实。

## 5. L3 Observer 契约

L3 输入包含同一 evidence ledger、确定性 L2 结果，以及有来源和时间边界的 RAG 摘要。新闻继续保持 `OBSERVE_ONLY`，不得因为本次改造注入 L3。

输出采用短格式契约：

```text
[L3_OBSERVATION]
CONTRACT_VERSION: L3_OBSERVER_V5
LIFECYCLE_STAGE: INITIATION | CONTINUATION | EXHAUSTION | UNCLEAR
THESIS_STATE: SUPPORTED | MIXED | BROKEN | UNKNOWN
SUGGESTED_GATE: PASS | HOLD | VETO
QUALITY_SCORE: 0-100
CONFIDENCE: LOW | MEDIUM | HIGH
SUPPORTING_EVIDENCE_IDS: ...
RISK_EVIDENCE_IDS: ...
MISSING_EVIDENCE: ...
INVALIDATION_CONDITIONS: ...
SUMMARY: ...
[/L3_OBSERVATION]
```

`SUGGESTED_GATE` 只是旁路反事实，不是生产 verdict。证据 ID 必须来自输入；缺失数据必须记为 unknown，不能把 RPS 当历史价格位置、把 VR 当资金流、把 RAG 缺失当负面证据。

## 6. 与新 L1 和全生命周期的关系

- 新 L1 Path Observer 仍是观察链，不作为本次提示词的权威输入。
- 当前生产 L1.5 的 `pattern_score / ma_alignment / pattern_name` 可以作为已存在的结构事实。
- 模型只能描述候选处于启动、延续、衰竭或不明确阶段，不得据此生成订单。
- 后续若新 L1 晋级，必须新增版本化 path evidence ID，不能让模型从价格文本自行猜路径标签。

## 7. 验收门

进入 observer 前：

- 当前提示词和代码 SHA 已冻结；
- 对抗测试覆盖模板残留、虚构事实、指标误义、交易指令、未知证据和证据 ID 越界；
- observer 输出不能改变确定性 L2/L3 或 L4 输入；
- 模型失败、超时、解析失败必须安全退回确定性路径。

进入 authority 评审前：

- 至少 30 至 50 个分层历史样本完成 point-in-time 回放；
- 至少 20 个自然前向候选形成 observer 结果；
- 模板残留和无证据断言为零；
- 证据引用覆盖率、结论一致性和延迟达到冻结门槛；
- 必须另行评审，不能通过环境变量静默升级。

## 8. 不变量

- 不修改历史 L3/L4 verdict。
- observer 不改变 L4 prompt、Shadow、RAG 写入和交易资格。
- 不启用 llama 3B 或 DeepSeek 1.5B 生产调用。
- 不因为本地模型重新运行而降低现有 evidence guard。
- 模型输出属于待验证观察，不是事实层。

## 9. 版本化模型身份

旧 `fin-auditor:latest` 内置了“10只标的、反向证伪、Suggested_Entry、均线入场价”的历史 System Prompt，与当前全生命周期和无交易指令契约冲突。BL-029 不覆盖旧模型，而是用同一 7B 权重建立版本化别名 `fin-auditor-observer:v0.1`。

可复现 Modelfile 位于 `02_brain/models/fin_auditor_observer.Modelfile`，并纳入 BL-021 审计合同文件集合。生产模型别名不得静默覆盖；后续版本使用新 tag 和新合同 SHA。
