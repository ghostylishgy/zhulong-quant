# BL-030 本地模型评测契约 v0.2

日期：2026-08-01
状态：离线评测授权，生产权限不变

## 1. 修订原因

旧 L2 评测把原始数字解释、指标方向、结构分类、证据冲突和 JSON 契约合并成一个任务，并把不同类型的失败压成单一语义通过率。五个候选分别出现格式失败、全量 `HEALTHY` 和全量 `UNKNOWN`，说明旧任务无法可靠区分模型容量、任务错配和合理判断分歧。

L3 的旧 observer 契约同时要求生命周期、论点状态、建议门、质量分、证据引用和自造文本证伪条件。建议门与分数会把旁路论证重新拉向裁决，并放大字段间连锁失败。

v0.2 只修改离线评测职责，不修改生产 `L2_OBSERVER_V5`、`L3_OBSERVER_V5`、确定性 L2/L3、L4、RAG、Shadow 或 daemon。

## 2. 共同原则

1. 原始值的单位、方向和基础语义由确定性代码解释后形成 evidence envelope。
2. 模型只能引用 envelope 内的 evidence ID、missing ID 和预生成 invalidation ID。
3. 净卖出解释为净买入、RPS 误作历史价格位置、VR 误作资金流、补写输入外事实、交易指令和自造数字仍是一票否决。
4. 相邻状态或合理风险偏好差异单独记为 judgment error，不与事实错误混合。
5. 使用多数类常数 dummy、输出分布和最大标签占比检测模式塌缩。
6. 模型输出只有观察权限，不覆盖确定性状态，不进入 L4、RAG、Shadow 或交易链路。
7. 保存最终结构化响应和评测错误分类；不保存 raw hidden thinking。

## 3. L2_INTERPRETATION_REVIEW_V2_1

L2 不再从原始数字生成权威结构状态。输入包含确定性事实包、确定性状态和一条待审解释。初版把 `CONFLICT` 与 `UNSUPPORTED` 设为互斥主标签，实测发现同一错误解释既可能与证据冲突，也可能属于证据外推，互斥标签会制造伪错误。

修订后主轴只回答解释是否成立：

- `SUPPORTED`：解释与有效证据一致；
- `REFUTED`：有效证据反驳解释，或解释存在方向颠倒和证据外推；
- `UNVERIFIABLE`：缺失、陈旧或含混证据使解释目前无法验证。

证据内部是否存在实质冲突由独立布尔字段 `conflict_present` 表达，不再与主标签争夺同一语义位置。

输出字段：

```json
{
  "contract_version": "L2_INTERPRETATION_REVIEW_V2_1",
  "interpretation_status": "SUPPORTED | REFUTED | UNVERIFIABLE",
  "conflict_present": false,
  "evidence_ids": [],
  "conflict_evidence_ids": [],
  "missing_evidence_ids": [],
  "confidence": "LOW | MEDIUM | HIGH",
  "summary": ""
}
```

模型没有 risk score、fact tags、PASS/VETO、仓位或交易动作字段。

## 4. L3_ARGUMENT_REVIEW_V2_1

L3 接收相同的确定性 evidence envelope，并审查候选论点。模型保留语言推理空间，但不能输出建议门或质量分。证伪条件由程序预生成，模型只能选择 ID，不能发明阈值。

```json
{
  "contract_version": "L3_ARGUMENT_REVIEW_V2_1",
  "lifecycle_stage": "INITIATION | CONTINUATION | EXHAUSTION | UNCLEAR",
  "thesis_state": "SUPPORTED | MIXED | BROKEN | UNKNOWN",
  "bull_evidence_ids": [],
  "bear_evidence_ids": [],
  "contradiction_evidence_ids": [],
  "missing_evidence_ids": [],
  "selected_invalidation_ids": [],
  "confidence": "LOW | MEDIUM | HIGH",
  "abstain": false,
  "summary": ""
}
```

`UNKNOWN` 必须 `abstain=true`。其他状态不得通过自然语言变相覆盖确定性结论。

## 5. 样本与错误分层

冻结样本为二十四个 point-in-time 合成案例，L2 三种解释状态各八例，L3 四种生命周期和四种论点状态各六例。覆盖趋势延续、早期启动、价格资金背离、情绪衰竭、反转未确认、财务冲突、新闻风险、板块共振、孤立强势、陈旧数据、公司行为、RAG 冲突与 RAG 缺失。

禁止结论正则只识别模型自身断言，不把“无法确认健康”“解释声称健康但证据矛盾”等明确反驳句误记为方向错误。v2.1 进一步加入句级引述归属、转折范围和匹配内部否定识别，区分“候选论点认为风险不会影响”与模型认可该观点，也区分“无机构行为证据”与“确认没有机构参与”。真实正向断言仍保持一票否决。

错误分层：

| 层级 | 含义 | 处理 |
|---|---|---|
| Schema | JSON、字段、枚举或 ID 绑定错误 | 工程淘汰 |
| Fatal fact | 交易指令、自造数字或输入外事实 | 一票否决 |
| Direction | 明确方向颠倒或命中禁止结论 | 一票否决 |
| Judgment | 可接受标签之外但未颠倒事实 | 独立计分 |

## 6. 晋级门

候选必须同时满足：

- Schema 通过率不低于百分之九十五；
- fatal fact error 为零；
- direction error 为零；
- strict pass 和主标签准确率均不低于百分之七十五；
- 主标签准确率超过多数类常数 dummy 至少二十个百分点；
- 任一主标签占比不高于百分之六十；
- 证据覆盖率不低于百分之七十五；
- L2 冲突识别或 L3 生命周期识别达到对应门槛。

这些门槛不因所有模型失败而降低。没有候选通过时，结果必须为 `NO_CHAMPION`。

## 7. 运行边界

评测工具只调用 Ollama 并写 `storage/reports/model_benchmarks`。禁止写 DuckDB、RAG、Shadow、`nexus_audits`，禁止修改 daemon 配置、生产模型权限或生成交易信号。生产模型切换、Prompt 升级和 RAG reasoning trace 仍需独立评审。
