# BL-020 审计漏斗观察器契约 v0.1

## 1. 目标

BL-020 用最小、可复现的旁路产物记录每日审计漏斗与数据质量，帮助区分三类现象：

- 数据缺失或批次不完整；
- 生产门控正常淘汰候选；
- 市场中满足成熟趋势条件的股票本来就稀少。

它不是新的交易门，不改变 L1/L1.5、L2-L4、Shadow、RAG 或交易行为。

## 2. 运行边界

- `observer_only=true`
- `no_trade_signal=true`
- DuckDB 以 `read_only=True` 打开；
- 默认只打印摘要，不写任何文件；
- 只有显式传入 `--write-artifacts` 才写 JSON 与 Markdown 报告；
- 不调用 `decision_engine.py`、`Nexus.run()`，不启动或重启 daemon；
- 不写 DuckDB、`nexus_audits`、Shadow、RAG，也不生成候选或交易任务。

阻断动作必须随产物保存，供下游审计。

## 3. 身份与时间口径

每个批次由以下字段共同绑定：

- `trade_date`
- `run_id`
- `nexus_state.json` 原始字节的 `source_state_sha256`
- 当前代码版本 `code_version`
- 数据证据摘要 `evidence_sha256`

观察器只接受与请求日期和运行编号完全一致、`current_phase=COMPLETED` 且结构完整的回执。同一路径已有不同身份产物时拒绝覆盖；身份相同则复用冻结产物。

## 4. 最小漏斗

输出固定记录以下阶段：

1. `SOURCE_BATCH`：当日 `fact_daily` 总行数与可进入 L1 的基础行数；
2. `L1_RAW`：生产回执中的原始候选数；
3. `L1_5_TREND_HUNTER`：生产回执中的通过数及冻结原因统计；
4. `L2`：输入、审计行数与通过数；
5. `L3`：评估数与非 VETO 数；
6. `L4`：终态行数及 PASS/HOLD/VETO 数。

L1.5 的原因计数可能重叠，不允许相加后当作唯一淘汰数。

## 5. 数据金丝雀

以下任一失败，观察器产物标记为 `DATA_FAIL_CLOSED`：

- 回执日期、运行编号、完成状态或必要结构不一致；
- 回执阶段数量不单调或 L1.5 报告异常；
- `fact_daily` 最新日期不等于审计日期；
- 当日股票代码重复或批次为空；
- 当日行数低于最近 20 个批次中位数的 80%；
- `close`、`pct_chg`、`vol`、`amount` 等关键字段缺失或非法；
- `fact_rps_results` 缺表或缺少精确的 symbol-date 对；
- 非零审计行未全部进入终态，且又不能由精确零候选回执解释。

`DATA_FAIL_CLOSED` 只判定本次观察产物不可用，不回写或推翻已经由生产链证明完成的历史审计。

## 6. 观察预警

以下情况只产生 `WARN`，不得自动调参或阻断生产链：

- L1.5 零通过；
- 全市场成熟均线多头排列比例低于 3%；
- L1 Top50 中出现北交所、ST 或历史长度不足的占位噪声。

这些信号描述市场与策略适配状态，不代表数据故障，也不能证明 L1/L1.5 应当放宽。

## 7. 市场诊断代理

旁路诊断使用稳定排序 `pct_chg DESC, symbol`，并计算：

```text
MA5 > MA10 > MA20 > MA60
AND current_volume / previous_14_day_average_volume >= 1.2
```

该代理只用于回答“市场中大约有多少成熟趋势延续对象”，不声称完整复刻生产 TrendHunter，也不参与候选选择。

## 8. 双轴状态

数据质量轴：

- `DATA_OK`：没有任何硬数据或完成性失败；
- `DATA_FAIL_CLOSED`：至少一个硬金丝雀失败，本观察产物不可用于比较。

观察状态轴：

- `OBSERVE_CLEAR`：没有市场或漏斗观察预警；
- `OBSERVE_WARN`：至少一个 warn-only 条件出现。

因此“数据完整但 L1.5 零入选”的正确表达是 `DATA_OK + OBSERVE_WARN`，不能写成数据质量降级。

## 9. 产物

显式启用写出时生成：

```text
storage/reports/audit_funnel_observer/
  audit_funnel_<YYYYMMDD>_<run_id>.json
  audit_funnel_<YYYYMMDD>_<run_id>.md
```

JSON 是权威结构化产物；Markdown 只做人读预览。默认 dry-run 不创建目录或文件。

## 10. 验收边界

- 合法零候选批次必须得到 `DATA_OK + OBSERVE_WARN`；
- 数据日期滞后、RPS 缺配或审计未完成必须得到 `DATA_FAIL_CLOSED`；
- 正常非零批次可以得到 `DATA_OK + OBSERVE_CLEAR`；
- 同一冻结身份重复运行可复用，证据或代码身份变化时拒绝覆盖；
- 任何结果都不得改变生产审计、阈值、交易信号或 daemon 状态。

## 11. 分阶段接入

- Phase A：独立只读 CLI、冻结契约、合成测试和真实快照 dry-run；
- Phase B：仅在生产审计原始完成状态已经确认后，由 daemon 触发旁路写出；
- Phase C：积累结构化序列后再评审告警阈值，观察期内禁止自动调参。
