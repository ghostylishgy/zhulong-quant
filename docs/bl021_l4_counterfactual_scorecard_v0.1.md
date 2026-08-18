# BL-021 L4 同口径反事实记分卡 v0.1

## 1. 要回答的问题

低 PASS 率本身不能说明 L4 过严，也不能说明 L4 有效。记分卡只回答：

> 在相同的可观察安全条件和相同的价格代理下，L4 的 PASS、HOLD、VETO 是否呈现可重复的后续收益分离？

它不是策略回测器，也不重新裁决历史记录。

## 2. 统一价格口径

v0.1 固定使用：

```text
T+1 权威交易日开盘价买入代理
T+3 权威交易日收盘价退出代理
PRICE_MODEL=T1_OPEN_T3_CLOSE_GROSS_PROXY_V0.1
```

交易日来自 `fact_trade_calendar`。若 T+1 停牌、无量、无成交额、一字价或缺少该固定会话的数据，样本不可评估；程序不得顺延到下一次有报价的日期。

当前收益为未计费用的诊断性 gross proxy。它只用于比较三个 verdict 在同一尺子下的相对分离，不等同于可实现收益或生产成交。

v0.1 只对 PASS/HOLD/VETO 共有且可追溯的资格、新闻和固定价格窗口做同口径比较，不重建仅在 PASS 后才进入的全部 Shadow 买前门控。潮汐、监管及其他真实执行条件不得被推定为已满足；它们的影响只在单独列示的实际 PASS Shadow 成交结果中观察。因此本记分卡始终是诊断工具，不是生产执行回放。

## 3. 样本资格

进入版本化统计必须同时满足：

1. `nexus_audits.status=L4_DONE` 且 verdict 为 PASS/HOLD/VETO；
2. 存在与 `trade_date + run_id` 精确匹配并验签通过的 BL-021 binding artifact；
3. 非 ST、非北交所，且 `fact_stock_basic` 身份可确认；
4. L4 新闻观察为 `NEWS_CLEAR` 或 `NEWS_SIGNAL`，不存在 `WOULD_CAP_HOLD/WOULD_VETO`，且 `l4_news_as_of` 与运行 binding 的 `evidence_as_of` 完全一致；
5. T+1 可成交代理有效，T+1 至 T+3 固定窗口数据完整。

历史未绑定样本可以生成 `DIAGNOSTIC_UNBOUND_MATURED` 描述统计，但不得进入版本化结论。风险或执行条件不满足的样本保留排除原因，不删除原始审计记录。

## 4. 输出指标

按 PASS/HOLD/VETO 分组输出：

- 样本数；
- T+1 open 到 T+3 close 平均/中位收益；
- 正收益比例；
- T+1 至 T+3 的 MFE/MAE。

真实 PASS 的 Shadow 仓位与实际盈亏单独汇总，不与 HOLD/VETO 的价格代理混合比较。

## 5. 评审边界

至少 50 个 `ELIGIBLE_BOUND_MATURED` 样本后，状态才可以变为 `READY_FOR_FIRST_DESIGN_REVIEW`。达到数量门只允许人工评审，不证明 Alpha，不自动修改 L4、阈值、Prompt、Shadow、RAG 或交易策略。

v0.1 不执行显著性停机、placebo、Prompt 重放、自动晋级或参数优化。

## 6. 安全边界

工具以 DuckDB `read_only=True` 打开数据库，默认只在终端输出摘要；只有显式 `--write-artifacts` 才写 JSON/Markdown 报告。

禁止：

- 写 DuckDB、Shadow、RAG、`nexus_audits`；
- 改写历史 verdict 或分数；
- 生成交易任务或推送买入建议；
- 修改 L1-L4 阈值；
- 触发或重启 daemon；
- 根据小样本自动调整生产策略。

## 7. CLI

只读预览：

```bash
python3 tools/review_l4_counterfactual_scorecard.py \
  --start-date 2026-07-01 \
  --end-date 2026-07-30
```

显式写报告：

```bash
python3 tools/review_l4_counterfactual_scorecard.py \
  --start-date 2026-07-01 \
  --end-date 2026-07-30 \
  --batch 20260730 \
  --write-artifacts
```
