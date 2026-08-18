# 10_us_radar 美股雷达开发计划

## 一、项目定位

`10_us_radar` 是烛龙节点上的多市场领先信号雷达（multi-market leading signal radar）。当前默认目标是：

> 用美股龙头、SEC 公告、内部人交易、13F、财报指引等领先信号，验证其对美股自身和 A 股产业链的传导价值。

它短期不是美股实盘交易系统，但架构必须保留未来切换为美股购买决策系统的能力。

一句话原则：**宽字段、松耦合、窄运行**。

- 宽字段：表结构支持 US/CN 多市场、多 target、多 action。
- 松耦合：事件、证据、target、验证、决策、执行分层。
- 窄运行：当前只启用研究验证，不启用 broker execution。

## 二、当前运行模式

当前配置模式：`research_validation`。嘉信账户仍在审批中，未来可能重新开启“美股购买决策系统”目标，但当前默认仍是研究雷达。

当前启用：

- SEC 事件采集。
- dry-run 证据提取。
- 美股源事件到多市场 target 的映射。
- 后续收益验证：T+1/T+3/T+5/T+20，多基准超额收益和窗口标签。
- 本地报告。

当前关闭：

- 美股实盘下单。
- broker adapter。
- 自动仓位决策。
- 对烛龙 A 股主 L4 决策的直接影响。

## 三、未来切换为美股购买决策系统时怎么扩展

未来如果重新启用美股交易目标，不需要推翻现有设计，只需要打开下一层：

1. 继续复用 `us_events`、`us_llm_signals`、`signal_targets`。
2. 对 `target_market = US` 且 `target_role = primary` 的目标启用美股影子组合。
3. 增加 `us_decision_engine`，根据证据、验证结果、风险规则生成研究动作或影子订单。
4. 增加 `us_risk_policy`，处理单票上限、行业敞口、流动性、盘前盘后等规则。
5. 增加 `broker_adapter`，在独立评审后才允许真实交易。

当前会预留字段，例如：

- `target_market`：US / CN / HK 等。
- `target_ticker`：目标标的。
- `target_role`：primary / transmission / second_order / benchmark / hedge。
- `enabled_for_research`：是否参与研究验证。
- `enabled_for_trading`：是否允许进入交易决策，当前默认 0。

## 四、硬边界

- 不修改 `zhulong_daemon.py`。
- 不把美股任务加入烛龙主 APScheduler。
- 不写烛龙主 DuckDB。
- 不让 `10_us_radar` 信号直接触发 A 股 L4 决策或真实订单。
- 所有运行产物放在 `10_us_radar/` 下。
- LLM 输出只作为证据和假设，不直接生成买卖指令。

## 五、核心数据流

```text
SEC / 新闻 / 财报
  -> normalized event
  -> evidence signal
  -> signal targets (US primary + CN transmission)
  -> validation results (1D/5D/20D/60D)
  -> research report
```

未来交易化时追加：

```text
validation + evidence + risk policy
  -> decision engine
  -> shadow portfolio
  -> broker adapter (optional, gated)
```

## 六、目录结构

```text
10_us_radar/
  PLAN.md
  API_REQUIREMENTS.md
  README.md
  .env.example
  config/
    settings.json
    watchlist.json
    industry_graph.json
    prompts.json
  data/
  logs/
  reports/
  scripts/
    run_once.sh
  src/us_radar/
    cli.py
    config.py
    evidence.py
    industry.py
    reports.py
    schema.py
    sec_edgar.py
    storage.py
  tests/
    test_sec_edgar.py
```

## 七、Phase 0：旁路骨架

已完成目标：证明模块可以独立运行，不碰主进程。

能力：

- 独立 SQLite 数据库：`10_us_radar/data/us_radar.sqlite3`。
- `init-db` 初始化表。
- `init-prompts` 写入 prompt 版本。
- `fetch-sec` 抓取 SEC Atom。
- `extract-evidence` 生成 dry-run 结构化证据。
- `expand-targets` 从产业图谱扩展 US/CN targets。
- `report` 生成 Markdown 报告。

## 八、Phase 1：领先信号与 target 映射

目标：把美股源事件映射为可验证的多市场 targets。

示例：

```text
NVDA 8-K / guidance / capex signal
  -> US: NVDA primary
  -> US: AVGO / AMD / TSM second_order
  -> CN: 中际旭创 / 新易盛 / 天孚通信 transmission
```

验收标准：

- 源事件可入库并去重。
- 源事件可扩展到 `signal_targets`。
- 每个 target 明确 market、ticker、role、theme、confidence。
- `enabled_for_trading` 默认关闭。

## 九、Phase 2：后验验证

状态：MVP 1.1 已接入基础收益验证，下一步是让它持续运行并积累样本。

验证窗口：

- 1D
- 3D
- 5D
- 20D

验证对象：

- 美股源标的相对 QQQ/SPY/SOXX 的超额收益。
- A 股传导标的相对沪深300、行业指数、同链中位数的超额收益。

输出表：

```text
源事件 / target_market / target_ticker / target_role / horizon / target_return / benchmark_return / excess_return / data_quality
```

已预留判定：

- `single_event_signal`：T+1 主超额绝对值大于 2%。
- `short_window_signal`：T+3 主超额绝对值大于 3%。
- `medium_window_signal`：T+5 主超额绝对值大于 5%。
- `reverse_independent_movement`：事件方向与目标走势相反，留给人工复盘。

## 十、Phase 3：研究报告

报告优先中文表格，并直接暴露验证状态：

```text
验证 / T+1主超额 / 判定 / 方向 / 窗口 / 源事件 / 美股源标的 / 产业主题 / 映射标的 / 市场 / 角色 / 证据 / 置信度 / 备注
```

当前待补闭环：

- 每天自动运行，形成不少于两周的沉默样本。
- 单独排查 `NO_PRICE_DATA`，避免行情源异常污染研究结论。
- 生成每周传导有效性复盘，而不是只看事件流水。

## 十一、Phase 3.5：独立推送层

目标：让旁路雷达主动把研究结果送到用户面前，但不混入烛龙主交易推送。

设计原则：

- 独立标题，例如 `【美股雷达旁路】每日传导观察`。
- 独立脚本和日志，不调用烛龙主推送入口。
- 只推研究摘要，不推买卖指令。
- 数据质量异常必须显式出现，例如 `RATE_LIMITED`、`PROVIDER_ERROR`、`NO_PRICE_DATA`。

每日简报建议在北京时间 07:00 左右推送：

- 新增 SEC 事件数。
- 高质量 Form 4 / 8-K 数量。
- 显著传导信号 Top 5。
- A股传导验证状态。
- 美股行情源状态和 API 限频情况。

周报建议在周末推送：

- 六条产业链的传导有效率。
- `true_business` 与 `sentiment` 的表现差异。
- `reverse_to_signal` 和 `no_significant_move` 的复盘候选。
- 数据质量统计和下周需观察的问题。

## 十二、Phase 4：可选交易化

只有在你确认要重新做美股购买决策时才打开：

- 美股影子组合。
- 美股风险规则。
- broker API。
- 订单审计日志。
- 人工确认或自动执行开关。

这个阶段必须单独评审，不能悄悄接入。

## 十三、你需要准备的辅助资料

优先级从高到低：

1. SEC User-Agent 邮箱。
2. 你认可的 US->CN 产业链映射初版，尤其是 AI 算力、电力、光通信、液冷。
3. A 股映射标的是否用股票代码、名称、还是烛龙内部 watchlist ID。
4. 验证基准：A 股默认沪深300，还是行业指数。
5. 推送渠道选择：复用 PushPlus token 但独立标题，或单独配置 Telegram/企业微信。
6. LLM API：Anthropic / OpenAI / DeepSeek，或继续 dry-run。
