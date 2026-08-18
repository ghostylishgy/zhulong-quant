# BL-023 合成安全门击发契约 v0.1

## 目标

以生产模块本体和当前规则验证买入前关键负约束确实在正确阶段生效，并输出精确原因码。该套件只证明安全门行为，不证明策略收益，不改变任何生产参数。

## 覆盖范围

- ST：`ACCOUNT_INELIGIBLE_ST`
- 默认账户北交所限制：`ACCOUNT_INELIGIBLE_BJ`
- 新闻重大风险：`SKIPPED_NEWS_RISK`
- 新闻谨慎风险：`SKIPPED_NEWS_CAUTION`
- 新闻未完成：`WAIT_NEWS_CHECK`
- 市场潮汐禁止入场：`TIDE_SUPPRESSED_NO_SHADOW_ENTRY`
- T+1 固定会话无日线/停牌代理：`WAIT_DATA + DAILY_MISSING`
- T+1 一字涨停：`UNFILLED + LIMIT_UP_ONE_PRICE`
- 过期实时行情：`QUOTE_STALE_DATE`
- 新闻拦截 PushPlus：中文说明且网络请求必须由 stub 接管

## 隔离规则

1. 测试日志使用 `tests/_test_log_isolation.py` 的临时目录。
2. 股票资格查询只读访问临时 DuckDB；测试前后校验 fixture SHA 不变。
3. 所有终态写入、`shadow_processed`、pending queue 和 PushPlus 网络请求均使用 mock/stub 捕获。
4. 不读取或写入生产 DuckDB，不写 Shadow/RAG/nexus_audits，不启动 daemon。
5. 每个门级测试显式放行它之前的门，避免“没有成交”被错误当成目标门已经生效。

## 判定边界

本套件是集成安全回归，不是完整交易重放。它不覆盖真实 Tushare 可用性、实际 PushPlus 到达率、成交费用、盘中流动性或后验收益；这些仍由生产观察与独立后验工具验证。
