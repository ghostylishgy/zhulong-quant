# BL-019 权威交易日历契约 v0.1

## 1. 目的

烛龙所有按交易日开放的运行阶段，不再依赖“工作日近似交易日”或只覆盖单年的静态节假日表。Tushare `trade_cal(exchange='SSE')` 是权威事实源，DuckDB 本地缓存是生产运行时唯一读取入口。

本契约只回答“某日、某权限域是否允许运行”，不生成股票候选、审计结论或交易信号。

## 2. 最小数据结构

### `fact_trade_calendar`

- `(exchange, cal_date)` 是唯一键。
- `is_open` 保存交易所开闭市事实。
- `pretrade_date` 保存上一交易日。
- `source`、`fetched_at`、`payload_sha256` 绑定来源、时间和整批内容。
- 周度刷新在一个事务内替换 API 实际返回的连续日期区间。空响应、重复冲突和日期缺口均拒绝写入。

### `ops_trade_calendar_override`

- 人工 override 只允许单日、单权限域、append-only 留痕。
- 每条记录必须包含 `reason`、`created_by`、`created_at`。
- 新记录不覆盖旧记录；同一日期和权限域以最新记录生效。

## 3. 权限域

- `AUDIT`：数据收割、盘后审计、观察器和其他研究阶段。
- `ENTRY`：Shadow T+1 买入探测、主轮和最终复核。

两者相互独立。`AUDIT` 人工开放不会打开 `ENTRY`；`ENTRY` 人工开放也不会修改 `AUDIT`。周末是不可 override 的硬关闭日。

## 4. 决策状态

- `OPEN` / `CLOSED`：权威缓存正常命中。
- `WEEKEND_CLOSED`：周末硬关闭。
- `UNKNOWN_DATE`：权威缓存无该日期，fail-closed。
- `STALE_CACHE`：对应记录超过 14 天未刷新，fail-closed。
- `SOURCE_DISAGREEMENT`：Tushare 与可用的公共节假日库方向冲突，fail-closed。
- `CACHE_ERROR` / `INVALID_DATE`：缓存或输入异常，fail-closed。
- `MANUAL_OVERRIDE_OPEN` / `MANUAL_OVERRIDE_CLOSED`：单日、单权限域人工决定。

`chinese_calendar` 只是交叉校验源。超出其支持年份时，只要 Tushare 缓存新鲜且明确，不能因辅助库无数据而误关；Tushare 缓存自身缺失时仍必须关闭。

## 5. 覆盖范围与刷新

- 每周日 18:30 请求当年 1 月 1 日至下一年 12 月 31 日。
- Tushare 尚未发布完整远期日历时，写入其已返回且连续的区间，同时标记 `SOURCE_HORIZON_TRUNCATED`。
- 覆盖检查使用最近一次刷新批次的最大日期，默认至少保留 90 个自然日。
- `HORIZON_SHORT` 触发专用预警；缺表、空表、缺口、当前日期缺失或已过期属于严重覆盖异常。
- 已知缓存日仍可按事实运行；任何未知日期一律关闭。

## 6. CLI

刷新：

```bash
python tools/manage_trade_calendar.py \
  --db storage/database/zhulong.duckdb \
  refresh --env-file .env
```

只读状态：

```bash
python tools/manage_trade_calendar.py \
  --db storage/database/zhulong.duckdb \
  status --date 2026-07-28 --scope AUDIT
```

人工 override：

```bash
python tools/manage_trade_calendar.py \
  --db storage/database/zhulong.duckdb \
  override --date 2026-07-28 --scope AUDIT --state open \
  --reason "已核对交易所公告" --created-by operator \
  --confirm MANUAL_CALENDAR_OVERRIDE
```

人工 override 是异常处置，不是日常开关。不得用 `TRADE_DATE` 环境变量绕过缓存决定。

## 7. 明确不做

- 不修改 L1-L4、潮汐、RAG、新闻或 Shadow 成交规则。
- 不激活 `07_macro`。
- 不因交易日历开放而自动产生审计候选或买入计划。
- 不把公共节假日库重新提升为主事实源。
- 不自动猜测未发布的下一年度交易日。
