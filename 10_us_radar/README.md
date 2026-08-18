# 美股雷达旁路模块说明

`10_us_radar` 是烛龙节点上的美股领先信号雷达。当前用途是研究验证：观察美股源事件是否能对美股自身和 A 股产业链产生可量化的领先提示。

它不是当前的实盘交易模块。嘉信账户仍在审批中，未来如果确认可以买入美股，本模块可以在单独评审后扩展为美股购买决策系统；在此之前只做旁路研究验证。

## 当前 MVP 能力

- SEC `8-K` / `Form 4` 抓取、解析、入库。
- SEC Daily Index 手工完整性对账：只比较 watchlist 公告是否漏抓，不自动补录或触发后续链路。
- Form 4 XML 详情解析，提取内部人、交易代码、买卖方向、数量、价格、金额。
- 事件质量分层：`8k_high_signal`、`8k_generic`、`form4_open_market`、`form4_unparsed` 等。
- dry-run 证据提取，记录 prompt/model/input_hash。
- 按产业图谱扩展多市场 targets：US source、CN true_business、CN sentiment。
- 后验收益验证：T+1/T+3/T+5/T+20，记录目标收益、基准收益、主超额、方向标签和窗口标签。
- 多基准归因：A股记录沪深300、行业指数、同链中位数；美股记录 QQQ、SPY、SOXX。
- 中文传导报告：`源事件 -> 美股源标的 -> A股映射 -> 传导类型 -> 后验验证状态`。
- MVP run 标记和派生数据清理命令。

## 当前禁用能力

- 不做真实美股下单。
- 不接 broker API。
- 不自动生成仓位。
- 不影响烛龙 A 股主系统 L4 决策。

## 常用命令

在 `/root/quant_project` 下运行：

```bash
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli init-db
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli reset-derived-data --yes
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli create-run --data-version mvp_watchlist_v1 --purpose research_validation
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli fetch-sec --form-type 8-K --limit 1
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli fetch-sec --form-type 4 --limit 1
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli reconcile-sec-index --date 2026-07-27 --forms 8-K,4
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli enrich-form4 --hours 72 --limit 10
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli classify-events --hours 72
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli extract-evidence --hours 72
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli expand-targets --hours 72
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli prepare-validation
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli validate-returns
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli check-market-data --market US
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli compare-market-data --symbols SPY,QQQ,NVDA --providers yfinance,alpaca,massive
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli check-market-data --market CN --benchmark
PYTHONPATH=/root/quant_project/10_us_radar/src python3 -m us_radar.cli report --hours 72
```

推荐入口：

```bash
10_us_radar/scripts/run_once.sh
10_us_radar/scripts/validate_once.sh
```

建议定时：

- 北京时间 06:45：运行 `run_once.sh`，处理美股夜间 SEC 事件。
- 北京时间 16:40：运行 `validate_once.sh`，等 A股日线更新后补收益验证。
- 不加入烛龙主 APScheduler，不重启 `zhulong_daemon.py`。

## 环境变量

真实 SEC 抓取：

```bash
export US_RADAR_SEC_USER_AGENT="zhulong-us-radar/0.1 your_email@example.com"
```

离线验证：

```bash
export US_RADAR_SKIP_NETWORK=1
```

A股行情默认只读取烛龙只读快照，不允许静默回退活库：

```bash
export US_RADAR_REQUIRE_SNAPSHOT=1
```

若 `storage/database/zhulong_api_readonly.duckdb` 缺失，A股行情会标记为 `PROVIDER_ERROR` 并等待后续重试，不会连接 `zhulong.duckdb`。只有经过人工确认的临时恢复操作才可显式设为 `0`，且仍保持 `read_only=True`。

## 数据路径

- 数据库：`10_us_radar/data/us_radar.sqlite3`
- 日志：`10_us_radar/logs/`
- 报告：`10_us_radar/reports/`

## SEC 日索引对账边界

`reconcile-sec-index` 是人工审计命令，不在 `run_once.sh` 或 cron 中运行。它只读取 SEC Daily Index、watchlist 和旁路 SQLite 中已有的 accession，输出 `OK` 或 `GAP_DETECTED`：

- 不自动把缺失公告写入 `us_events`。
- 不扩展 A股传导标的，不创建验证任务。
- 不读取或写入烛龙主 DuckDB。
- 发现缺口后先人工核对，再决定是否通过现有 Atom 采集路径补抓。

## 下一步：独立推送

当前不会进入烛龙主推送链路。下一步计划增加 `10_us_radar` 自己的研究推送：

- 每日简报：新增 SEC 事件、高质量事件、显著传导 Top 5、数据源状态。
- 周末复盘：产业链有效率、真实业务传导 vs 情绪传导、反向显著信号、数据质量统计。
- 推送标题必须带 `【美股雷达旁路】`，避免和烛龙主交易信号混淆。

## 安全边界

- 不 import 或启动 `zhulong_daemon.py`。
- 不向主 APScheduler 添加任务。
- 不写烛龙主 DuckDB。
- A股行情默认强制读取 `zhulong_api_readonly.duckdb`；快照缺失时失败关闭。
- 所有输出只作为研究和验证材料。
- 美股交易化必须等账户可用后单独评审；当前不启用 broker、不生成真实订单。
