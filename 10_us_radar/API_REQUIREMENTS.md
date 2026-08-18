# 美股雷达 API 与辅助资料准备清单

本文档列出 `10_us_radar` 接下来需要你准备的账号、API key 和研究辅助资料。

当前目标不是立即买美股，而是验证：

> 美股领先事件是否能提前提示美股自身和 A 股产业链的机会/风险。

同时，字段和表结构会保留未来切换为美股购买决策系统的能力。

## 一、Phase 1 必备

### 1. SEC EDGAR User-Agent

SEC 公共数据不需要 API key，也不需要注册账号。自动化访问时需要声明 HTTP `User-Agent`。

推荐格式：

```text
zhulong-us-radar/0.1 your_email@example.com
```

环境变量：

```bash
export US_RADAR_SEC_USER_AGENT="zhulong-us-radar/0.1 your_email@example.com"
```

你需要准备：一个可接收邮件的邮箱。

### 2. US->CN 产业链映射确认

我已经放了初版，但这部分最好由你按产业认知校准。

建议你优先确认四条线：

- AI 算力：GPU / ASIC / HBM / advanced packaging / server。
- AI 电力：发电 / 输配电 / 变压器 / UPS / 数据中心供电。
- 光通信：光模块 / CPO / 硅光 / 高速互联。
- 液冷：液冷设备 / 热管理 / 数据中心基础设施。

你可以直接给我：

```text
美股源标的 -> A股传导标的 -> 传导理由 -> 强/中/弱
```

### 3. A 股验证基准

需要决定 A 股传导验证用什么 benchmark：

- 默认：沪深300 `000300.SH`。
- 更精细：按行业指数或自定义主题篮子。
- 最稳妥：先用沪深300，后续再细分行业基准。

## 二、Phase 2 建议准备

### 1. LLM API Key

用于真实公告/新闻/财报的结构化证据提取。

可选：

- `ANTHROPIC_API_KEY`：长文本与复杂推理优先。
- `OPENAI_API_KEY`：第二模型或成本/速度补充。
- `DEEPSEEK_API_KEY`：非美视角，可选。

当前代码可以继续 dry-run，不会阻塞开发。

### 2. 推送渠道

先不急。等日报格式稳定后再接。

可选：

- Telegram。
- Server 酱 / 企业微信。
- Bark。

## 三、Phase 3 可选

### 1. 行情数据

做后验验证需要行情数据。短期可以先复用已有 A 股数据能力，美股部分后续补。

可选来源：

- yfinance：只适合 MVP 研究，不作为实盘事实源。
- Polygon.io。
- FMP。
- 券商 API。

### 2. 新闻和 transcript

SEC 跑顺后再加：

- 公司 IR RSS。
- NewsAPI / GDELT。
- 财报电话会 transcript。

## 四、未来如果切换到美股购买决策，需要额外准备

这些现在不用准备：

- 美股券商 API。
- 账户权限和 sandbox。
- 真实订单审计规则。
- 美股交易时段、盘前盘后权限确认。
- 税务和币种处理规则。

当前代码会预留字段，但不会启用。
