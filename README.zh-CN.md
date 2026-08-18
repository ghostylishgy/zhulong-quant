# 烛龙量化系统

[English README](README.md)

烛龙是一个面向 A 股的可解释研究与影子交易审计系统。它把确定性行情处理、受限本地模型辅助、云端模型法庭、新闻与市场环境风控、全生命周期模拟持仓以及事实优先的策略记忆组合在一起。

本仓库是经过脱敏的公开源码快照，不包含生产数据库、行情缓存、向量库、API 密钥、私有环境文件、运行日志、真实持仓配置或内网地址。

> 烛龙不连接券商，也不会发出真实下单指令。它是研究软件，不构成投资建议，也不是公开荐股服务。

## 设计目标

系统围绕一个问题工作：

> 哪些候选值得接受完整、可追溯、受证据约束的审计，之后才有资格进入 T+1 影子交易计划？

烛龙把候选发现、资格审查、买入计划、持仓复核、卖出和后验学习视为同一条生命周期。单日强势只是调查起点，不等于可以买入。

## 当前审计链路

```text
行情收获
  Tushare 与本地数据契约 -> DuckDB 快照

候选召回
  旧 L1 强度筛选
  + 独立路径观察器
  + Eagle Active Path 盘中发现（只观察）

L1.5 结构门
  TrendHunter 检查趋势排列、量能结构和形态质量

L2 证据准备
  确定性事实与风险字段拥有权威
  本地模型只可检查闭集的缺失证据 ID

L3 论点审计
  确定性论点、评分和失效条件卡拥有权威
  本地模型只可从预生成失效条件 ID 中选择

L4 云端法庭
  Bull / Bear / Judge / Notary 受证据约束地复核
  必需供应商故障时暂停审计，不允许静默降级

买入前风控门
  新闻证据、账户资格、市场环境和计划有效性
  风险可以阻止影子买入，利好不额外抬高评分

Shadow 全生命周期
  T+1 模拟成交、持仓监控、交易原型、卖出与后验复核

RAG 策略记忆
  事实优先、确定性模板、可选增强、来源校验、隔离和结果关联
```

## 权限边界

| 组件 | 当前权限 |
| --- | --- |
| L1 / L1.5 | 生产候选入口与结构资格判断 |
| L2 确定性层 | 权威证据和风险字段 |
| L2 本地模型 | 无裁决权的闭集观察器，默认关闭 |
| L3 确定性层 | 权威论点与失效条件卡 |
| L3 本地模型 | 无裁决权的失效条件 ID 选择器 |
| L4 云端法庭 | 最终审计裁决 |
| 新闻买入门 | L4 后的风险拦截，不改写历史 L4 结论 |
| Eagle Active Path | 只观察的盘中候选发现 |
| Compass / ima | 离线 dry-run 研究输入 |
| US Radar | 独立海外证据旁路 |
| Shadow | 仅模拟交易 |
| RAG / L5 研究 | 证据记忆与后验研究，不直接取得自动裁决权 |

## Fail-Closed 原则

- 任一必需 L4 供应商缺失或不可用，当前审计请求暂停。
- PushPlus 发送简洁故障通知。
- 系统按可配置周期重试同一请求，默认每 30 分钟一次。
- 供应商恢复并返回有效响应后，原审计才继续。
- 本地观察模型不得覆盖 L2/L3 确定性权威字段。
- 新闻检查不可用时，买入前安全门可以阻止新的影子买入。
- 被隔离的 RAG 记录不能通过润色或向量更新重新进入检索。

## 研究旁路

### Eagle Active Path

Eagle 使用新鲜盘中报价窗口和真实计数器增量，寻找不一定进入单日涨幅榜、但信息密度较高的活跃候选。在前向证据充分之前，它与 L1、L4、Shadow 和交易权限保持隔离。

### Compass / ima

Compass 报告首先被规范化为可追溯的 dry-run 产物。主题不会自动映射股票；ticker 只允许精确匹配或人工确认，非支持市场保持 watch-only。

### US Radar

US Radar 使用独立存储和报告链路研究海外事件与供应链证据，不写烛龙主库，也不生成交易任务。

## 目录结构

| 路径 | 作用 |
| --- | --- |
| `01_engine/` | 数据网关、Tushare 桥接、RAG 刷新、备份和数据契约 |
| `02_brain/` | L1-L4 主决策引擎、TrendHunter、RPS 和证据守卫 |
| `03_tactics/` | Eagle、Owl 与盘中观察桥 |
| `04_governance/` | 治理、推送、评分和风险工具 |
| `05_shadow/` | T+1 模拟成交、持仓生命周期和后验记忆 |
| `06_watchtower/` | 运维 API 与 Web 终端 |
| `07_macro/` | 宏观和市场状态上下文 |
| `08_spark_etf/` | 与主股票审计隔离的 ETF 持仓纪律工具 |
| `10_us_radar/` | 独立美股事件研究旁路 |
| `config/` | 公开配置样例；真实 `.env` 不入库 |
| `docs/` | 可公开的架构与协议文档 |
| `tests/` | 确定性契约、失败路径与回归测试 |
| `tools/`、`scripts/`、`utils/` | dry-run、诊断与维护工具 |

## 快速开始

环境要求：

- Linux 与 Python 3.11
- 可供 DuckDB 使用的本地存储
- 用于行情采集的 Tushare Token
- 可选 Ollama 本地模型服务
- L4 所需的云模型凭据

```bash
git clone https://github.com/ghostylishgy/zhulong-quant.git
cd zhulong-quant

python3 -m venv .venv
. .venv/bin/activate
pip install -r config/requirements.txt

cp config/.env.example config/.env
ln -s config/.env .env
chmod 600 config/.env
# 只在本地填写私有参数，不要提交。
```

公开仓库不包含生产数据库。启动 daemon 前，需要通过你自己的合法数据链路恢复或构建所需 schema 与数据。

运行回归测试：

```bash
export PYTHONPATH="$PWD/03_tactics:$PWD"
python3 -m unittest discover -s tests -p 'test_*.py' -q
```

编译关键模块：

```bash
python3 -m py_compile   zhulong_daemon.py   01_engine/lib/db_gateway.py   02_brain/decision_engine.py
```

## 部署安全

- `.env`、数据库、Chroma、报告、日志、密钥、备份仓库和真实持仓文件必须留在 Git 之外。
- 公开代码中的地址均为 RFC 5737 文档示例，部署时必须通过私有配置替换。
- Watchtower 使用 `X-Watchtower-Key` 保护研究数据接口，并采用显式 CORS 白名单；公网部署仍应在反向代理层增加第二道身份验证。
- Shadow 必须与券商 API 保持断开。
- 所有网页证据在人工批准前都按不可信内容处理。
- 部署任何 API 前先阅读 [SECURITY.md](SECURITY.md)。

## 公开快照策略

公开分支刻意排除了私有工程流水账、生产事故证据、历史提示词快照、机器名、内网地址、代理细节、恢复凭据、运行数据、真实持仓、账户信息和个人观察池。

完整私有历史另行保存在加密备份中。

## 权利声明

Copyright (c) 2026 ghostylishgy. All rights reserved.

本仓库仅以 source-available 方式供查看和讨论，不授予开源许可。详见 [LICENSE](LICENSE)。

## 免责声明

历史观察、回测、影子交易、模型输出和候选报告均不能预测未来收益。本仓库中的任何内容都不构成证券买卖建议。
