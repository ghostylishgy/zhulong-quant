# BL-021 审计契约指纹 v0.1

## 1. 目标

为后续统一后验评估建立一个可复现、无密钥的审计契约身份。它回答的是：

> 本次候选从 L1 到 L4 所使用的代码和关键运行开关，属于哪个确定版本？

现有 `l3_logic_hash` 是单条审计内容的哈希。它随标的和推理文本变化，不能代表代码、提示词或模型开关版本，因此不能作为跨批次比较的版本键。

## 2. 当前阶段

本阶段只完成独立 manifest 生成器：

- 读取固定白名单中的审计代码文件；
- 读取固定白名单中的非敏感运行开关；
- 生成逐文件 SHA-256 和聚合 `contract_sha256`；
- 支持对 manifest 做离线完整性校验；
- 默认输出到 stdout，只有显式 `--output` 才写 JSON 文件。

当前 manifest 明确标记：

```text
run_identity_bound=false
binding_status=MANIFEST_ONLY_NOT_YET_BOUND_TO_AUDIT_RUN
```

因此它还不是某次生产审计的完整证据链，也不能用于宣称历史批次具有相同审计契约。

## 3. 指纹范围

指纹覆盖可能改变候选准入、证据解释、提示词、模型解析或 L4 裁决的文件，包括：

- `decision_engine.py` 与 ZPE 优化逻辑；
- TrendHunter、RAG、新闻、Zeta、潮汐相关逻辑；
- 模型网关、模型解析与治理配置。

运行环境只采集固定白名单中的审计策略、候选数量、阶段阈值、模型开关、新闻策略和超时控制。未设置的值统一记录为 `<UNSET>`，默认值仍由代码文件哈希覆盖。

## 4. 安全边界

以下内容不进入 manifest：

- API key、token、secret、password、cookie；
- PushPlus、数据库和外部服务凭据；
- endpoint 或连接字符串；
- `run_id`、交易日和证据截止时间等单次运行身份。

工具不读取或写入 DuckDB，不调用 `decision_engine`、`Nexus.run()`、Shadow 或 RAG，不触发 daemon，也不改变任何交易判断。

## 5. 使用方式

只输出到终端：

```bash
python3 tools/build_audit_contract_manifest.py
```

显式生成文件并复核：

```bash
python3 tools/build_audit_contract_manifest.py --output /tmp/audit_contract.json
python3 tools/build_audit_contract_manifest.py --verify /tmp/audit_contract.json
```

绑定一次审计运行：

```bash
python3 tools/build_audit_contract_manifest.py \
  --bind-trade-date 2026-07-30 \
  --bind-run-id a1b2c3d4 \
  --evidence-as-of 2026-07-30T21:00:00+08:00 \
  --output /tmp/audit_contract_bound.json
python3 tools/build_audit_contract_manifest.py --verify /tmp/audit_contract_bound.json
```

绑定层单独生成 `binding_sha256`。代码/配置未变化时 `contract_sha256` 保持一致；交易日、运行编号或证据截止时间变化时，只有运行绑定身份随之变化。

相同代码内容和相同白名单开关必须得到相同 `contract_sha256`；任一受控文件或开关变化都必须改变该值。缺少受控文件时工具失败关闭。

## 6. 后续绑定

下一阶段才允许把经过验证的 `contract_sha256` 绑定到：

1. 审计启动时冻结的 `trade_date + run_id + evidence_as_of`；
2. `nexus_state.json` 的审计完成回执；
3. BL-020 漏斗观察 artifact；
4. 后续只读 L4 反事实计分表。

绑定必须采用同一次运行开始时冻结的值，不能在审计结束后按当前工作区重新计算并冒充历史版本。绑定改动应独立提交，并在生产审计结束后部署验证。

## 7. v0.1 不做事项

- 不新增 DuckDB 字段或正式表；
- 不回填历史审计契约；
- 不修改 L1-L4、Shadow、RAG 或推送逻辑；
- 不改变 daemon 调度；
- 不启动 L4 反事实评分。

本阶段的验收标准是：指纹可复现、变更可感知、密钥不泄露、缺失文件失败关闭，并且不影响现有生产链路。
