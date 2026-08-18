# 美股雷达测量口径修复记录（2026-07-26）

## 一、修复目的

本轮修复针对旁路审计发现的测量可信度问题，保持 `10_us_radar` 独立运行，不修改烛龙主进程、主 APScheduler 或主 DuckDB，不启用 broker 或交易路径。

## 二、代码提交

1. `6b7ad002`：修复 A 股交易时段锚定、长假基期回看，并对未复权公司行动失败关闭。
2. `7e68efb2`：按 SEC Atom 实际 `category` 精确过滤表单，接受 `4/A`、`8-K/A`，拒绝 `424B*`。
3. `968c4987`：扩展瞬态行情状态重试，并支持幂等补齐缺失的 T+1/T+3/T+5/T+20 窗口。
4. `d3a6a46c`：默认强制读取 `zhulong_api_readonly.duckdb`；缺少快照时不回退活库。
5. `8d65c393`：修复质量窗口中文乱码，并让 SEC 全断流和脚本 stderr 可被退出码及独立日志观察。

## 三、修复前归档

- 备份文件：`10_us_radar/data/backups/us_radar_pre_measurement_repair_20260726T224318+0800.sqlite3`
- SHA-256：`be0ba6d45aeac49e071e5b10a5403cce2bfa2833fe620555953a16b807d2e2e2`
- 备份完整性：`PRAGMA integrity_check = ok`
- 同目录 JSON manifest 记录备份路径、哈希及清理前后计数。

## 四、存量清理

- SEC 原始事件由 314 条变为 311 条。
- 删除 3 条被旧前缀匹配误标为 Form 4 的 `424B2/424B3/424B5`。
- 删除前确认这 3 条事件没有 `event_quality`、Form 4 交易、传导目标或收益验证派生记录。
- 回填 41 条乱码 `transmission_window`；修复后乱码计数为 0。
- 修复后旁路 SQLite 完整性：`ok`。

## 五、CN 样本重算

在 `run_once` 与 `validate_once` 双锁保护下，使用只读快照重算全部 592 条 CN 验证行：

| 状态 | 行数 | 处理口径 |
|---|---:|---|
| `DATA_OK` | 446 | 可进入有效样本统计 |
| `INSUFFICIENT_FORWARD_DATA` | 129 | 等待窗口成熟，不计入有效率 |
| `CORPORATE_ACTION_UNADJUSTED` | 17 | 等待复权能力，不计入有效率 |

- 592 条 CN 行全部更新为 `price_v3_cn_session_corp_action_guard_20260726`。
- 与归档版本相比：28 行基期日期变化，21 行 horizon 日期变化，38 行 `primary_excess` 变化，17 行质量状态变化。
- `is_effective_sample=1` 但质量非 `DATA_OK` 或主超额为空的记录数为 0。
- 重算命令返回 `errors=0`，并刷新 452 条事件链汇总。

## 六、边界复核

- A 股行情实际选择：`storage/database/zhulong_api_readonly.duckdb`。
- 烛龙 daemon 修复前后均为 PID `191`，未重启。
- 本轮写入仅发生在 `10_us_radar/data/us_radar.sqlite3`、其备份和旁路日志；未向烛龙主 DuckDB 写入。
- 真实交易、broker、主 L1/L4 与 Shadow 路径均未启用。

## 七、后续观察

- 17 条 `CORPORATE_ACTION_UNADJUSTED` 是有意保留的待验证状态。在接入可靠复权因子前，不应恢复为有效样本。
- 129 条 `INSUFFICIENT_FORWARD_DATA` 由日常 cron 按交易日自然补齐。
- 本轮修复后重新开始解释正式观察样本；修复前结论仅作历史参考，不与新口径直接混算。
