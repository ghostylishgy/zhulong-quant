# BL-030 / L2-L3 v0.3 语义契约与模型评测边界

状态：测试前基线，未接入生产 Observer、L4、RAG、Shadow 或交易链。

## 核心规则

- L2 输出中性证据卡，不输出相对论点的 `market_role`。
- L2 的缺失证据、数据质量、时间对齐和 signal/data conflict 由确定性程序处理。
- 论点和条件必须原子化，且使用同一 `dimension`、`variable` 和 `as_of`。
- L3 模型只判断条件确认后，原子 claim 命题为真概率的变化：`SUPPORTS`、`CONTRADICTS`、`UNRELATED`。
- L3 模型不可看到 `claim_role`、`claim_state`、`candidate_direction`、`dimension`、`variable`、`quality` 或 `as_of`。
- 状态机只生成 claim 级材料包，不直接宣布 thesis 失效或重开。
- 自由文本 `summary`、`confidence`、`questions_for_l3` 不进入模型权威输出。
- 结构化已注册条件优先由程序查表；模型只评估非结构化或注册表外语义。

## 字段边界

L2 deterministic:

`evidence_id`, `dimension`, `normalized_fact`, `quality`, `as_of`, `missing_evidence_ids`, `signal_divergences`, `data_conflicts`

L3 model input:

`claim_text`, `hypothesis`, `candidate_evidence_ids`

L3 model output:

`relation`, `bound_evidence_ids`

Program-only:

`claim_role`, `claim_state`, `candidate_side`, reducer matrix, lifecycle hint

## 评测原则

- 不以模型通过率为目标，不放松事实纪律。
- 关系测试使用类别平衡样例、逻辑镜像、同义改写、字段顺序扰动和同输入多次运行。
- 关系测试与状态机矩阵测试分开。
- 跨变量因果关系不交给 L3。
- `STALE/MISSING` 是时效和完备性状态，不是 `CONTRADICTS`。

## 安全边界

本阶段只生成离线 benchmark artifact；不写 DuckDB，不修改 L2/L3 authority，不调用 Nexus，不写 Shadow/RAG，不触发 daemon，不生成交易信号。
