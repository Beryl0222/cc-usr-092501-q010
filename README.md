# 社区肝病队列治理后端

覆盖 13 个省份的社区肝脏健康项目队列治理服务：站点以**本地身份映射**生成研究标识，
知情同意、基线访视、设备与校准版本、问卷版本、无创结果、风险分层、临床转诊与
随访失访**分别留痕**；只有各组成部分通过版本化质量规则的访视才能进入**冻结分析批次**；
中央从**冻结快照**复算分层统计并输出缺失与排除原因。

只使用 Python 3.11+ 标准库（SQLite 事件存储 + `http.server` API）。

## 设计要点

| 需求 | 实现 |
| --- | --- |
| 身份证不离开站点、避免误合并 | `src/identity.py`：站点盐 HMAC 化名（跨站点不可逆对应）；跨区复查凭**一次性关联码**显式归并 |
| 各组成部分分别留痕、可修订 | 事件溯源（`src/store.py`/`src/projection.py`）：问卷/无创/风险/随访/转诊是访视聚合上的独立事件；迟到更正发新版本，历史事件保留 |
| 质量闸门 | `src/quality.py`（quality 规则版本化）：知情范围、问卷版本、校准窗口（保留校准历史，不追溯否定旧访视）、有效针次/IQR、数值区间、风险复算一致性 |
| 风险分层版本化 | `src/risk.py`：risk-v1（FIB-4 + 糖尿病/重度饮酒上调），按访视记录的规则版本复算 |
| 冻结与不可变发布 | `src/snapshots.py` + `src/analytics.py`：冻结行随事件固化；发布后快照分母/口径不变；迟到更正只影响之后新冻结的统计 |
| 安全重传 / 冲突隔离 | `src/uploads.py`：一凭证一载荷；同凭证同内容幂等回放，内容不同**整批隔离**（只落 `BATCH_QUARANTINED`），其他地区批次不受影响 |
| 并发锁库 | SQLite `BEGIN IMMEDIATE` + 冻结锁 TTL；锁库期间上传返回可重试（不隔离），第二冻结收到 409；崩溃后过期锁可接管 |
| 撤回传播 | `aggregates=remove` 排除既有聚合；`retain` 保留撤回前聚合、撤回后访视一律排除；未来分析停止 |
| 分权与站点隔离 | `src/access.py`：`research:read` 与 `referral:read` 两条通道；站点协调员只见本站点参与者；身份映射需 `identity:map` |
| 结果溯源 | `GET /visits/<id>/trace`：某结果来自哪次访视、哪批上传、哪版风险/质量规则、出现在哪些（含已发布）快照 |
| 服务恢复 | 投影可随时从事件流整体重建；冻结锁靠 TTL 恢复 |

## 目录

- `contracts/domain.schema.json`：领域事件与聚合类型契约。
- `src/contracts.py`：事件、聚合、角色、范围、原因码常量。
- `src/envelope.py`：事件信封校验（必填、正整数版本、带时区 ISO 8601、枚举）。
- `src/identity.py`：站点盐化名、一次性关联码、上传凭证。
- `src/store.py`：SQLite 事件表（只追加）、批次台账、凭证首用、冻结锁、用户、关联码。
- `src/projection.py`：事件流重放的只读模型（参与者化名归并、访视组件最新版、校准历史、规则版本、快照）。
- `src/uploads.py`：命令批次 → 事件翻译、引用校验、幂等与整批隔离。
- `src/quality.py` / `src/risk.py`：版本化质量规则与风险分层引擎。
- `src/snapshots.py` / `src/analytics.py`：冻结行构建、基线去重、撤回传播、复算与发布。
- `src/access.py` / `src/service.py`：分权访问与服务门面。
- `src/api.py` / `src/cli.py`：HTTP API 与命令入口。
- `tests/`：单元测试、HTTP 集成测试、六个规定场景测试、测试世界夹具。
- `examples/demo_13_provinces.py`：13 省端到端演示。

## 本地运行

校验单个事件文件（旧用法保留）：

```bash
python3 -m src.cli data/sample.json
```

启动 HTTP 服务：

```bash
python3 -m src.cli serve --db data/cohort.db --port 8080
```

13 省端到端演示（引导 13 站点 → 上传 → 跨区复查 → 校准失效 → 隔离 →
锁库 → 撤回 → 迟到更正 → 两次冻结发布 → 溯源 → 分权 → 重开恢复）：

```bash
python3 examples/demo_13_provinces.py
```

运行测试：

```bash
python3 -m unittest discover -s tests
```

编译检查：

```bash
python3 -m compileall -q src tests examples
```

## HTTP API 摘要

所有请求带 `Authorization: Bearer <user_key>`，上传请求体额外含 `token`（上传凭证）。

| 方法与路径 | 角色 | 说明 |
| --- | --- | --- |
| `POST /admin/sites` `/admin/users` `/admin/tokens` | admin | 站点（生成站点盐）、用户、上传凭证 |
| `POST /uploads` | 站点协调员 | 上传命令批次；200 接收 / 200 幂等重放 / 422 整批隔离 / 202 锁库可重试 |
| `POST /linkages/code` `/linkages/consume` | 站点协调员 | 跨区复查一次性关联码的签发与消费 |
| `GET /participants` `/participants/<pid>` | research | 参与者研究信息（站点范围受限） |
| `GET /visits/<vid>/trace` | research | 结果溯源（访视/上传批次/规则版本/质量结论/快照） |
| `GET /referrals` | clinician | 临床转诊通道，与研究数据分离 |
| `POST /freezes` | central-analyst | 冻结分析批次（并发锁库返回 409） |
| `POST /snapshots/<id>/recompute` | research | 从冻结快照复算（预览，不发布） |
| `POST /snapshots/<id>/publish` | central-analyst | 发布患病率快照（不可变） |
| `GET /snapshots` `/snapshots/<id>/stats` | research | 快照清单与已发布统计 |

## 批次命令

上传体：`{"token": "...", "upload_id": "...", "commands": [ ... ]}`。命令类型：
`enroll`（可给本地 `id_card` 由站点盐即时化名，身份证号不入事件）、`consent`、
`withdrawal`、`device_register`、`calibration`、`questionnaire_publish`、
`rule_publish`、`visit`（内嵌 `questionnaire`/`nitx`/`risk`/`followup`/`referral`）、
以及迟到更正 `questionnaire_update`、`nitx_update`、`risk_update`、
`followup_update`、`referral_update`（同一聚合新版本）。

质量/排除原因码见 `src/contracts.py` 的 `Reasons`（如 `calibration_expired`、
`nitx_shots_insufficient`、`risk_level_mismatch`、`participant_withdrawn`、
`baseline_already_counted`），中央复算按这些口径输出缺失与排除计数。
