# 社区肝病队列治理后端

覆盖多省份的社区肝脏健康项目随访阶段，基层机构对无创检查、糖尿病状态、饮酒
暴露与转诊结局的更新节奏不一，参与者会跨区复查，也可能撤回研究使用许可。本服务
在事件溯源（event sourcing）基础上提供队列治理：所有事实只可追加，修订以新版本
事件表达，任何统计都可回答“这是哪次访视、哪版规则、哪次上传的结果”。

只依赖 **Python 3.11+ 标准库**（含 `sqlite3`），无需其他服务。

## 它如何回应治理要求

| 要求 | 实现 |
| --- | --- |
| 本地身份映射，不按身份证去重 | `src/identity.py`：研究标识由跨区联动胡椒 HMAC 派生，跨省份同一人一致；站点别名仅本站可识别；证件号不落事件、日志与库 |
| 知情范围与各类事实分别留痕 | 招募/知情、基线访视、设备与校准、问卷版本、无创结果、风险分层、转诊与结局、随访失访、撤回各有独立聚合与版本 |
| 一次访视全部组成部分通过质量规则后才能冻结 | `freeze_batch` 逐条列出去/留原因（`src/quality.py`），含设备校准有效期（v1 为 180 天）、有效测量次数、IQR/中位数、问卷版本等 |
| 迟到更正影响未发布统计，已发布快照锁口径 | `SECTION_CORRECTED` 新版本事件；未发布快照复算读最新版本；已发布快照返回发布事件封存的分母/分层/口径，已发布访视拒绝原地更正 |
| 同一凭证内容一致安全重传，冲突整批隔离 | 原始记录内容指纹先行判定：`duplicate` / 内容冲突仅追加 `BATCH_QUARANTINED` 标记，批次事件不写库，其他地区不受影响 |
| 撤回停止未来分析并按许可处理既有聚合 | `WITHDRAWAL_APPLIED` 携带 `retain_aggregates`；未发布复算按许可剔除/保留；已发布快照免疫；新批次门禁拒绝撤回者 |
| 临床转诊与研究数据分权 | 四角色（coordinator/researcher/clinician/site_user）；临床角色只见基本信息与 `/referrals`，研究角色禁入转诊视图，站点只见自身参与者 |
| 中央复算命令 | `recompute(snapshot_id)` 输出分层（low/indeterminate/high/unclassified）、糖尿病与当前饮酒患病率、缺失与排除原因（含阶段） |
| 结果溯源 API | `GET /visits/{id}/trace`：组成部分版本、事件 ID、上传凭证、规则版本、冻结/发布快照 |
| 并发锁库与恢复 | `BEGIN IMMEDIATE` 写锁，抢锁得 `LibraryLocked`（HTTP 503 Retry-After）；WAL + 同事务登记，重启重放事件流即可恢复 |

## 目录

- `contracts/domain.schema.json`：领域对象与事件名称约定（21 种事件、13 类聚合）。
- `src/identity.py`：本地身份映射（研究标识/站点别名/证件号校验）。
- `src/catalog.py`：版本化规则与问卷目录（内置 v1，可事件发布新版本）。
- `src/quality.py`：各组成部分质量规则、校准有效期判定、FIB-4 与风险分层。
- `src/events.py`：事件类型与构造、稳定序列化。
- `src/envelope.py`：事件信封字段校验。
- `src/repository.py`：SQLite 事件库、上传幂等/冲突隔离、写锁与令牌。
- `src/governance.py`：投影折叠与治理动作（上传、门禁、冻结、发布、撤回、分权视图）。
- `src/analysis.py`：中央复算与口径封存。
- `src/api.py`：HTTP API（`http.server`，Bearer 令牌）。
- `src/cli.py`：命令入口。
- `data/`：示例事件与可上传的站点批次样例。
- `tests/`：46 个测试，含六个规定的自动化场景。

## 本地运行

```bash
# 校验一条领域事件
python3 -m src.cli validate data/sample.json

# 站点上传（5 条记录展开为 10 条领域事件：访视内含 5 个组成部分）
python3 -m src.cli ingest --db cohort.db --site site-3701 --upload up-001 data/sample_upload.json

# 冻结分析批次（不达标会逐条返回排除原因）
python3 -m src.cli freeze --db cohort.db --snapshot S-2026Q3 --visits VISIT-3701-000001

# 中央复算：分层统计 + 缺失与排除原因
python3 -m src.cli recompute --db cohort.db --snapshot S-2026Q3

# 发布患病率快照（分母与口径此后不可变）
python3 -m src.cli publish --db cohort.db --snapshot S-2026Q3

# 令牌、自检、起服务
python3 -m src.cli token --db cohort.db --role coordinator
python3 -m src.cli verify --db cohort.db
python3 -m src.cli serve --db cohort.db --port 8080
```

## HTTP API 摘要

| 方法 | 路径 | 角色 |
| --- | --- | --- |
| POST | `/uploads/{site}/{upload_id}` | coordinator / 本站 site_user |
| GET  | `/uploads?site=` | coordinator / 本站 site_user |
| GET  | `/participants/{study_id}` | coordinator / researcher / clinician（临床仅基本信息）/ 相关站点 |
| GET  | `/visits/{visit_id}` | 研究角色与相关站点（clinician 拒绝） |
| GET  | `/visits/{visit_id}/trace` | coordinator / researcher / 相关站点 |
| GET  | `/referrals/{referral_id}` | coordinator / clinician / 本站 site_user |
| GET  | `/snapshots/{id}/report` | coordinator / researcher |
| POST | `/admin/freeze`、`/admin/publish/{id}` | 仅 coordinator |

状态码：`200` 成功/`duplicate`；`409` 批次隔离；`422` 冻结被质量门禁拒绝；
`503 + Retry-After` 事件库被锁；`403` 角色或站点越权。

## 自动化场景（tests/）

1. `test_cross_site.py`：跨区复查识别为同一参与者但访视独立、联动留痕、站点隔离。
2. `test_quality_freeze.py`：校准过期/设备停用阻断冻结；迟到更正改变未发布复算；发布后口径锁定。
3. `test_concurrency.py`：外部持写锁 → `LibraryLocked`；并发冻结一胜一拒；锁释放后恢复。
4. `test_batch_failure.py`：幂等重传、内容冲突整批隔离、单条非法整批不落库、跨省不连带。
5. `test_withdrawal.py`：撤回按许可改变未发布分母、已发布免疫、新批次禁入、临床/研究分权。
6. `test_recovery.py` 与 `test_api.py`：重启重放、未提交不留痕、隔离标记存活；API 分权、溯源、409/503 与重试成功。

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

## 安全模型说明

- 跨区联动胡椒（linkage pepper）与站点密钥通过带外渠道发放，站点在本地调用
  `SiteIdentityMapper.enroll()` 后只上传研究标识；服务端永远不接收证件号。
- 令牌以 SHA-256 哈希存储；生产部署应置于 HTTPS 反向代理之后并限制
  `serve` 监听地址。
- 事件库文件与 WAL 应保存在受控目录；`verify` 在启动/恢复时检查事件唯一性与
  聚合版本一致性。

## 领域约定

事件标识一旦接收不得原地复用为另一份内容；同一聚合版本单调递增；时间采用带时区
的 ISO 8601 格式。业务修订通过新事件表达，原始记录继续用于追溯。
