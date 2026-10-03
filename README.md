# 水库防汛调度与操作确认

根据库位、入库流量、下游警戒和施工限制生成复核授权的泄洪指令。观测、指令、操作记录与审计共用同一**依据版本（basis_version）**。

## 依据版本与复核失效

- 每次提交水情观测生成一个全局递增的 `basis_version`；新建指令、操作记录默认绑定当前最新依据版本，审计事件同样记录该版本。
- 观测更新（库位或入库流量变化）后，所有依据版本更旧且**尚未执行**的指令（`checked`/`authorized`）其旧复核自动失效，状态退回 `recheck_pending`（待复核），必须由值班员重新复核、总工重新授权后方可执行。
- 重新复核时绑定最新观测；若复核/授权时依据版本已过期，返回 409，不能凭旧读数放行。
- 总工授权时对**当时观测读数**和**全部未关闭记录**做快照（`auth_snapshot`），写入指令与审计。
- 已执行（`executed`）/已关闭（`closed`）指令不受后续观测更新影响。
- 历史数据中缺少依据版本的指令在启动迁移时升级为 `supplement_pending`（待补核），常规流转被禁止；补绑观测依据（补核）后回到迁移前状态。

## 幂等与并发

- 写请求可携带 `request_id`（请求体字段或 `X-Request-Id` 头）。同一请求号只生效一次：两名值班员同时提交同一指令时只认先到的一次。
- 写入失败后可用**原请求号**重试，接口回放首次结果，业务数据和审计都不会重复；业务写入、审计写入、幂等登记在同一事务内原子提交。
- 操作记录仍可用 `external_ref` 做内容级去重。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限、关闭不变量与依据版本规则。
- `src/repository.py`：SQLite建表、事务、版本控制、幂等键、历史迁移和审计链。
- `src/service.py`：权限检查、用例编排、并发控制、失效退回和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败场景及依据版本/并发/迁移测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8315
```

默认端口为`8315`，首次启动自动建库；检测到旧版库结构时自动迁移（缺依据版本的历史指令进入待补核）。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `POST /api/observations`：提交水情观测（库位、入库流量、下游警戒），返回新依据版本与被失效指令
- `GET /api/observations`：观测列表
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `POST /api/items/{id}/recheck`：待复核指令基于最新观测重新复核（提交`expected_version`）
- `POST /api/items/{id}/supplement-basis`：待补核历史指令补绑`basis_version`
- `GET /api/audit`

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
