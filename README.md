# 水库防汛调度与操作确认

根据库位、入库流量、下游警戒和施工限制生成复核授权的泄洪指令。

## 依据版本（观测、指令、记录、审计共用）

- 每次水情观测生成一个单调递增的依据版本（`observations.version`），记录库位、入库流量。
- 指令创建时绑定当前依据版本（`basis_obs_version`）；操作记录同样绑定依据版本。
- 新增观测后，所有未执行指令（`draft`/`checked`/`authorized`）的旧复核失效、退回待复核（`draft`），依据版本更新为新版本；已执行/已关闭指令不回退。
- 总工授权时在审计中记下当前依据版本与全部未关闭记录（`open_records`）。
- 历史数据中缺依据的未执行指令，启动时升级为待补核（`supplement`）；补齐依据并复核后转入 `checked`。

## 幂等与并发

- 写请求可携带 `request_no`（请求号）。同一 `request_no` 的并发或重试只生效一次，返回首次结果，审计不重复写入。
- 不同 `request_no` 但同一 `external_ref` 的指令只认先到的一次，后者返回冲突。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制、依据版本、幂等键和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败和依据版本测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8315
```

默认端口为`8315`，首次启动自动建库并初始化默认水情观测。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`（可带`request_no`）
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`（可带`request_no`）
- `POST /api/items/{id}/transition`，必须提交`expected_version`（可带`request_no`）
- `POST /api/items/{id}/supplement`：待补核指令补齐依据并复核
- `GET /api/observations`
- `POST /api/observations`：记录新的水情观测版本
- `GET /api/audit`

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
