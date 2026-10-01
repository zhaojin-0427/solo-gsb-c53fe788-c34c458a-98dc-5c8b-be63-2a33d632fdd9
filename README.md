# 多人应急演练 Web 应用

主持人用 **YAML** 定义演练的阶段、各角色可见信息、限时选项与条件分支；参与者凭
**角色邀请码** 加入，只能看到本角色的内容并在限时内提交选择。后端基于
**FastAPI + SQLite + WebSocket**，前端为无构建步骤的原生 JavaScript。

- **剧本版本与发布管理**：上传/编辑 YAML 时生成不可变版本号（v1、v2…）与
  **SHA-256 内容摘要**；内容相同则复用同一版本（按摘要幂等去重）。草稿只有
  **发布后**才能用于新会话；发布采用基准版本乐观锁，他人抢先发布会被拒绝且
  **不改变当前发布版**。主持台可查看版本列表、按结构（阶段/角色/视图/选项/
  转移规则）对比任意两版、从任一历史版本复制为新草稿。
- **会话永久绑定创建时版本**：已创建或进行中的会话固化其剧本次序号与摘要，
  后续编辑、再发布都不会改变它的快照、分支与回放；API、WebSocket 快照、
  回放与记录导出均带版本标识。
- **联合演练编排（联演）**：主持人可把**多个独立会话**编入同一场联演，为各会话
  的阶段配置**有向依赖**；上游阶段达到**指定结局**后，系统向目标会话的指定阶段
  追加**一次性联演注入**，并按该会话的角色权限推送。**多个前置分支须全部满足
  （AND 屏障）**才释放。保存前校验依赖**无环**且会话、阶段、结局均有效；
  重复或乱序到达**不会重复投递或提前解锁**，主持台实时显示各分支的等待状态与
  阻塞原因；各会话仍保留**各自独立的剧本版本与事件记录**。
- 服务端以 **单调递增版本号** 原子结算每一轮（每次状态变更是一个数据库版本）
- 正确处理 **重复提交**（幂等）、**断线重连**（WebSocket 自动重连 + 快照重同步）和
  **截止时刻的并发竞争**（拒绝与结算在同一个 SQLite 立即事务内完成）
- 每次状态变化向客户端推送 **按权限裁剪的快照**（角色视角 / 主持人全量视角）
- 结束后可依据不可变事件日志 **确定性回放任一版本**，并导出
  **个人记录（不含任何他人隐藏信息）** 与 **主持人全量记录**

---

## 目录结构

```
.
├── backend/
│   ├── app/
│   │   ├── main.py          # FastAPI：HTTP API、WebSocket、定时结算、静态托管
│   │   ├── engine.py        # 纯事件溯源 reducer + 原子提交（版本号/结算/幂等）
│   │   ├── definitions.py   # YAML 剧本解析与校验
│   │   ├── joints.py        # 联合演练编排：DAG 校验、AND 屏障、一次性注入
│   │   ├── versions.py      # 不可变版本摘要（SHA-256）与结构化差异对比
│   │   ├── db.py            # SQLite（WAL、立即事务、事件日志、版本表与迁移）
│   │   ├── snapshots.py     # 按角色裁剪的快照构造
│   │   ├── records.py       # 确定性回放 + 个人/全量记录导出（Markdown/JSON）
│   │   ├── hub.py           # WebSocket 连接管理（在线状态、背压安全）
│   │   └── config.py        # 路径与密钥（环境变量）
│   ├── tests/
│   │   └── e2e_check.py     # 回归测试：版本/发布/绑定/差异、截止竞争、幂等、回放、迁移、端到端
│   └── requirements.txt
├── frontend/                # 原生 HTML/CSS/JS（由后端直接托管）
│   ├── index.html
│   ├── styles.css
│   └── app.js
├── drills/
│   └── chem-plant.yaml      # 示例剧本：化工厂反应釜泄漏与次生火灾
├── Dockerfile
└── docker-compose.yml
```

---

## 快速开始（Docker Compose）

前置要求：安装 Docker 与 Docker Compose 插件。

```bash
# 1. 启动（首次会构建镜像）
docker compose up -d --build

# 2. 查看日志
docker compose logs -f
```

打开浏览器访问：

| 入口 | 地址 |
| --- | --- |
| 应用首页（主持人/参与者入口） | <http://localhost:8000/> |
| 健康检查 | <http://localhost:8000/api/health> |
| OpenAPI 文档 | <http://localhost:8000/docs> |

停止与清理：

```bash
docker compose down          # 停止
docker compose down -v       # 同时删除会话数据库卷（drill-data）
```

### 验证部署

```bash
# 健康检查
curl http://localhost:8000/api/health        # => {"ok":true}

# 冒烟流程：上传草稿 → 发布 → 创建会话 → 加入 → 主持台开始 → 提交 → 版本/回放/导出
curl -H "X-Host-Key: host-1234" -H "Content-Type: application/json" \
     -X POST http://localhost:8000/api/drills \
     -d '{"yaml": "<YAML 文本>", "base_version": null}'
# => {"id":"...","version":1,"status":"draft","sha256":"…","digest_short":"…"}
curl -H "X-Host-Key: host-1234" -H "Content-Type: application/json" \
     -X POST http://localhost:8000/api/drills/<drill_id>/versions/1/publish \
     -d '{"base_version": null}'          # => {"version":1,"status":"published",…}
curl -H "X-Host-Key: host-1234" -X POST "http://localhost:8000/api/sessions?drill_id=<drill_id>"
# => {"session_id":"xxxxxxxx","drill_version":1,"drill_sha256":"…","digest_short":"…"}
curl -X POST "http://localhost:8000/api/sessions/<session_id>/join" \
     -H "Content-Type: application/json" -d '{"invite":"CMD-2026","name":"张三"}'
# => {"role_id":"cmd",...,"token":"...","version":1,"drill_version":1,"drill_sha256":"…"}
# 之后打开 http://localhost:8000/ 用主持人密钥进入主持台开始演练；
# 版本列表/差异/复制：
curl -H "X-Host-Key: host-1234" http://localhost:8000/api/drills/<drill_id>/versions
curl -H "X-Host-Key: host-1234" \
     "http://localhost:8000/api/drills/<drill_id>/diff?from=1&to=2"
# 回放与导出（响应均含 drill_version / drill_sha256）：
curl -H "X-Host-Key: host-1234" "http://localhost:8000/api/sessions/<session_id>/replay"
curl -H "X-Host-Key: host-1234" "http://localhost:8000/api/sessions/<session_id>/record?fmt=md"
```

### 回归测试

测试不依赖 Docker，直接在本机 Python 运行（覆盖：先加入再启动、末名提交
自动结算、截止竞争、**连续两个限时阶段间旧定时器的身份核对**（末名提交 /
主持人手动结算竞争，旧回调空转不产生事件、正常回调只结算一次）、并发幂等、
任意版本前缀回放、手动终止、定时器登记薄、剧本版本/发布/绑定/迁移、
**联合演练编排（保存前无环/会话阶段结局校验、AND 屏障不提前解锁、乱序重复不重复
记账、一次性/迟到补投、按角色权限裁剪、已投递不可改删、HTTP+联演 WebSocket 端到端）**、
HTTP+WebSocket 端到端）：

```bash
cd backend
pip install -r requirements.txt httpx
python tests/e2e_check.py        # 全部通过时退出码为 0
```

### 不使用 Docker（本地开发）

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# 前端与 drills/ 目录通过相对路径自动定位；数据库默认写入仓库根 /data
```

---

## 配置

所有配置通过环境变量提供（见 `docker-compose.yml`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST_KEY` | `host-1234` | 主持人密钥。前端首页输入；HTTP 用请求头 `X-Host-Key`，WebSocket 用查询参数 `host=`。**生产环境必须修改** |
| `DATA_DIR` | `/data` | SQLite 数据库目录（`drills.db`）。Compose 中为持久化卷 |
| `DRILLS_DIR` | `/drills` | YAML 剧本种子目录，启动时自动加载：新文件发布为 v1、变更文件导入为草稿。Compose 中为只读挂载，版本数据以数据库为准 |
| `STATIC_DIR` | `/app/frontend` | 前端静态文件目录 |

端口映射在 `docker-compose.yml` 的 `ports` 中修改（如 `"18000:8000"`）。
时间均以服务器 UTC 时间戳（POSIX 秒）传输，前端按浏览器本地时区显示。

> 部署模型为 **单副本**：写串行锁与 WebSocket 连接表在进程内维护。
> SQLite 本身已是单文件、零外部依赖；需要横向扩展时应改用粘性会话或外移状态。

---

## 使用指南

### 主持人

1. 打开 <http://localhost:8000/>，选择“主持人入口”，输入密钥（默认 `host-1234`）。
2. 剧本来源两种：
   - 预先把 YAML 放到宿主机 `./drills/` 目录（容器内 `/drills`，只读挂载）：
     服务**首次见到**该文件时自动发布为 v1；之后文件相对当前发布版发生变化时，
     会导入为一条**待发布草稿**（不影响线上会话）。
   - 在“主持人控制台”粘贴 YAML 上传/编辑：保存即生成一条不可变**草稿版本**与
     SHA-256 摘要（数据库为准；只读目录写不进去也不影响保存，响应里的
     `yaml_written` 表示是否同时镜像到了 `drills/`）。
3. 点击剧本的 **“版本管理”**：
   - 查看版本列表（`已发布 / 草稿 / 历史版本 / 已废弃草稿`、摘要、基准版本、时间）；
   - 在“结构化差异对比”里选择任意两版，按 **剧本属性 / 角色 / 阶段 /
     各角色视图 / 选项 / 转移规则** 查看差异；
   - 从任意历史版本 **“复制为新草稿”**，或载入发布版/草稿 YAML 继续编辑；
   - 校对无误后 **“发布此草稿”**。发布带乐观锁：若该草稿所基于的版本已被他人
     更新，发布返回 409 且**当前发布版完全不变**，需重新复制/编辑后再发布。
4. **草稿不能创建会话**；只有当前发布版可以。点击“创建会话（vN）”得到一个
   **8 位会话编号**，进入主持台。会话在创建瞬间固化 **剧本次序号 vN + 内容摘要**，
   此后即使剧本再发布新版，本会话的快照、分支、回放与导出都保持创建时版本。
5. 把 **邀请链接/邀请码** 发给对应角色的参与者（主持台可一键复制）。
6. 人员就绪后点击“▶ 开始演练”。主持台可看到：
   - 每个阶段的公开信息、**所有角色的隐藏信息**、选项指向的分支；
   - 各角色是否已提交、在线/离线状态、倒计时；
   - “⏭ 立即结算当前阶段”（未提交角色按超时处理）、“■ 结束整场演练”；
   - 顶部显示“会话单调版本号”与“绑定剧本版本/摘要”。
7. 结束后在主持台导出 **全量记录（Markdown/JSON）** 或打开“确定性回放”，
   两者都标注该会话固化的剧本版本与摘要。

### 主持人：联合演练编排（联演）

1. 先为每个独立演练**分别创建会话**（可以是同一剧本或不同剧本，各自固化自己的
   剧本版本）。在主持人控制台的 **“🔗 联合演练编排”** 卡片点击
   **“编排新的联合演练”**。
2. 填写联演名称，勾选至少 **2 个成员会话**，然后逐条添加规则：
   - **目标会话 / 目标阶段**：注入将追加到该会话的这一阶段；
   - **注入内容**：全员可见的公开文本，以及可选的**按角色推送**密报
     （只有目标会话里对应角色能看到，沿用该会话的角色权限裁剪）；
   - **前置分支**：选择上游会话、上游阶段与其**指定结局**。结局下拉来自该上游
     会话**绑定版本**中该阶段真正可能产生的去向；特殊结局
     **`@end`** 表示该阶段结算后整场演练结束。
3. 一条规则可以有**多个前置分支**，它们构成 **AND 屏障：全部满足**系统才释放一次
   注入。可编排多条规则；规则之间按（会话, 阶段）构成有向图。
4. **保存即校验**：成员会话/阶段/结局有效、推送角色存在、注入内容非空、分支不重复、
   不允许同一会话内的自依赖，且整张依赖图**无环**。任一不满足返回明确的中文原因，
   编排不会落库。
5. 保存后进入 **联演监控台**（主持人专用实时通道）：
   - 每个成员会话的状态、当前阶段、绑定剧本版本；
   - 每条规则各分支是 **✓ 已满足（命中版本/时间）** 还是 **等待**，等待时给出
     **阻塞原因**（未到达、等待结算、结局不符等），以及规则整体状态
     （等待前置 / 前置已满足 / 已投递 / 迟到补投）。
6. 运行期保证：
   - 上游阶段结算到指定结局才记一次“满足”，**乱序/重复结算不会重复计数**，
     分支未齐全**绝不提前解锁**；
   - 屏障齐全且目标阶段已开放时，注入作为目标会话的**一个新事件版本**写入其
     **自己的事件日志**；若目标阶段已经结算，则作为**迟到补投**仍只投递一次并
     标记 `late`，不会重开阶段、不改变流转；若目标阶段始终未到达则继续等待并说明原因；
   - 注入只进入**目标会话**，上游/其他成员会话的事件日志不变；各会话继续保留
     **独立的剧本版本、快照、回放与记录**，注入内容同样按角色权限进入回放与
     Markdown/JSON 导出。
7. **已投递**的联演不能删除，已投递的规则不能再修改依赖或内容（一次性注入已成为
   目标会话不可变事件记录的一部分）；未投递的规则可随时增删改，未改变的规则保留
   稳定标识与其等待状态。服务重启后会自动重新收敛各联演屏障并补投。

### 参与者

1. 打开邀请链接（形如 `http://主机:8000/#/join/<会话编号>?invite=<邀请码>`），
   或在首页手动输入会话编号与邀请码、姓名。
2. 加入后浏览器本地保存访问令牌（用于断线重连、个人回放与导出）。
3. 页面只显示 **公开信息 + 本角色可见信息 + 本角色的选项**；
   在倒计时内点选并确认提交（**提交不可更改**）。
4. 网络中断会自动重连并重新获取快照；若重发了已提交的选择，服务器幂等返回原结果。
5. 演练结束后可导出自己的 **个人记录**，并查看自己视角的版本回放。

### 示例剧本的邀请码（chem-plant）

| 角色 | 邀请码 |
| --- | --- |
| 值班班长 | `CMD-2026` |
| 外操员 | `EVA-2026` |
| 安全员 | `SAF-2026` |

---

## 剧本版本与发布模型（不可变版本 / 草稿 / 发布）

### 版本号与内容摘要

- 每次“保存”都在 `drill_versions` 表新增一行，版本号 **`MAX(version)+1` 单调递增、
  永不修改**；状态为 `draft`（草稿）、`published`（当前发布）、`history`（被替代的
  旧发布版）或 `superseded`（被再次保存顶掉、未发布的旧草稿）。
- 每行保存规范化后的剧本 JSON 与 **SHA-256 摘要**（对解析后的结构取哈希，
  因此注释/缩进/键序不同但语义相同的 YAML 摘要一致）。保存与发布都按摘要去重：
  内容没变就复用既有版本，不会平白产生新版本。
- 一个剧本同时最多只有一条开放草稿；再次保存会把旧草稿置为 `superseded`
  （行仍保留、可追溯）并生成新草稿。

### 草稿 → 发布（乐观并发）

```text
上传/编辑 YAML ──▶ 不可变草稿 vN（base=当前发布版）
                     │ 主持台查看差异、从历史版本复制
                     ▼
        发布：校验 base == 当前发布版？
           ├─ 是：旧发布版转 history，vN 成为 published（单一当前发布版）
           └─ 否（他人已抢先发布）：409，当前发布版不变，草稿保留，重新校对后再发
```

- **草稿只能在发布后用于新会话**：未发布时 `POST /api/sessions` 返回 409。
- 发布是单个 `BEGIN IMMEDIATE` 事务：冲突时整体回滚，失败绝不会改动当前发布版。

### 会话与版本的绑定（后续编辑不得改变快照/分支/回放）

- 创建会话时同时写入 `sessions.drill_version` 与 `sessions.drill_sha256`。
- 会话运行期的定义读取**只按这对标识**从 `drill_versions` 取对应不可变版本，
  从不读“最新发布版”。因此：
  - 创建于 v1 的会话，在剧本发布 v2、v3 后，阶段文本、角色视图、选项、
    `default_next`、条件转移全部仍是 v1；
  - 只有**新建**的会话才会使用当时的最新发布版；
  - 旧数据库升级时迁移程序自动把既有发布版补为不可变 v1，并把存量会话绑定到 v1。
- 版本标识贯穿各出口：
  - **HTTP**：创建会话响应、会话列表、加入响应都带 `drill_version`/`drill_sha256`；
  - **WebSocket 快照**：`snapshot.drill.version` 与 `snapshot.drill.sha256`；
  - **回放**：顶层 `drill_version`/`drill_sha256`，帧帧一致；
  - **记录导出**：JSON 顶层与 `drill` 块、Markdown 抬头“剧本版本：vN（摘要）”。

---

## 联合演练编排模型（多会话 / 有向依赖 / AND 屏障 / 一次性注入）

### 基本对象

- **联演（joint）**：一个命名对象，把 ≥2 个**独立会话**编为成员。会话不依赖联演而
  存在，仍各自固化创建时的剧本版本、各自维护事件日志。
- **规则（joint rule）**：一条“前置 → 注入”的有向依赖：
  - 目标：`target_session_id` + `target_stage_id` + 注入内容
    （`title`、全员可见的 `public`、按角色的 `views` 密报）；
  - 前置：若干 **分支（branch）**，每条为
    `(source_session_id, source_stage_id, outcome)`。
- **结局（outcome）**：上游阶段结算后实际走到的“下一阶段 id”；特殊值
  **`@end`** 表示该阶段结算后整场演练结束（`stage_sealed.next IS NULL`）。
  可声明的结局取自上游会话**绑定版本**中该阶段的选项 `next`、条件转移 `next`、
  `default_next` 的并集（必要时含 `@end`）——保存时按各会话自己的版本逐一校验，
  不可能产生的结局直接拒绝。

### 保存前校验（全部在服务端，失败不落库）

1. 联演名非空；成员 ≥2 且会话都存在；至少一条规则；
2. 目标会话在成员中、目标阶段存在于其**绑定版本**；推送角色存在且密报非空；
   公开文本与角色密报至少有一项非空；
3. 每个前置分支：上游会话在成员中、上游阶段存在、声明结局在该阶段的可能结局集合内；
4. 分支之间不允许指向同一个（会话, 阶段）（重复 / 互相矛盾的 AND 分支无意义）；
5. **依赖只能跨会话**：目标与任一分属同一会话的规则被拒绝，会话内流转用剧本自身的
   选项/条件分支表达；
6. 以（会话, 阶段）为节点、分支→目标为有向边做 **DFS 环检测**，发现回边即拒绝并给出
   具体环节点序列。

### 运行时状态机与一致性

```text
上游阶段结算（submit 自动结算 / deadline / 主持人 settle/end）
        │  对包含该会话的每个联演执行一次 reconcile
        ▼
joint_hits(rule_id, idx) 记账？  ── 已存在（重复/乱序）──▶ 忽略，不重复计数
        │ 新命中且 outcome 相符
        ▼
规则的全部分支都有 hit？ ── 否 ──▶ 继续等待（监控台给出每个分支的阻塞原因）
        │ 是（AND 屏障释放，绝不提前）
        ▼
目标阶段状态？
  ├─ 已开放/刚开放：在目标会话事件日志追加一个新版本 joint_injection（late=0）
  ├─ 已结算：仍一次性追加，late=1（迟到补投，不重开阶段、不改流转）
  └─ 从未到达：暂不投递，规则保持就绪/等待，待其到达时由收敛流程投递
```

- **两道幂等防线**：
  - `joint_hits` 主键 `(rule_id, idx)`：同一分支的重复、乱序结算信号最多记一次；
  - `joint_deliveries` 主键 `rule_id`：每条规则**恰好投递一次**。该台账行与
    `joint_injection` 事件在目标会话的**同一个 `BEGIN IMMEDIATE` 事务**内提交，
    并发的重复触发靠主键冲突空转，绝不产生第二条注入。
- **锁序**：联演锁 → 会话锁（全局一致，避免死锁）；会话内的选择/结算仍由原有会话锁与
  立即事务串行化。收敛是“读账本 + 逐分支独立事务记账 + 屏障齐全后调用目标会话投递”，
  不在持有事务时跨会话调用引擎。
- **事件与版本独立**：注入是**目标会话** `events` 中的一个新版本（`seq=0`，
  类型 `joint_injection`），继续满足“一次原子提交 = `last_version+1`、同版本 seq
  从 0 连续”的既有不变量；上游与其他成员会话的日志不出现该事件。因此各会话的
  快照、确定性回放、个人/全量记录依旧彼此独立，注入内容在这些出口同样按角色权限裁剪。
- **重启恢复**：启动时对所有联演执行一次收敛——停机期间结算的分支补上幂等命中，
  已满足但未投递的规则在目标阶段可达时补投，台账保证不会重复。
- **可变性边界**：已投递规则的依赖与内容不可修改、已投递联演不可删除（事件已进入
  不可变日志）；未投递规则可自由增删改。更新编排时，未变化的规则保留其稳定 id 与
  既有命中/投递账本，被改动的规则获得新 id（作为一条需重新满足的新屏障），
  被删除且未投递的规则清掉其等待账本。

### 监控台状态（`GET /api/joints/{jid}` 与联演 WebSocket）

- 规则级：`waiting`（仍有分支未满足，附剩余数量/目标阶段是否到达）、
  `ready`（前置全满足、待目标阶段到达）、`delivering`（目标已结算，将迟到补投）、
  `delivered`（已投递，附事件版本、时间、是否迟到）；
- 分支级：`matched`（命中版本与时间）或 `waiting`，等待时 `block_reason` 说明
  “等待上游会话到达该阶段 / 等待上游阶段结算到指定结局 / 已结算但结局为 X 未命中
  要求结局 Y”等具体原因。

---

## YAML 剧本格式

最小示例：

```yaml
id: demo                 # 必须与文件名一致（demo.yaml）
name: 示例演练
description: 一句话说明

roles:
  - id: cmd
    name: 值班班长
    invite: CMD-2026     # 每个角色唯一的加入邀请码

stages:
  - id: s1
    title: 第一阶段
    public: 所有角色都能看到的公开信息
    announcement: 阶段公告（默认同 public）
    duration: 45         # 限时秒数；null 表示只能由主持人手动结算
    default_next: s2     # 无匹配分支时的去向；省略或 null 表示演练结束

    views:               # 仅对应角色可见的隐藏信息
      cmd: 只有班长知道的情况

    options:             # 需要做决定的角色与其限时选项
      cmd:
        - id: evacuate
          text: 组织撤离
        - id: hold
          text: 原地等待
          next: s3       # 单决策角色可直接用选项指定去向（优先级最高）

    transitions:         # 多角色条件分支：按顺序匹配，首个全部相等的生效
      - when: {cmd: evacuate}
        next: s2
      - when: {cmd: hold}
        next: s3
```

### 结算与分支规则

1. 每个阶段的“决策角色”是 `options` 下出现的角色；其余角色该阶段只需等待。
2. 满足任一条件即结算该阶段：
   - **全部决策角色都已提交** → 自动结算（`complete`）；
   - **到达 `duration` 截止时刻** → 定时器结算，未提交者选择记为 `null`（超时，`deadline`）；
   - **主持人手动结算** → 未提交者同样记为超时（`host`）。
3. 下一阶段按优先级确定：
   1. 已提交选项上的 `next`（适合单决策者直接跳转）；
   2. `transitions` 中首个 `when` 条件全部满足的规则
      （`{role: choice_id}` 全等；超时角色不等于任何选项 id，因此不会命中）；
   3. `default_next`；为 `null`/省略则演练结束。
4. `duration: null` 的阶段永不自动超时，由主持人手动结算（如复盘阶段）。
5. 校验规则：id 唯一、邀请码唯一、所有 `next`/`default_next` 必须指向已存在阶段、
   选项/视图/分支引用的角色必须存在、`duration` 为正整数。

---

## HTTP / WebSocket 接口摘要

### HTTP（`X-Host-Key` 为主持人密钥；参与者接口用 `?token=` 或加入返回的令牌）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查 |
| GET | `/api/drills` | 剧本列表（含 `published_version`/`draft_version`，主持人） |
| POST | `/api/drills` | 上传/编辑 YAML，body `{"yaml": "...", "base_version": n|null}`：生成不可变草稿版本+SHA-256（主持人） |
| GET | `/api/drills/{id}/versions` | 版本列表（含每版状态、摘要、基准、时间与 YAML） |
| GET | `/api/drills/{id}/versions/{v}` | 取某一版本（含原始 YAML） |
| POST | `/api/drills/{id}/versions/{v}/publish` | body `{"base_version": n|null}`：发布草稿；基准被他人更新时 409 且不改变当前发布版 |
| POST | `/api/drills/{id}/copy-draft` | body `{"source_version": v, "base_version": n}`：从任一历史版本复制为新草稿 |
| GET | `/api/drills/{id}/diff?from=a&to=b` | 按结构返回两版差异（属性/角色/阶段/视图/选项/转移规则） |
| POST | `/api/sessions?drill_id=...` | 仅可基于当前发布版创建；返回 `session_id` 及绑定的 `drill_version`/`drill_sha256`（主持人） |
| GET | `/api/sessions` | 会话列表（含每会话绑定的剧本次序号与摘要） |
| POST | `/api/sessions/{sid}/join` | body `{"invite": "...", "name": "..."}`，返回角色、令牌与剧本版本标识 |
| GET | `/api/sessions/{sid}/replay?token=&version=` | 逐版本确定性回放（主持人或参与者），顶层带 `drill_version`/`drill_sha256` |
| GET | `/api/sessions/{sid}/record?fmt=md\|json&token=` | 全量记录（主持人）/ 个人记录（参与者），均标注剧本版本 |
| GET | `/api/sessions/{sid}/outline` | 该会话**绑定版本**的阶段、角色与各阶段可选结局（编排联演时用，主持人） |
| GET | `/api/joints` | 联演列表（主持人） |
| POST | `/api/joints` | 创建联演：body `{"name","session_ids":[≥2],"rules":[...]}`；保存前校验会话/阶段/结局有效、分支不重复、跨会话、依赖无环（主持人） |
| GET | `/api/joints/{jid}` | 联演实时监控状态：成员会话、规则与各分支的等待/命中、阻塞原因、投递结果（主持人） |
| PUT | `/api/joints/{jid}` | 更新编排（同创建 body）；已投递规则不可改、已投递联演不可删（主持人） |
| DELETE | `/api/joints/{jid}` | 删除尚无任何投递的联演；已有投递返回 409（主持人） |

### WebSocket

```
ws://<host>:8000/ws/{sessionId}?host=<HOST_KEY>      # 主持人
ws://<host>:8000/ws/{sessionId}?token=<participantToken>  # 参与者
ws://<host>:8000/ws/joint/<jointId>?host=<HOST_KEY>  # 联演监控台（仅主持人）
```

- 服务端推送：`{"type":"snapshot","snapshot":{...}}`（裁剪后的完整快照）。
  快照的 `drill.version` / `drill.sha256` / `drill.digest_short` 即本会话
  创建时固化的剧本版本与摘要，整个生命周期恒定不变。
- **联演监控通道**推送 `{"type":"joint_snapshot","joint":{...}}`：成员会话状态、
  每条规则整体状态（`waiting/ready/delivering/delivered`）、每个前置分支的
  `state`（`waiting/matched`）与 `block_reason`（阻塞原因）、投递版本与是否迟到。
  该通道只读；分支满足或注入投递时由服务端主动推送，断线自动重连后立即收到最新状态。
- 注入投递后，**目标会话**的所有连接者收到的新快照里，对应阶段块会带
  `injections`（公开文本 + 本角色可见密报；其他角色密报不会出现）。
- 参与者发送：`{"type":"submit","choice_id":"...","client_stage":"..."}`，
  服务端回 `submit_result`（`accepted` / `duplicate` 幂等重放 / 截止拒绝并已结算）。
- 主持人发送：`{"type":"start"}`、`{"type":"settle"}`、`{"type":"end"}`。
- 心跳：客户端 `{"type":"ping","t":...}`，服务端回 `{"type":"pong",...}`（同时用于时钟校准）。
- 认证失败返回 `error` 后以 4401 关闭；队列积压时服务端丢弃旧消息并触发快照重同步。

---

## 一致性设计（重复提交 / 断线重连 / 截止竞争）

- **事件溯源 + 单调版本号**：所有状态由 `events(session_id, version, seq)` 折叠得到；
  一次原子提交占用且仅占用一个新版本，版本号恒为 `last_version + 1`（join、start、
  submit、结算、终止无一例外，绝不写死）；同一次提交内的多个事件按固定因果顺序
  分配 `seq = 0,1,2…`，因此主键 `(session_id, version, seq)` 不会冲突，
  任意版本前缀都能确定性回放。各路径的稳定顺序：

  | 操作 | 版本 | 事件（按 seq 顺序） |
  | --- | --- | --- |
  | join | v+1 | `participant_joined` |
  | start | v+1 | `session_started` → `stage_opened` |
  | submit（未触发结算） | v+1 | `stage_choice` |
  | submit（末名提交，自动结算） | v+1 | `stage_choice` → `stage_sealed` → `stage_opened`/`session_ended` |
  | submit（超过截止，被拒） | v+1 | `stage_sealed` → `stage_opened`/`session_ended` |
  | 截止/手动结算 | v+1 | `stage_sealed` → `stage_opened`/`session_ended` |
  | 终止（未开始） | v+1 | `session_ended` |
  | 终止（进行中） | v+1、v+2 | `stage_sealed` →（`stage_opened`）；`session_ended` |
  | 联演注入投递（目标会话） | v+1 | `joint_injection`（AND 屏障全部满足后的一次性注入；迟到补投带 `late:true`） |

- **原子结算**：写操作在会话级 `asyncio.Lock` 内执行
  `BEGIN IMMEDIATE`，提交选择与可能触发的结算在同一事务内完成。
- **截止竞争**：到达 deadline 后到达的提交会被拒绝，且“拒绝 + 超时结算”在同一事务；
  每个定时器在创建时**捕获自身的 `expected_deadline`** 并随回调一路传入
  `Engine.settle`，在**同一个 SQLite 事务内**与当前 `state.deadline` 精确比对：
  阶段一的截止任务已经唤醒，但末名提交或主持人结算先把会话推进到同样有限时的
  阶段二时，若旧回调受会话锁或事件循环延迟、直到阶段二也到期才真正执行，
  身份不符（捕获值 ≠ 当前值）的旧回调只会**空转且不新增任何事件**，
  绝不会被当成阶段二的超时写入结算；阶段二自己的正常回调不受影响，
  且同一阶段的重复回调最多结算一次。`duration: null` 的阶段永不超时。
  主持人手动结算与定时器结算由同一把锁串行化，同一阶段只会被结算一次。
- **重复提交**：`submissions(session, stage, role)` 主键去重；
  重复请求（含阶段已结束后的重发）幂等返回首次结果，不产生第二条事件；
  重复的开始/加入/终止请求被拒绝或空转，同样不新增事件。
- **断线重连**：WebSocket 指数退避自动重连，连上即收到当前版本快照；
  在线状态随连接建立/关闭广播；服务重启后为所有运行中会话重新武装截止定时器。
- **权限裁剪**：参与者快照只含已开放阶段、公开文本、本人视图/选项/本人选择；
  其他角色的私有视图、选项分支与决定不会出现在任何参与者响应、回放或导出中。
  跨会话的**联演注入**沿用同一裁剪：参与者只见注入的公开文本与发给本人角色的密报，
  其他成员会话是否存在、其他角色的密报均不可见。
- **确定性回放**：回放只依赖事件日志与会话**绑定的固定剧本版本**，逐版本折叠，
  同一版本永远得到同一快照；主持人可见全量事件，参与者仅得到本人视角。
  剧本后来如何编辑/发布都不改变既有会话的回放结果。
