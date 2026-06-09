# Engram L1 — 基座 + 泛化 Agent Loop 设计

> 状态：已通过 brainstorm 评审（2026-06-09）。下一步：writing-plans 生成实现计划。
> 北极星总架构见 `docs/architecture.md`（四区拓扑，对齐 M5）。本文档只深度设计 **L1**——第一个端到端可跑、可验证的真实单元。

## 0. 决策前提（已对齐）

1. **架构对齐 M5、分层落地**：内部架构按 M5 设计（River 队列、search variant A、为 warm sidecar 预留接口），代码逐层实现、每层独立可验证、可 diff。
2. **泛化 agent loop**：`LLMProvider` 做成接口（fake 测试 + 真 Anthropic 适配器，后者属 L2），agent loop 本身 provider 无关。
3. **接入面先做 Go 库接口**：agent loop 是同进程 Go 程序，通过干净 Go 接口消费记忆系统；HTTP/MCP/跨语言留到以后包一层。
4. **gitfs 走自定义 Storer over ObjStore**（文档首选）：blob/tree/commit 天生内容寻址，这是 SHA 缓存 / 跨版本去重 / mark-sweep GC 成立的根。

## 1. 范围

### In scope（L1）
- `objstore`：ObjStore 接口 + 本地 FS 后端。
- `gitfs`：go-git 自定义 Storer over ObjStore；物化工作树。
- `refs`：Postgres refs + CAS + 同 tx 入队。
- `memstore`：MemStore，组装三者，实现 `docs/architecture.md §11` 接口。
- `agent`：泛化 agent loop——`LLMProvider` 接口 + `FakeProvider` + 记忆工具集（list/read/recall/edit）+ 上下文装配 + commit。
- `search` / `cache`：仅定义接口 + L1 最简实现（grep / passthrough）。
- `db/migrations`：`agent_refs` / `memory_jobs` / `maintenance_cursor`。
- `cmd/api`：dev 装配 + 一个手动跑一轮 turn 的入口。

### Out of scope（留给后续层）
- L2：真 Anthropic Messages 适配器 + 单写者 router。
- L3：SHA 键读缓存的真实实现（system/ + tree index 的 LRU）。
- L4：trigram 索引（variant A）+ line-range recall 的真实实现 + reindex。
- L5：River（同 tx InsertTx 入队）+ 维护 worker（reflection/defrag/GC/reindex）+ git worktree 写回。

## 2. 不变量（继承自架构，L1 必须守住）

1. **对象不可变**：内容寻址，只追加。
2. **ref 可变且强一致**：唯一可变指针 `agent_id -> HEAD` 在 Postgres，所有并发收口到对它的原子 CAS。
3. **其余皆派生**：cache / index / 工作副本可从对象重建，无状态、可丢弃。
4. **写序：先对象后 ref**：CAS 前崩溃只留可 GC 的垃圾对象，绝无撕裂 ref。

## 3. 组件设计

### 3.1 ObjStore（`internal/memstore/objstore`）

```go
type ObjStore interface {
    Has(ctx context.Context, key string) (bool, error)
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, data []byte) error // 幂等
    Iter(ctx context.Context, fn func(key string) error) error // 为 GC 预留
}
```

- 本地 FS 后端：`objects/<hash[:2]>/<hash>`，分片避免单目录过大。
- 幂等 PUT：已存在则跳过；否则写临时文件 + rename，保证原子可见。
- S3/OSS 后端 L 以后接，同接口；强制 `ObjStore` 抽象，服务层永不直触后端。

### 3.2 gitfs — 自定义 go-git Storer（`internal/memstore/gitfs`）

go-git 的 `storage.Storer` 由多个子接口组合，逐一处理：

- **`EncodedObjectStorer`** → 落到 ObjStore：
  - `SetEncodedObject`：算 git hash、序列化对象字节、`objstore.Put(<hash>, bytes)`。
  - `EncodedObject` / `HasEncodedObject` / `IterEncodedObjects`：从 ObjStore 读。
  - blob/tree/commit 天生内容寻址、跨版本去重原生成立。
- **`ReferenceStorer`** → 进程内 memory ref storer：每次会话从 Postgres 把 HEAD 种进去。**权威 ref 永远在 Postgres，不在 git。** go-git 创建 commit 需要 ref，这层只服务单次物化/提交会话。
- **Config / Index / Shallow / Module storer** → 内存 / no-op。

物化：
- `Materialize(ctx, agentID, sha, dir)`：用 go-git checkout 指定 commit 的 tree 到 SHA-keyed scratch 目录（本地磁盘 / tmpfs），供工具集编辑。目录按 SHA 命名，可缓存、可丢弃、可重建。

### 3.3 refs（`internal/memstore/refs`，pgx）

表结构见 `docs/architecture.md §9`：`agent_refs`、`memory_jobs`、`maintenance_cursor`。golang-migrate，expand-contract，永不在单步做破坏性变更。

- `ResolveHead(ctx, agentID) -> CommitHash`：`SELECT head FROM agent_refs WHERE agent_id=$1`。
- `CommitRef(ctx, agentID, parent, new, jobs)`：**单 Postgres tx**
  1. `UPDATE agent_refs SET head=$new, updated_at=now() WHERE agent_id=$id AND head=$parent`；
  2. 0 行 → 返回 `ErrCASConflict`；
  3. 同 tx 对每个 job `INSERT INTO memory_jobs (...)`（L1 入 `reindex`/`reflect` 行，worker 桩）。
- Bootstrap：新 agent → 建初始空 commit（空 tree）+ `INSERT agent_refs`。
- **L5 River 切换点**：把第 3 步的 `INSERT memory_jobs` 换成 `river.InsertTx(ctx, tx, ...)`，同 tx 语义不变。`memory_jobs` 表在 L1—L4 期间充当队列。

### 3.4 MemStore（`internal/memstore`）

实现 `docs/architecture.md §11` 接口：

```go
type MemStore interface {
    ResolveHead(ctx context.Context, agentID string) (CommitHash, error)
    Materialize(ctx context.Context, agentID string, at CommitHash, dir string) error
    CommitWithCAS(ctx context.Context, agentID string, parent CommitHash, dir string, jobs []Job) (CommitHash, error)
}
```

`CommitWithCAS` 流程（**先对象后 ref**）：
1. 以自定义 Storer（对象 → ObjStore）打开 repo，memory ref storer 种入 `parent`。
2. `worktree.Add` + `worktree.Commit` 暂存工作树变更并建 commit。
3. go-git 写出新 blob/tree/commit → 已落 ObjStore（幂等）。
4. 取新 commit hash。
5. 调 `refs.CommitRef(parent → new, jobs)`（单 tx：CAS + 入队）。
6. 0 行 → `ErrCASConflict`。

### 3.5 泛化 agent loop（`internal/agent`）

```go
type LLMProvider interface {
    // 消息 + 工具定义 → 文本 + 工具调用
    Generate(ctx context.Context, req Request) (Response, error)
}
```

- **FakeProvider**：脚本化 / 确定性，按预定序列发工具调用（如 `recall → read → edit → done`），让 L1 不依赖网络即可端到端验证。L2 接真 Anthropic 适配器，同接口。
- **记忆工具集**（对着工作副本）：
  - `list`：tree index——文件名 + 各文件 frontmatter 的 `description`。
  - `read(path, range?)`：读文件（可选行范围）。
  - `recall(query)`：L1 桩 = 工作树内 grep，返回匹配行范围；L4 换 trigram。
  - `edit(path, content)`：写工作副本。
- **循环**：`ResolveHead → Materialize → 装配上下文（system/ 常驻 + tree index）→ LLMProvider.Generate → 执行工具调用 → 完成时 CommitWithCAS`；`ErrCASConflict` → 重解析 HEAD 重试整轮（单写者下罕见）。

### 3.6 上下文装配 / 文件格式

- `system/` 下文件全量常驻（每轮入上下文，保持小而精）。
- tree index = 遍历 tree、读每文件 YAML frontmatter 的 `description`（文件名 + 描述）。
- recall 命中的行范围在 mid-loop 追加，不做推理前 top-k 灌入。
- 文件格式：markdown + YAML frontmatter，`description` 必填；`system/` 常驻，其余懒加载。

### 3.7 派生视图接口（L1 定义 + 最简实现）

- `cache.SystemCache`：L1 = passthrough（每次重建，无缓存）；L3 接 LRU。
- `search.Search`：L1 `Recall` = 工作树 grep 返回行范围，`Reindex` = no-op；L4 接 trigram variant A。
- 接口在 L1 即定型，L2–L5 替换实现不动调用方。

## 4. 错误处理

- 所有 I/O 错误 `%w` 包裹，保留 `errors.Is/As` 链。
- `ObjStore.Put` 幂等；`Get` 缺失返回明确错误。
- `ErrCASConflict`：L1 走"重解析 HEAD + 重跑整轮 turn"，**不做有损 markdown 合并**（单写者下冲突罕见）。
- `context.Context` 为所有 I/O 首参，支持取消 / 超时。

## 5. 测试策略（表驱动）

- **objstore**：临时目录往返 + 幂等（重复 Put 同 key 不报错、内容一致）。
- **gitfs**：blob/tree/commit 写读往返、hash 稳定；materialize → edit → commit → diff 可见预期变更。
- **refs**：需 Postgres（CI 容器）；**CAS 冲突测试**——同 parent 两次 commit，仅一胜，败者得 `ErrCASConflict`。
- **agent loop**：FakeProvider 脚本跑一轮 → 断言历史含 read/edit/commit、commit 可 diff。
- **端到端**：objstore(tmp) + Postgres(test) + fake LLM 跑一 turn → 断言 HEAD 推进、文件内容变更、历史多一个 commit。

## 6. L1 完成标志（Definition of Done）

一个 agent 能通过泛化 loop 读 / 编辑 / commit 记忆，历史版本化、可 diff，CAS 防并发；ObjStore、Postgres、LLMProvider 三者均真实可换；cache / search / maintenance 接口已定义、留最简实现，可无痛接 L2–L5。

## 7. 守则（继承自 CLAUDE.md，L1 不得违反）

- 不引入 Temporal / Kafka。
- 不修改对象（追加式、内容寻址）。
- 并发控制只在 ref CAS 这一个序列化点。
- `system/` 不得无界增长。
- cache / index / 工作副本不是真相源——派生、可丢弃；对象 + ref 才是权威。
- 不 shell out `git` 二进制；git 只作工作格式，走 go-git。
