# Engram L2 — 泛化 Agent Loop + Session 设计

> 状态：已通过 brainstorm 评审（2026-06-10）。下一步：writing-plans 生成实现计划。
> 依赖 L1（已合并入 main）：`internal/memstore`（MemStore：ResolveHead / Materialize / CommitWithCAS / CreateAgent）。
> 北极星总架构见 `docs/architecture.md`；L1 基座设计见 `docs/superpowers/specs/2026-06-09-engram-l1-foundation-design.md`。

## 0. 决策前提（已对齐）

1. **泛化 agent loop**：`LLMProvider` 做成 provider 无关接口 + 确定性 `FakeProvider`（驱动所有 TDD）+ 真 `AnthropicProvider`（Messages API + tool-use，读 `ANTHROPIC_API_KEY`）。cmd/api 能用真模型跑完整闭环。
2. **接入面：Go 库接口**（同进程）。HTTP/MCP/跨语言留到以后。
3. **有状态 Session**：Session 持有一次会话生命周期内的临时 chat history + 活的工作目录；**记忆 repo 仍是唯一长期持久态**。
4. **工作树策略 A**：Session 起始物化一次工作目录，跨 turn 复用；每 turn 结束若本轮发生过 `edit` 则 `CommitWithCAS` 推进 HEAD，否则跳过提交（不产生空 commit）。
5. **recall 仍是 grep 桩**：L2 的 `Search.Recall` = 工作树 grep 返回行范围；L4 换 trigram（variant A）。

## 1. 范围

### In scope（L2）
- `internal/agent/provider.go`：`LLMProvider` 接口 + provider 无关协议类型（Request/Response/Message/ToolDef/ToolCall/ToolResult）。
- `internal/agent/fake.go`：`FakeProvider`——脚本化确定性 provider。
- `internal/agent/anthropic.go`：`AnthropicProvider`——真 Anthropic Messages API + tool-use 映射。
- `internal/agent/tools.go`：记忆工具集 list/read/recall/edit，绑定到 Session 工作目录。
- `internal/agent/session.go`：`Session`——持有 agentID/HEAD/workdir/chat history/toolset；`Send` 跑一 turn。
- `internal/agent/router.go`：单写者 `Router`——per-agent 进程内锁，发放/回收 Session。
- `internal/search/search.go` + `grep.go`：`Search` 接口 + recall 的 grep 桩实现。
- `cmd/api/main.go`：dev 装配 + 手动跑 turn 的入口（fake 或 anthropic 可选）。

### Out of scope（后续层）
- L3：SHA 键读缓存的真实实现（system/ + tree index 的 LRU）。本层 system/ + tree index 每 turn 重算。
- L4：trigram 索引 + 真实 recall + reindex。本层 recall = grep 桩。
- L5：River + 维护 worker（reflection/defrag/GC/reindex 消费 memory_jobs）；多 pod sticky 路由。本层 Router 仅进程内锁；commit 时照常入队 reindex/reflect job（消费者 L5）。
- HTTP/MCP/跨语言接入面。
- 多写者合并、会话压缩（compaction）/ reflection 触发策略。

## 2. 继承的不变量（L2 不得破坏）

- 记忆对象不可变、内容寻址；唯一可变指针 `agent_id→HEAD` 经 MemStore 的单点 CAS；写序"先对象后 ref"由 `CommitWithCAS` 保证。
- **单写者 per agent**：Router 保证一个 agent 同时只有一个写者 Session。不做有损 markdown 合并。
- cache/search/工作副本是派生、可丢弃；对象 + ref 才是权威。
- `system/` 保持小而精（每 turn 入上下文）。recall 不做推理前 top-k 灌入——长尾由模型 mid-loop 拉取。
- `context.Context` 为所有 I/O 首参；`%w` 包裹错误；小接口、表驱动测试。

## 3. 组件设计

### 3.1 LLMProvider 协议（`internal/agent/provider.go`）

provider 无关，刻意贴近 Anthropic Messages 的 tool-use 形态以便映射，但不含任何 Anthropic 专有类型。

```go
type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolDef advertises a tool to the model.
type ToolDef struct {
	Name        string
	Description string
	InputSchema map[string]any // JSON Schema for the tool's input object
}

// ToolCall is a model-initiated tool invocation.
type ToolCall struct {
	ID    string
	Name  string
	Input map[string]any
}

// ToolResult is the outcome of executing a ToolCall, fed back to the model.
type ToolResult struct {
	CallID  string
	Content string
	IsError bool
}

// Message is one turn-internal conversation entry.
type Message struct {
	Role      Role
	Text      string       // user/assistant text
	ToolCalls []ToolCall   // assistant-initiated calls (Role==assistant)
	Results   []ToolResult // tool results (Role==tool)
}

type Request struct {
	System   string
	Messages []Message
	Tools    []ToolDef
}

// Response: if ToolCalls is non-empty the turn is NOT complete — the caller
// executes them and feeds ToolResults back via a new Generate call. Otherwise
// Text is the final assistant message.
type Response struct {
	Text      string
	ToolCalls []ToolCall
}

type LLMProvider interface {
	Generate(ctx context.Context, req Request) (Response, error)
}
```

### 3.2 FakeProvider（`internal/agent/fake.go`）

确定性、脚本化，用于所有自动测试，不触网。脚本是一组按调用序号产出的 `Response`（含可选断言钩子，检验上一轮 `ToolResult`）。

```go
// FakeProvider returns scripted responses by call index, driving the agent
// loop deterministically in tests.
type FakeProvider struct {
	Steps []func(req Request) Response // one per Generate call; index = call count
	calls int
}
func (f *FakeProvider) Generate(ctx context.Context, req Request) (Response, error)
```
- 典型脚本：第 0 次返回 `{ToolCalls:[recall("x")]}`，第 1 次返回 `{ToolCalls:[edit("note.md","...")]}`，第 2 次返回 `{Text:"done"}`。
- 调用次数超出 `Steps` 长度 → 返回 error（防测试写错）。

### 3.3 AnthropicProvider（`internal/agent/anthropic.go`）

把 provider 无关 Request 映射到 Anthropic Messages API（HTTP POST `/v1/messages`，`anthropic-version` 头，`x-api-key` 读 `ANTHROPIC_API_KEY`）。

- `Tools` → Anthropic `tools`（`name`/`description`/`input_schema`）。
- `Message` 映射：`RoleUser`→user text block；`RoleAssistant` 文本 + `tool_use` blocks；`RoleTool`→user 消息内的 `tool_result` blocks（`tool_use_id`=CallID，`is_error`）。
- 响应 `stop_reason=tool_use` → 收集 `tool_use` blocks 为 `Response.ToolCalls`；否则汇总 text 为 `Response.Text`。
- 默认模型 `claude-sonnet-4-6`（构造参数可配），非流式，`max_tokens` 可配（默认 4096）。
- HTTP client 可注入（测试用 mock RoundTripper 验证请求构造 + 解析；不打真 API）。

### 3.4 记忆工具集（`internal/agent/tools.go`）

工具对着 Session 的工作目录 `workdir` 操作；`recall` 走注入的 `Search`。每个工具暴露 `ToolDef` 并提供 `Execute(input map[string]any) ToolResult`。

- `list`：遍历 `workdir`，对每个文件读 YAML frontmatter 的 `description`，返回 `path: description` 列表（tree index）。
- `read`：入参 `path`，可选 `start`/`end`（1-based 行范围）；返回内容或行范围。路径校验：必须落在 `workdir` 内（拒绝 `..` 逃逸）。
- `recall`：入参 `query`；调 `search.Recall(ctx, agentID, query, k)`；返回 `[]Hit{Path,LineStart,LineEnd,Snippet}` 的文本化。
- `edit`：入参 `path`/`content`；写入 `workdir`（建中间目录）；置 Session `dirty=true`；路径校验同 read。
- 工具集 `Toolset` 聚合这四个，提供 `Defs() []ToolDef` 与 `Dispatch(ctx, ToolCall) ToolResult`（按 Name 路由，未知工具→IsError）。

### 3.5 Session（`internal/agent/session.go`，有状态）

```go
type Session struct {
	agentID string
	store   memstore.MemStore
	prov    LLMProvider
	tools   *Toolset
	head    memstore.CommitHash
	workdir string
	history []Message
	dirty   bool
	maxSteps int // 防失控 tool-use 循环，默认如 16
}
```
`Send(ctx, userMessage string) (string, error)`：
1. `history = append(history, {Role:user, Text:userMessage})`。
2. 循环至多 `maxSteps`：
   - `sys := assembleSystem(workdir)`（system/ 常驻内容 + tree index）。
   - `resp, err := prov.Generate(ctx, {System:sys, Messages:history, Tools:tools.Defs()})`。
   - 若 `len(resp.ToolCalls)>0`：append `{Role:assistant, ToolCalls:resp.ToolCalls}`；对每个 call `tools.Dispatch` → 收集 `[]ToolResult`；append `{Role:tool, Results:...}`；continue。
   - 否则：append `{Role:assistant, Text:resp.Text}`；break。
   - 循环超过 `maxSteps` 仍在调工具 → 返回 error（失控保护）。
3. 若 `dirty`：`newHead, err := store.CommitWithCAS(ctx, agentID, head, workdir, []memstore.Job{{Kind:"reindex"},{Kind:"reflect"}})`；冲突→见 §4 重试；成功→`head=newHead; dirty=false`。
4. 返回最后 assistant 文本。

`assembleSystem(workdir)`：读 `system/` 下所有文件内容（常驻）+ 遍历全树生成 tree index（path + frontmatter description）。保持 system/ 小。

### 3.6 Router（`internal/agent/router.go`，单写者）

进程内每 agent 一把锁，保证同一 agent 同时只有一个写者 Session。

```go
type Router struct {
	store   memstore.MemStore
	prov    LLMProvider
	mu      sync.Mutex
	locks   map[string]*agentLock // per-agent writer lock
	scratch string                // base dir for materialized workdirs
}
// Open acquires the agent's writer lock, materializes HEAD into a fresh
// workdir, and returns a Session. Returns ErrAgentBusy if already held.
func (r *Router) Open(ctx context.Context, agentID string) (*Session, error)
```
- `Open`：尝试取 agent 锁——已被占用 → `ErrAgentBusy`（不阻塞，调用方决定重试/排队）。`ResolveHead` → `Materialize(head, workdir)` → 按该 `workdir` 构造一个 per-session `GrepSearch(workdir)`（见 §3.7）注入工具集 → 构造 Session。`Search` 是 per-session、绑定到当前 agent 活工作树的；Router 不持有全局 Search（L4 换 trigram 时再决定索引的生命周期归属）。
- `Session.Close()`：释放锁、`os.RemoveAll(workdir)`。
- L5 注记：多 pod 时锁要换成 sticky 路由 / 分布式协调；CAS 仍是最终兜底。

### 3.7 Search 接口 + grep 桩（`internal/search/`）

```go
type Hit struct { Path string; LineStart, LineEnd int; Snippet string }
type Search interface {
	Recall(ctx context.Context, agentID, query string, k int) ([]Hit, error)
	// Reindex is a no-op stub in L2; real incremental index is L4.
	Reindex(ctx context.Context, agentID string, from, to string) error
}
```
- `GrepSearch`：对一个工作目录做子串/正则扫描，返回最多 k 个命中的行范围（含少量上下文 snippet）。`Reindex` no-op。绑定 workdir 由 Router/Session 注入（L2 recall 只扫当前 agent 的活工作树）。

### 3.8 cmd/api（`cmd/api/main.go`）

dev 装配 + 手动跑 turn 的最小入口：
- 读 env：`ENGRAM_DB`（Postgres DSN）、`ENGRAM_OBJ`（本地对象根目录）、`ENGRAM_PROVIDER`（`fake`|`anthropic`）、`ANTHROPIC_API_KEY`、`ENGRAM_AGENT`（agent id）。
- 装配 ObjStore(local) + refs(pgx pool, Migrate) + MemStore；按需 `CreateAgent`（首次 seed 一个 `system/about.md`）。
- 构造 Router(store, provider, GrepSearch)；`Open` 一个 Session；从 stdin 读用户消息、`Send`、打印 assistant 文本；`Close`。
- 用途：拿真 Anthropic 跑通 recall/read/edit/commit 闭环的人肉验证；不属自动测试。

## 4. 错误处理

- 所有 I/O `%w` 包裹，`context.Context` 首参。
- `ErrCASConflict`（单写者下罕见）：Session 在 commit 处捕获 → `ResolveHead` 重解析 + `Materialize` 重新物化 workdir → **重放本轮的 edit**？不可行（edit 已写入旧 workdir）。L2 策略：重新物化后，把本轮 dirty 文件从旧 workdir 拷贝覆盖到新 workdir，再次 `CommitWithCAS`；仍冲突则返回错误交调用方。单写者下基本不触发，保持实现简单、有界重试（如 1 次）。
- `ErrAgentBusy`：Router.Open 在锁被占用时返回，调用方自行处理。
- `maxSteps` 超限：返回 error，避免失控 tool-use 循环消耗 token。
- AnthropicProvider：HTTP 非 2xx / 解析失败 → 包裹错误返回；不在库层重试（交调用方/上层）。

## 5. 测试策略（表驱动；fake 驱动，不触网）

- **provider 协议**：FakeProvider 往返；构造 Request、读取 Response.ToolCalls/Text。
- **anthropic 映射**：注入 mock `http.RoundTripper`，断言请求 body（tools / messages / tool_result 映射、模型、max_tokens、头部），并用录制的响应 JSON 验证解析（tool_use → ToolCalls，end_turn → Text）。**不打真 API。**
- **toolset**：list（frontmatter description 提取）、read（全文 + 行范围 + 路径逃逸拒绝）、edit（写入 + dirty 置位 + 路径逃逸拒绝）、recall（经 GrepSearch 返回行范围）。
- **GrepSearch**：在临时工作树上命中行范围、k 截断、无命中。
- **Session 端到端**（需 live Postgres + local ObjStore）：FakeProvider 脚本 `recall→read→edit→done` 跑一 turn → 断言文件被编辑、`dirty` 触发 commit、HEAD 推进、history 顺序正确（user→assistant(toolcalls)→tool(results)→…→assistant(text)）。
- **多 turn Session**：第二 turn 基于第一 turn 推进后的 HEAD；纯读 turn（无 edit）不产生 commit、HEAD 不变；chat history 跨 turn 累积。
- **Router 单写者**：同 agent 第二次 `Open` 在未 `Close` 时返回 `ErrAgentBusy`；`Close` 后可再 Open。
- **CAS 重试路径**：构造一个在 commit 前推进了 HEAD 的场景（直接用底层 store 抢先 commit），断言 Session 的有界重试行为符合 §4。

## 6. L2 完成标志（Definition of Done）

通过 Router 拿到某 agent 的 Session，在多轮对话中模型用 recall/read/edit 工具读改记忆；每个含改动的 turn 把工作目录持久化为可 diff 的新 commit、HEAD 推进、并入队 reindex/reflect job（消费者 L5）；纯读 turn 不产生 commit。FakeProvider 全自动覆盖该闭环与多 turn/单写者/CAS 重试；AnthropicProvider 经 mock HTTP 测试请求/解析，并能在 cmd/api 用真模型人肉跑通。

## 7. 守则（继承自 CLAUDE.md）

- 不引入 Temporal/Kafka；维护 job 经 MemStore 同-tx 入队，消费在 L5。
- 不修改对象；并发控制只在 ref CAS 这一个序列化点（Router 锁是写者准入，不是第二个一致性点）。
- 不把 recall 变成推理前自动 top-k 灌入；长尾 mid-loop 拉取。
- `system/` 不无界增长。
- 不 shell out git；不直触对象后端（走接口）。
