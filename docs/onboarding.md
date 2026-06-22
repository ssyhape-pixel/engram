# Engram 新人入门指南

> 面向第一次接触本仓库、也第一次接触 agent-memory 系统的开发者。尽量少用术语；必须用的术语（CAS、trigram、embedding、RRF 等）第一次出现时一句话解释。
> 权威设计见 `architecture.md`；分层设计见 `superpowers/specs/`。本文讲的是**已经建好的东西**（L1–L4 已合并；L5 维护 worker 尚未实现）。

## 1. 一句话 + 电梯演讲

**一句话：** Engram 是一个云端、多租户的 Agent 记忆系统——让 AI Agent 在每次对话结束后依然记得它学过的东西，下次对话从断点继续。

**电梯演讲：** 现有的 LLM 对话是无状态的：会话结束，记忆消失。Engram 解决这个问题——给每个 Agent 一份**可自我编辑、版本化的"记忆仓库"**，存在云端、跨会话持久。多个 Agent 各自隔离，共用一套无状态计算节点，由一个 Postgres 充当协调中枢。整套设计刻意轻量：没有 Kafka，没有 Temporal——**一个对象存储桶 + 一个 Postgres，就够了。**

## 2. 一个比喻：图书馆 + 一本带版本历史的私人笔记本

- **图书馆员（Session Router）**：每次 Agent 来访，图书馆员确保同一时刻只有一位读者能修改这本笔记本（**单写者**），防止两个进程同时乱写。
- **私人笔记本（每个 Agent 的 Git 记忆仓库）**：每一个历史版本都被永久保存、永不覆盖——只能往后翻新页，不能擦旧内容。笔记本存在云端**对象存储**（S3/OSS），按内容哈希编址。
- **书签（HEAD ref，在 Postgres 里）**：书架目录记录"这位 Agent 当前翻到第几页"。这是唯一会被修改的东西，改时必须用原子操作 **CAS**（Compare-And-Swap：先比较当前值是否符合预期，符合才更新、否则失败——防并发冲突）。
- **常用摘要页（`system/` 目录）**：笔记本最前面几页是"常驻内容"，每轮对话都塞进 LLM 上下文。小而精，每轮必读。
- **索引卡（tree index）**：一张快速索引，列出所有文件的文件名 + 一句话描述，让 Agent 知道"我有哪些记忆"而不必全部读入。
- **桌边速查复印件（L3 读缓存）**：图书馆员在桌边放一份常用摘要的复印件（按笔记本版本号索引），省去每次翻原本。
- **全文检索服务（L4 混合搜索）**：Agent 中途说"找一下关于 X 的段落"，图书馆员用两套系统——按关键词精确匹配（**trigram 索引**）+ 按语义意思模糊匹配（向量相似度）——再把两套结果融合排名（**RRF**，Reciprocal Rank Fusion：把两份排名按各自名次的倒数相加合并，兼顾两种召回的优势），只返回最相关的几行，不是整篇。

## 3. 三条铁律（不可违反的设计约束）

**① 对象不可变。** 文件内容、目录树、提交记录——一旦写入，永不修改；每个对象由其内容的哈希唯一标识（内容寻址）。好处：可放心按哈希缓存（同哈希永远对应同内容，缓存永不需要"失效"概念）、跨 Agent 去重、用标记-清除回收无用对象（GC）。

**② 引用（ref）是唯一可变指针，且强一致。** `agent_id → HEAD` 存 Postgres，更新必须 CAS。所有并发控制、写顺序、一致性全部汇聚到这一个点；两个并发写者只有一个能赢，另一个重试。没有第二个并发控制点。

**③ 其他一切都是可丢弃的派生视图。** 缓存、搜索索引、worker 工作目录——可随时销毁重建。Pod 被杀没关系，Postgres + 对象存储里的内容才是真相。这条让系统能无状态横向扩展。

## 4. 一次对话发生了什么（端到端）

假设 Agent A 开始一轮新对话：

1. **Session Router 准入**：确保当前只有一个 Session 在为 A 处理写入；有冲突则后来者排队/被拒。
2. **解析 HEAD，物化工作树**：从 Postgres 读出 A 当前的 HEAD 提交哈希，从对象存储把对应版本的文件树恢复到 worker 本地临时目录（"工作树"，用完即弃）。
3. **组装上下文（走 L3 缓存）**：读 `system/` 常驻内容 + tree index。L3 缓存以 `system/` 子树哈希为键——只要这部分没变，即使别的文件改了，缓存依然命中，不用重读对象存储。拼成 LLM 初始上下文发给模型。
4. **模型在循环中途主动拉记忆（工具调用）**：模型不是一次性拿到全部记忆，而是在推理中按需调用：
   - `recall(query)` → L4 混合搜索（trigram 精确 + 语义向量，RRF 融合），返回最相关的**几行**。
   - `read(path[, 行范围])` → 读具体文件。
   - `edit(path, content)` → 改记忆文件（写入工作树本地副本）。
5. **提交（先写对象，再更新 ref）**：
   - 先把新文件内容、目录树、提交对象写入对象存储（幂等，崩溃重试无碍）。
   - 再在**一个 Postgres 事务**里：CAS 更新 HEAD（旧值须等于本次读取的 HEAD，否则失败）+ 向 job 队列插入维护任务。
   - 顺序关键：写对象后、CAS 前崩溃，只留可被 GC 清理的孤立对象，绝不出现"ref 指向不完整提交"的撕裂状态。

## 5. 已建好的四层（L1–L4）

**L1 — 基础存储（MemStore）** · `internal/memstore`
对象存储后端（开发用本地 FS，生产用 S3）、`gitfs`（用 go-git + 自定义 Storer 把 git 数据结构映射到对象存储，不调系统 git）、Postgres refs 表（CAS + job 队列）。整个系统的权威数据源。

**L2 — Agent 循环** · `internal/agent` + `cmd/api`
Router（单写者序列化）、Session（持有工作树和工具集）、工具（recall/read/edit）、provider 适配（fake 测试 / anthropic 真 API）。`cmd/api` 是可直接运行的交互式开发入口。

**L3 — SHA 读缓存** · `internal/cache`
线程安全的按条目数 LRU（最近最少使用淘汰）。`assembleSystem` 先查缓存（键 = `system/` 子树哈希），命中即用、未命中才重算并写回；有未提交编辑时绕过缓存，保证不读脏数据。每 pod 一个共享 LRU、跨 agent 去重。

**L4 — 混合搜索** · `internal/search`
`TrigramIndex`（按行 trigram 倒排，查询零对象存储访问）+ `SemanticIndex`（按 markdown 标题切 section，用 Voyage embedding 生成向量，暴力 cosine 相似度；向量复用 L3 LRU，键是分块内容哈希，一段内容一生只 embed 一次）+ `HybridSearch`（RRF 融合两者，语义服务故障时自动降级为纯 trigram）。默认注入 Router，recall 工具调用方式不变。

## 6. 还没建的（L5）· `cmd/maintenance` / `internal/maintenance`

维护 worker——一个**独立异步进程**，通过 Postgres job 队列取活（Agent 提交时同事务入队），在自己的 git worktree 里操作、完成后合并回主 HEAD，不阻塞前台会话：

- **Reflection**：回顾最近提交历史，用 LLM 把分散记录整合成更结构化的记忆。
- **Defrag**：拆分过大文件、合并重复条目、整理目录结构。
- **GC**：标记-清除不可达对象（提交失败遗留的孤立对象）。
- **增量 reindex**：按 `git diff old..new` 只重建变更部分的搜索索引。

设计成独立进程而非嵌入前台，是因为这些任务耗时、不在每轮推理的关键路径上，且要求严格的单-agent 独占锁（不能两个进程同时 defrag 同一个仓库）。

## 7. 新人最容易误解的 3 点

**误解一："系统启动时会把相关记忆全部喂给模型"——错。**
Engram 的记忆是 **agentic，不是 RAG**（检索增强生成：推理前自动检索并填入上下文）。推理开始时上下文里只有 `system/`（少量核心常驻）+ tree index（文件名索引）。长尾记忆由模型在推理循环中途**主动调用 recall 工具**按需拉取，不预先把 top-k 段落自动塞进去。这样模型自己决定要哪些记忆，也避免 context 窗口被撑爆。

**误解二："本地磁盘 / 缓存 / 搜索索引存了重要数据"——错。**
工作树、L3 读缓存、L4 搜索索引全是**派生视图，可随时丢弃**，能从对象存储 + Postgres 从零重建。Pod 崩溃、缓存清空、索引损坏都不是数据丢失，只是下次重算。真正的数据只有两处：对象存储里的不可变内容、Postgres 里的 HEAD ref。

**误解三："需要 Redis / Kafka / 分布式队列才能协调多 worker"——错。**
协调中枢只有一个 Postgres：并发控制靠 CAS（一行 SQL 原子更新），维护队列也存在 Postgres 的 job 表里，提交和入队在同一事务完成（没有 Kafka outbox 的双写不一致问题）。这是刻意取舍：在 Agent 记忆规模下这已足够，不值得为"标准分布式架构"引入额外运维复杂度。

---

## 上手做点什么

```bash
# 构建
go build ./...

# 起一个测试用 Postgres
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"

# 跑测试
ENGRAM_TEST_DB="$ENGRAM_TEST_DB" go test ./...

# 跑交互式 dev 入口（fake provider，读 stdin）
ENGRAM_PROVIDER=fake go run ./cmd/api
```

接着读 `architecture.md`（总设计），再按 `superpowers/specs/` 里 L1→L4 的顺序看分层设计，对照 `internal/` 下对应包的代码。
