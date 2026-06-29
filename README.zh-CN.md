# Engram

[English](README.md) · **简体中文**

**一套面向 AI agent 的云端、多租户记忆系统。**

Engram 给每个 agent 一本可跨会话**读取、搜索、持久编辑**的"版本化笔记本"。它把
一套 git 风格的记忆文件系统搬到无状态服务端:用**内容寻址的不可变对象**做存储,用
**一处 Postgres 的 compare-and-swap** 保证一致性,再用一个**后台 worker** 持续打理
每个 agent 的记忆——反思整合、拆分大文件、垃圾回收、崩溃恢复。

这里的记忆是**主动式的,不是 RAG**:推理前只装载一小份常驻要点 + 一份"有哪些东西"的
薄目录;真正的细节由模型在思考过程中**自己**通过 recall/read 工具去取,且返回精确的行号范围。

![status](https://img.shields.io/badge/status-L1–L5d%20complete-2ea44f)
![go](https://img.shields.io/badge/Go-1.25-00ADD8)
![storage](https://img.shields.io/badge/store-content--addressed%20%2B%20Postgres%20CAS-bf5e3c)

---

## 三条核心不变量

整套系统都由这三条派生而来:

1. **对象不可变、按内容指纹寻址。** 文件内容、目录树、提交,都以"自身字节的指纹"命名。
   你永远不改一个对象,只新增——正是这一点让"按指纹缓存、去重、安全垃圾回收"成为可能。
2. **唯一可变的指针,由 CAS 守护。** 唯一会变的是 `agent_id → HEAD`(当前是哪个提交),
   放在 Postgres 里。每次写入都用一次原子 CAS 来挪它:*只有当它还停在我出发那一版才更新,
   否则这次写入落败、重基重来。* 所有并发、排序、一致性都汇聚到这一处。
3. **其余一切都是可重建、可丢弃的派生视图。** 缓存、搜索索引、worker 的工作副本,都能从
   权威的对象 + 指针重新算出来,因此它们无状态、可随时扔。

## 架构

```mermaid
flowchart TB
  Client["客户端"] --> Router["Session 路由<br/>每个 agent 一个写者"]
  Router --> Worker["agent worker<br/>无状态,跑 agent 循环"]
  Worker --> LLM["LLM provider"]
  Worker --> Cache["按指纹的读缓存"]
  Worker --> Search["搜索索引"]
  Worker -->|"先写对象,再 CAS 改指针 — 同一事务"| Obj["对象存储<br/>内容寻址,不可变"]
  Worker --> PG["Postgres<br/>HEAD 经 CAS + 任务队列"]
  PG -->|"提交成功才入队"| Maint["维护 worker<br/>GC · 回收 · defrag · 消化队列"]
  Maint --> Obj
  Maint --> PG
```

四个区:**请求路径**在前台跑 agent;**权威存储**(对象 + Postgres)是唯一事实来源;
**读加速器**(缓存 + 搜索)和**维护 worker**都是它的派生,可随时重建。

## 一层层盖起来

每一层都独立设计、计划、审查,再合并。设计文档与实现计划在
[`docs/superpowers/`](docs/superpowers)。

| 层 | 加了什么 | 保证 |
|------|------|-----------|
| **L1** | 存储内核:内容寻址对象 + Postgres refs/CAS | 对象不可变 · 唯一序列化点 · 先对象后指针 |
| **L2** | Agent 循环 + 多轮会话 | 每 agent 单写者 · provider 无关 · 主动召回 |
| **L3** | 按指纹的读缓存 | 命中即等于重算;"失效"只是 LRU 淘汰 |
| **L4** | 混合搜索(trigram + 语义 + RRF) | 两种检索融合;算向量的服务挂了也能降级 |
| **L4b** | 向量持久化 + 增量补算 | 昂贵向量只算一次、跨会话共享;与 GC 隔离 |
| **L5a** | 维护 GC | 对可达对象做 mark-sweep + 龄期宽限 |
| **L5b** | `memory_jobs` 消费 + 反思 | SKIP LOCKED 消费 · 按 agent 单例 · 不自触发死循环 |
| **L5c** | 确定性 defrag | 按标题拆大文件,且可证明收敛 |
| **L5d** | 回收卡死的 running 任务 | 捞回被崩溃 worker 搁浅的任务 |

## 目录结构

```
cmd/
  api/            请求路径 dev harness(路由 + agent worker)
  maintenance/    后台 worker(GC · 回收 · defrag 扫描 · 消化队列)
internal/
  memstore/       权威存储
    objstore/       内容寻址对象后端(本地 | S3 风格)
    refs/           Postgres refs + CAS + 任务队列 + 迁移
    gitfs/          go-git Storer over objstore;物化工作树
  cache/          按指纹的读缓存(LRU)+ 持久 ObjCache + Tiered
  search/         trigram + 语义向量 + RRF 融合
  agent/          agent 循环:装配上下文、工具(recall/read/edit)、提交
  maintenance/    反思 / 补算 / defrag / GC / 回收
docs/
  architecture.md     完整设计(权威参考)
  onboarding.md       新人通俗导览
  reports/            六份图文深入解析报告(中文,自包含 HTML)
  superpowers/        每层的设计文档 + 实现计划
```

## 快速上手

```bash
# 构建
go build ./...

# 依赖 Postgres 的测试需要一个数据库:
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
ENGRAM_TEST_DB="$ENGRAM_TEST_DB" go test ./...

# 跑请求路径 dev harness(一个交互会话,读 stdin)
ENGRAM_PROVIDER=fake go run ./cmd/api

# 跑维护 worker(每轮 GC + 回收 + defrag 扫描 + 消化队列)
go run ./cmd/maintenance
```

完整环境变量见 [`CLAUDE.md`](CLAUDE.md)。

## 深入阅读

- **[`docs/architecture.md`](docs/architecture.md)** —— 权威设计,带图。
- **[六份图文深入解析报告](https://ssyhape-pixel.github.io/engram/)**(已上 GitHub Pages,
  按浏览器语言自动跳;也可直接看 [`docs/reports/cn/`](docs/reports/cn/index.html) 中文 /
  [`docs/reports/en/`](docs/reports/en/index.html) English):架构总览、请求路径、存储与一致性、
  搜索与索引、维护管线、构建历程。纯手工 SVG/HTML、零依赖、离线可读。
- **[`docs/onboarding.md`](docs/onboarding.md)** —— 给新人的通俗导览。

## 技术栈

Go 1.25(重 stdlib、小接口、处处 `context.Context`、表驱动测试)· `pgx` 连 Postgres ·
`golang-migrate`(expand-contract 迁移)· `go-git` + 自定义 Storer over 对象后端 ·
对象存储藏在接口后(dev 用本地文件,prod 用 S3/OSS 风格)。

## 现状

规划的路线(L1 → L5d)已全部完成并合并;全套测试(串行)通过,并发模块的竞态检测干净。
唯一刻意推后的是向量库的淘汰——向量是派生的、可重算、体量小,目前没有压力。

## 许可证

源码可见,采用 [PolyForm Noncommercial License 1.0.0](LICENSE.md):可阅读、学习、用于**非商业**目的;
**商业使用权保留**——需要商用授权请联系作者。
