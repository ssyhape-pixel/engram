# CLAUDE.md

Operating guide for this repository. Read this first, every session, before planning or writing code.

## What we are building

A **cloud-side, multi-tenant agent memory system**. It adapts MemFS (a git-backed memory filesystem, à la Letta) to a server environment: many agents, each owning a versioned memory repo, served by stateless workers. An agent can read its resident memory, recall the long tail on demand, and edit memory durably across sessions.

The design is **deliberately lightweight**: stateless compute + content-addressed object storage + a single Postgres as the coordination hub. We intentionally do **not** use Temporal or Kafka (see Guardrails).

Full design detail lives in `docs/architecture.md` (read it before implementing a component). A rendered, diagram-rich version is `docs/architecture.html` (for humans).

## Core invariants (non-negotiable — every design choice derives from these)

1. **objects are immutable.** File contents, trees, and commits are content-addressed by their own hash. Never mutate an object; only add new ones. This is what lets us cache by SHA, dedup, and GC.
2. **refs are mutable and strongly consistent.** The only mutable pointer is `agent_id -> HEAD`. It lives in Postgres. **All** concurrency, ordering, and consistency are funneled through an atomic CAS on this ref.
3. **everything else is a rebuildable derived view.** Caches, the search index, and worker working-copies can all be recomputed from the authoritative objects. Therefore they are stateless and disposable.

## Architecture at a glance

Four zones (detail in `docs/architecture.md`):

- **① request path** — `Session router` (serializes writes per agent) -> `Agent worker` (stateless pod, runs the agent loop) -> `LLM provider`.
- **② read accelerators** — `Memory cache` (keyed by commit SHA) + `Search service` (local trigram index).
- **③ canonical store** — `Object storage` (content-addressed objects) + `Postgres` (refs HEAD·CAS **and** job queue).
- **④ maintenance** — one `Go worker` (reconcile loop): reflection, defrag, GC, reindex. Writes via git worktree, never blocks the foreground.

## Key design decisions

- **Single-writer per agent.** The router serializes all writes for one agent (sticky routing or a per-agent lock). Do not implement optimistic multi-writer merge on prose markdown — it is lossy.
- **Commit is one Postgres transaction.** On commit: write new objects to object storage (idempotent), then in a **single** Postgres tx do the ref CAS **and** enqueue maintenance jobs. This replaces Kafka + outbox: a job is enqueued iff the commit committed.
- **Write order: objects first, then ref.** A crash before CAS leaves only unreachable garbage objects (GC cleans them), never a torn ref.
- **Search variant A (default).** The trigram index stores file contents, so queries are answered locally with zero object-storage access at query time. Memory is small text; the duplication is negligible.
- **Memory is agentic, not RAG.** Pre-inference we load only resident `system/` + a cheap tree index (filenames + descriptions). The long tail is pulled mid-loop by the model via recall/read tools. Do not turn recall into an automatic pre-inference top-k dump.

## Tech stack & conventions

- **Go.** `context.Context` as first arg on all I/O. Wrap errors with `%w`. No global mutable state. Small interfaces at package boundaries. Table-driven tests.
- **Postgres** for refs + CAS + job queue. Use `pgx`. Migrations via `golang-migrate` (expand-contract; never destructive in one step).
- **Object storage**: S3/OSS in prod, local filesystem backend for dev/tests. Always behind an interface.
- **git** as the working format only: use `go-git` with a custom `Storer` backed by our object store. Do not shell out to the `git` binary in services.
- Keep packages small and dependency-light. Prefer the standard library.

## Proposed repo layout

```
cmd/
  api/            # request-path service: router + agent-worker host
  maintenance/    # background Go worker (reconcile loop)
internal/
  memstore/       # MemStore: the authoritative store (see interface in docs)
    objstore/     # content-addressed object backend (s3 | local)
    refs/         # Postgres refs + CAS + job queue
    gitfs/        # go-git Storer over objstore; materialize working trees
  cache/          # SHA-keyed memory cache (system/ + tree index)
  search/         # trigram index: build + query
  agent/          # agent loop: assemble context, tools (recall/read/edit), commit
  maintenance/    # reflection / defrag / gc job implementations
docs/
  architecture.md
  architecture.html
CLAUDE.md
go.mod
```

## Build / test / run

> Not scaffolded yet — fill these in as the project takes shape, and keep them current.

```
# build
go build ./...
# test
go test ./...
# run the request-path API (dev)
go run ./cmd/api
# run the maintenance worker (dev)
go run ./cmd/maintenance
```

## Milestones — build in this order

Start at **M1** and get a walking skeleton committing durably before adding accelerators.

- **M1 — walking skeleton.** `MemStore` with a local-FS object backend + Postgres refs + CAS. An agent worker that materializes a working tree, lets the agent edit it, and commits in one tx (CAS + enqueue). Single-writer per agent. No cache, no search service; reindex/reflection stubbed. _Done when:_ an agent can read/edit/commit memory with a versioned, diffable history.
- **M2 — read cache.** SHA-keyed cache for `system/` + tree index (start with per-pod in-memory LRU).
- **M3 — maintenance worker.** Reconcile loop that finds agents whose HEAD advanced past `processed_sha`, runs reflection/defrag/GC in a git worktree, merges back.
- **M4 — search.** Trigram index (variant A) + a reindex job driven by the maintenance worker; expose recall as an agent tool returning line ranges.
- **M5 — scale.** Swap reconcile loop for River (same Postgres) if you want transactional enqueue + better retries; add a warm per-agent sidecar if grep fidelity demands it.

## Guardrails — do NOT

- **Do not add Temporal or Kafka.** The maintenance jobs are idempotent, re-derivable, and short; the reconcile loop + a cron + (later) River cover all needs. Only revisit if jobs become genuinely long/multi-step with expensive mid-flight state.
- **Do not mutate objects.** Append-only, content-addressed. Changing memory = new objects + a new commit.
- **Do not add concurrency control outside the ref CAS.** One serialization point.
- **Do not let `system/` grow unbounded.** It costs tokens every turn. Keep it small and curated; push the long tail to lazy-loaded files.
- **Do not treat cache / index / working-copy as a source of truth.** They are derived and disposable; the objects + ref are authoritative.
