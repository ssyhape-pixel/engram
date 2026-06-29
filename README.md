# Engram

**English** · [简体中文](README.zh-CN.md)

**A cloud-side, multi-tenant memory system for AI agents.**

Engram gives every agent a versioned "notebook" it can read, search, and durably
edit across sessions. It adapts a git-backed memory filesystem to a stateless
server: **content-addressed immutable objects** for storage, a **single Postgres
compare-and-swap** for consistency, and a **background worker** that keeps each
agent's memory tidy — reflection, defrag, garbage collection, and crash recovery.

Memory here is *agentic, not RAG*: before inference the agent loads only a small
resident set plus a cheap index of what exists; the long tail is pulled mid-loop
by the model itself, via recall/read tools that return precise line ranges.

![status](https://img.shields.io/badge/status-L1–L5d%20complete-2ea44f)
![go](https://img.shields.io/badge/Go-1.25-00ADD8)
![storage](https://img.shields.io/badge/store-content--addressed%20%2B%20Postgres%20CAS-bf5e3c)

---

## The three invariants

Everything in the system derives from these:

1. **Objects are immutable and content-addressed.** File contents, trees, and
   commits are named by the hash of their own bytes. You never mutate an object —
   you only add new ones. This is what makes caching-by-hash, deduplication, and
   safe garbage collection possible.
2. **One mutable pointer, guarded by a compare-and-swap.** The only thing that
   changes is `agent_id → HEAD` (which commit is current), stored in Postgres.
   Every write moves it via an atomic CAS: *update only if it still points where
   I started; otherwise this write loses and re-bases.* All concurrency, ordering,
   and consistency funnel through this one point.
3. **Everything else is a rebuildable, disposable derived view.** Caches, the
   search index, and worker working-copies can all be recomputed from the
   authoritative objects + ref. So they are stateless and throwaway.

## Architecture

```mermaid
flowchart TB
  Client["client"] --> Router["Session router<br/>one writer per agent"]
  Router --> Worker["agent worker<br/>stateless, runs the loop"]
  Worker --> LLM["LLM provider"]
  Worker --> Cache["SHA read cache"]
  Worker --> Search["search index"]
  Worker -->|"objects first, then ref CAS — one tx"| Obj["object store<br/>content-addressed, immutable"]
  Worker --> PG["Postgres<br/>HEAD via CAS + job queue"]
  PG -->|"enqueue iff the commit committed"| Maint["maintenance worker<br/>GC · reap · defrag · drain"]
  Maint --> Obj
  Maint --> PG
```

Four zones: the **request path** runs the agent in the foreground; the
**canonical store** (objects + Postgres) is the single source of truth; the
**read accelerators** (cache + search) and the **maintenance worker** are
derived from it and can be rebuilt at any time.

## Built layer by layer

Each layer was designed, planned, and reviewed independently, then merged. Specs
and plans live in [`docs/superpowers/`](docs/superpowers).

| Layer | Adds | Guarantee |
|------|------|-----------|
| **L1** | MemStore core: content-addressed objects + Postgres refs/CAS | immutable objects · one serialization point · objects-first-then-ref |
| **L2** | Agent loop + multi-turn Session | single writer per agent · provider-agnostic · agentic recall |
| **L3** | SHA-keyed read cache | a hit always equals recompute; "invalidation" is just LRU eviction |
| **L4** | Hybrid search (trigram + semantic + RRF) | two retrieval modes fused; degrades gracefully if the embedder is down |
| **L4b** | Embedding persistence + incremental reindex | expensive vectors computed once, shared across sessions; GC-isolated |
| **L5a** | Maintenance GC | mark-sweep over reachable objects + an age grace period |
| **L5b** | `memory_jobs` consumer + reflection | SKIP LOCKED draining · per-agent singleton · no self-trigger loop |
| **L5c** | Deterministic defrag | splits oversized markdown at headings; provably converges |
| **L5d** | Stale-`running` job reaper | recovers jobs stranded by a crashed worker |

## Repository layout

```
cmd/
  api/            request-path dev harness (router + agent worker)
  maintenance/    background worker (GC · reap · defrag scan · drain jobs)
internal/
  memstore/       authoritative store
    objstore/       content-addressed object backend (local | S3-style)
    refs/           Postgres refs + CAS + job queue + migrations
    gitfs/          go-git Storer over objstore; materialize working trees
  cache/          SHA-keyed read cache (LRU) + persistent ObjCache + Tiered
  search/         trigram + semantic embeddings + RRF fusion
  agent/          agent loop: assemble context, tools (recall/read/edit), commit
  maintenance/    reflection / reindex / defrag / GC / reaper
docs/
  architecture.md     full design (the authoritative reference)
  onboarding.md       plain-language newcomer guide
  reports/            six illustrated deep-dive reports (中文, self-contained HTML)
  superpowers/        per-layer specs + implementation plans
```

## Quickstart

```bash
# build
go build ./...

# Postgres-backed tests need a database:
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
ENGRAM_TEST_DB="$ENGRAM_TEST_DB" go test ./...

# run the request-path dev harness (one interactive session, reads stdin)
ENGRAM_PROVIDER=fake go run ./cmd/api

# run the maintenance worker (GC + reap + defrag scan + drain jobs each round)
go run ./cmd/maintenance
```

Full environment-variable reference is in [`CLAUDE.md`](CLAUDE.md).

## Deep dives

- **[`docs/architecture.md`](docs/architecture.md)** — the authoritative design, with diagrams.
- **`docs/reports/index.html`** — six illustrated, self-contained deep-dive reports
  (architecture, request path, storage & consistency, search & index, maintenance,
  build journey). Hand-built SVG/HTML, no dependencies — open `index.html` locally,
  or enable GitHub Pages on `docs/` to read them online.
- **[`docs/onboarding.md`](docs/onboarding.md)** — a plain-language tour for newcomers.

## Tech stack

Go 1.25 (stdlib-heavy, small interfaces, `context.Context` everywhere, table-driven
tests) · `pgx` for Postgres · `golang-migrate` (expand-contract migrations) ·
`go-git` with a custom Storer over the object backend · object storage behind an
interface (local filesystem for dev, S3/OSS-style for prod).

## Status

The designed roadmap (L1 → L5d) is complete and merged; the full suite passes
(serialized) with the race detector clean on the concurrent packages. The one
deliberately deferred piece is embedding-store eviction — embeddings are derived,
rebuildable, and small, so there is no pressure yet.
