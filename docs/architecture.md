# Cloud Agent Memory System — Architecture

Server-side, multi-tenant adaptation of MemFS (git-backed memory filesystem). This is the implementation reference; `CLAUDE.md` is the short operating guide. A rendered, diagram-rich version is in `architecture.html`.

## 1. Goals

Give an agent long-lived, self-editable memory in the cloud, with multi-tenant isolation, horizontal scalability, auditability, and controllable cost. Keep the moving parts minimal.

## 2. Invariants (the spine)

1. **objects immutable** — contents, trees, commits are content-addressed by their own hash. Enables caching by SHA, cross-version dedup, mark-sweep GC; a hash never maps to two different bytes.
2. **refs mutable + strongly consistent** — the only mutable pointer is `agent_id -> HEAD`, in Postgres. All concurrency/ordering/consistency funnel through an atomic CAS on it.
3. **everything else is rebuildable** — caches, the trigram index, working copies are derived from objects and therefore stateless and disposable.

## 3. Topology

```
┌─ (1) request path ──────────────────────────────────────────────┐
│  Session router ──▶ Agent worker ──▶ LLM provider                │
│  serialize/agent     stateless pod    inference                  │
└───────────────┬─────────────────────────────────┬───────────────┘
   read system/ │  recall (mid-loop tool)          │ commit (1 tx)
                ▼                                   │
┌─ (2) read accelerators ───────────────┐          │
│  Memory cache        Search service   │          │
│  keyed by SHA        trigram, local   │          │
└──────────┬──────────────────┬─────────┘          │
 materialize│        range GET │ (variant B only)   │
            ▼                  ▼                     │
┌─ (3) canonical store (authoritative) ────────────────────────────┐
│  Object storage                 Postgres                         │
│  content-addressed objects      refs (HEAD·CAS) + job queue      │
└───────────▲────────────────────────────▲────────────────────────┘
  worktree  │  GC               poll / dequeue │
            │                                  │
┌─ (4) maintenance — one Go worker ────────────────────────────────┐
│  reconcile loop · reflection · defrag · GC · reindex             │
└──────────────────────────────────────────────────────────────────┘
```

Flow legend: reads flow down through accelerators; the write path (commit) goes straight to the store; maintenance is async and off the critical path.

## 4. Components

### 4.1 Session router
Serializes writes per `agent_id` (sticky routing or a per-agent lock). This rebuilds the implicit serialization that a single human user provided on the client. **Single-writer**, not optimistic multi-writer — markdown auto-merge is lossy.

### 4.2 Agent worker (stateless)
Runs the agent loop. On turn start: resolve HEAD, materialize the working tree to local scratch (cached by SHA), assemble context. Holds file/bash/git tools pointed at the working copy. On turn end: commit. Local disk is a disposable cache; the pod can be killed/scaled anytime.

### 4.3 LLM provider
Pure inference. Receives the assembled context (resident `system/` + tree index + recalled regions). Stateless.

### 4.4 Memory cache (keyed by commit SHA)
Caches the **materialized** `system/` content + the memory tree index, so workers don't re-read object storage each turn.

- **Why SHA:** the commit SHA is content-addressed → `sha -> materialized` is an immutable mapping → the cache **never needs invalidation**; "invalidation" degrades to LRU/TTL eviction for space. New content = new SHA = new key = miss = repopulate.
- The mutable thing that needs freshness is the pointer `agent_id -> HEAD` (in Postgres), not the content cache.
- Optimization: key by the `system/` subtree hash, not the top-level commit, so a commit touching only non-system files doesn't bust the cache.
- Multi-tier (per-pod LRU + shared Redis) needs no coherence protocol because the key is immutable.

### 4.5 Search service (trigram, local)
Lexical/keyword recall — the "grep at scale" technology (zoekt/bleve/tantivy).

- **Variant A (default):** index stores file contents + positions; queries answered entirely from local shards, **zero object-storage access at query time**. Memory is small text, so the duplication is fine.
- **Variant B:** index stores only `(object_hash, byte_offset, len)`; query does index lookup then a small in-region Range GET. Offsets are stable because they point at immutable objects.
- The index is a derived view: rebuildable from objects, sharded by `agent_id`, cold agents loaded on demand. **Eventually consistent** — freshly written memory may be unsearchable for a few seconds; acceptable for recall.

## 5. Recall design (matching client `grep` in the cloud)

Client grep is cheap because compute is co-located with data: it scans locally and returns sub-file regions. The cloud separates them, so naive "materialize the whole file to the agent" pays twice: cross-region egress (worse for big/cross-region) and — usually bigger — context bloat + a second scan by the model.

Fix: **push search to the data, return only regions** (predicate pushdown, like ClickHouse pushing a filter to storage and reading only matching granules).

- Expose recall as a tool that returns matching line ranges, not whole files.
- Back it with the trigram index so you don't linear-scan object storage.
- Fetch actual bytes via Range GET (variant B) or answer from the index (variant A).
- Residual cost is honest: you can replicate grep's *behavior*, not its *zero-infrastructure* property — cloud grep costs an index + a service + reindex-on-commit. Start by lowering recall granularity from file to line-range (nearly free); add the index under load.

## 6. Canonical store

### 6.1 Object storage (content-addressed)
Each content/tree/commit stored as `objects/<hash>`, idempotent PUT. Content-addressing is the sweet spot for object storage: cheap, durable, dedupable. For strict tenant isolation use per-tenant key prefixes and forgo cross-tenant dedup (dedup leaks existence). Barely on the query path — reads are absorbed by cache/index; writes are idempotent.

### 6.2 Postgres (refs + job queue)
Holds `agent_id -> HEAD` and is the single point of CAS-based concurrency control. Also doubles as the job queue (this is what removes Kafka). See schema in §9.

**Write order: objects first, then ref.** A crash before CAS leaves only unreachable garbage (GC reclaims it), never a torn ref pointing at an incomplete commit.

## 7. Maintenance (one Go worker)

A separate Go process (not the request-serving pod). Tasks: **reflection** (review recent history, consolidate into memory), **defrag** (split large files, merge duplicates, restructure), **GC** (mark-sweep unreachable objects, pack small objects), **reindex** (incremental trigram update).

Two ways to get work:
- **Reconcile loop (lightest, default):** poll Postgres for agents whose HEAD advanced past `processed_sha`; the refs table is the work list; record the cursor when done. Crash-safe: re-derive work from state on restart. (k8s-controller pattern.)
- **River (when you want a real queue):** dequeue jobs enqueued in the commit tx; gives retries/backoff, periodic jobs, per-agent unique (singleton) — all in the same Postgres, no extra cluster.

Maintenance writes via **git worktree** (a separate working dir off the same repo), commits there, merges back — so the foreground worker keeps reading its SHA-pinned snapshot and is never blocked. Per-agent singleton via a lock / River unique job (never two defrags on one agent).

## 8. Data flows

### 8.1 Read (foreground)
`router (serialize) -> worker assembles context: system/ via SHA cache + tree index; recall via search (mid-loop tool) -> LLM`. Only on cache miss does it hit object storage.

### 8.2 Write (commit, the key simplification)
1. Write new content-addressed objects to object storage (idempotent).
2. In **one Postgres transaction**: CAS `HEAD: parent -> new` **and** enqueue maintenance job(s).

CAS + enqueue atomic ⇒ "job enqueued iff commit committed" ⇒ no Kafka, no outbox, no dual-write inconsistency. Worker returns immediately. On CAS conflict, reconcile (rebase/merge) and retry.

### 8.3 Maintenance (async)
Go worker pulls work (reconcile/River), runs reflection/defrag/GC in a worktree, updates the trigram index from `git diff old..new`. Off the user's critical path; reflection is often triggered on a **compaction event** (when context is about to be summarized) rather than every turn.

## 9. Postgres schema (sketch)

```sql
-- the authoritative mutable pointer + CAS target
CREATE TABLE agent_refs (
  agent_id    text PRIMARY KEY,
  head        text NOT NULL,            -- commit hash
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- CAS update (fails -> 0 rows -> caller reconciles & retries):
-- UPDATE agent_refs SET head=$new, updated_at=now()
--   WHERE agent_id=$id AND head=$expected_parent;

-- maintenance work; enqueued in the SAME tx as the CAS
CREATE TABLE memory_jobs (
  id          bigserial PRIMARY KEY,
  agent_id    text NOT NULL,
  kind        text NOT NULL,            -- 'reindex' | 'reflect' | 'defrag' | 'gc'
  from_sha    text,                     -- commit that triggered the job
  state       text NOT NULL DEFAULT 'pending',
  attempts    int  NOT NULL DEFAULT 0,
  created_at  timestamptz NOT NULL DEFAULT now()
);
-- dequeue with FOR UPDATE SKIP LOCKED for safe concurrent workers.
-- per-agent singleton: partial unique index on (agent_id, kind) WHERE state='pending'.

-- reconcile-loop cursor (alternative to a queue)
CREATE TABLE maintenance_cursor (
  agent_id      text NOT NULL,
  kind          text NOT NULL,
  processed_sha text NOT NULL,
  PRIMARY KEY (agent_id, kind)
);
```

## 10. Storage layout

- Objects: `objects/<hash>` (or `tenant/<tid>/objects/<hash>` for hard isolation). Immutable, idempotent PUT.
- Working trees: ephemeral, on worker scratch / tmpfs, keyed by SHA; rebuildable.
- Memory file format (inside the tree): markdown with YAML frontmatter; `description` (required) feeds the tree index; files under `system/` are always-resident, others are lazy-loaded.

### 10.1 Deployment storage requirements (what each layer actually needs)

Separate **authoritative durable storage** from **compute locality**:

- **Authoritative durable state needs only an object-storage bucket + Postgres** — no OS-pod persistence. The `ObjStore` contract is just key→bytes with **idempotent, per-key-atomic PUT** + `Get`/`Has`/`List` — exactly S3/OSS, so a bucket suffices (the dev `Local` backend's temp+rename is a filesystem-backend detail, not a contract requirement). Postgres is the one strongly-consistent service (the `agent_id→HEAD` CAS + job queue). One consistency assumption: the bucket must be **read-after-write strongly consistent** (S3 since 2020, OSS) so that objects written before a CAS are visible to a reader resolving the new HEAD.
- **Compute (agent worker) is stateless but, in the current implementation, requires a writable local filesystem / tmpfs** for the ephemeral working tree: `Router.Open` uses `os.MkdirTemp`, `gitfs.Materialize` checks out via go-git's `osfs`, and the toolset reads/writes/greps via `os.*` on that path. This scratch is disposable and rebuildable from objects — pods stay stateless; the disk is locality, not storage.
- **Removing the local-FS requirement is a clean future option (not yet done):** materialize into go-git's in-memory billy filesystem (`memfs`) and route the toolset through the billy `Filesystem` abstraction instead of `os.*`. That yields zero-local-disk, RAM-only workers (serverless-friendly), with the bucket + Postgres unchanged.

Minimal infra, current design: **object bucket + Postgres + stateless compute pods with disposable scratch.**

## 11. Core interface (Go sketch)

```go
type CommitHash string

var ErrCASConflict = errors.New("memstore: HEAD moved")

// MemStore is the authoritative store: content-addressed objects + a
// CAS-guarded HEAD ref. All durable writes go through it.
type MemStore interface {
    // ResolveHead returns the current HEAD for an agent (cheap ref read).
    ResolveHead(ctx context.Context, agentID string) (CommitHash, error)

    // Materialize checks out the tree at `at` into `dir` (a working copy).
    Materialize(ctx context.Context, agentID string, at CommitHash, dir string) error

    // CommitWithCAS writes objects for the working tree, builds a commit on
    // `parent`, and atomically advances HEAD parent->new while enqueueing jobs
    // in the same DB tx. Returns ErrCASConflict if HEAD moved.
    CommitWithCAS(ctx context.Context, agentID string, parent CommitHash, dir string, jobs []Job) (CommitHash, error)
}

// SystemCache serves the resident system/ + tree index, keyed by SHA.
type SystemCache interface {
    Get(ctx context.Context, agentID string, sha CommitHash) (*Resident, error) // immutable; miss -> rebuild
}

// Search is recall. Variant A answers locally; variant B may Range-GET objstore.
type Search interface {
    Recall(ctx context.Context, agentID, query string, k int) ([]Hit, error) // Hit = {path, lineStart, lineEnd, snippet}
    Reindex(ctx context.Context, agentID string, from, to CommitHash) error   // incremental, git diff driven
}
```

Reconcile loop (pseudocode):

```go
for {
    agents := refs.AgentsAdvancedSince(ctx, cursor) // HEAD != processed_sha
    for _, a := range agents {
        lock := perAgentLock(a.ID); if !lock.TryAcquire() { continue }
        wt := worktreeFor(a.ID, a.Head)             // separate working dir
        runReflectionDefragGC(ctx, wt)              // idempotent
        search.Reindex(ctx, a.ID, a.Processed, a.Head)
        wt.MergeBack(); cursor.Set(a.ID, a.Head)
        lock.Release()
    }
    sleep(pollInterval)
}
```

## 12. Why not Temporal / Kafka

- **Temporal** answers "who runs long, crash-resumable, multi-step workflows." Our maintenance jobs are idempotent, re-derivable from state, and short — accepting "retry from start" drops the need for durable mid-step resume, leaving only {queue/poll + retries + singleton + schedule}, all lightweight.
- **Kafka** answers "decouple the write path and fan out to many consumers." We have one consumer (the maintenance worker), and Postgres same-tx enqueue gives us decoupling without dual-write inconsistency.

Revisit only if jobs become genuinely long/multi-step with expensive mid-flight state (→ Temporal) or consumers multiply and need replay (→ Kafka).

## 13. Rollout

M1 walking skeleton → M2 SHA cache → M3 reconcile-loop maintenance → M4 trigram search + line-range recall → M5 River / warm sidecar under load. Detail in `CLAUDE.md`.

## 14. Open questions (decide as you build)

- Eventual-consistency window of the index vs read-your-writes (mitigation: just-written files are still in this turn's context; or reindex the foreground agent synchronously).
- Cross-tenant object dedup vs existence-leak isolation — pick per security requirements.
- Reflection trigger policy: every N turns vs on compaction event (prefer compaction). _(L5b currently enqueues `reflect` per commit, deduped by the partial-unique index — a placeholder for a real compaction trigger.)_
- Warm-sidecar threshold: when grep fidelity / latency justifies giving up pure statelessness.
- **Stale-`running` job reaper — RESOLVED (L5d).** `ClaimJob` stamps `claimed_at` (migration 000002); each maintenance round, before draining, `ReapStaleJobs` returns `running` jobs claimed longer ago than `ENGRAM_JOB_REAP_AFTER` (default 10m), or with a NULL `claimed_at`, back to `pending` — bumping `attempts` so a job that crashes the worker on every claim fails out at `maxAttempts` (poison-job protection). Assumes a single worker (reap-after ≫ any job's seconds-scale `running` window); multi-worker would need reap-after ≫ the longest job or a worker heartbeat. River remains a drop-in if richer semantics are wanted.

## 15. Best-practice cross-check & A/B candidates (2026 research)

A 2026 best-practice review (deep, multi-source, adversarially fact-checked) cross-checked the decisions above. Most are **validated**; two are real optimization targets, one is a deliberate-but-defensible bet, one is a future-risk flag. Recorded here as **alternatives to A/B against** — not decisions to adopt now. Numbers from vendor sources are flagged; claims that failed adversarial verification are listed in §15.6 so they are not cited as support.

### 15.1 Validated — keep as-is
- **Immutable objects in object storage + mutable ref CAS in a consistent store** mirrors lakeFS exactly: lakeFS keeps committed metadata in object storage but refs in a KV store and guards branch pointers with `SetIf()` CAS, *because object stores lack conditional-write primitives*. This is one-to-one with our `agent_id -> HEAD` CAS in Postgres. (verified 3-0)
- **Same-tx CAS + job enqueue (no Kafka/outbox)** is genuine best practice, not a shortcut: it prevents orphaned jobs from rolled-back txs and the commit/emit race that external brokers (Redis/Kafka) suffer. Matches our "job enqueued iff the commit committed." (verified 3-0)

### 15.2 Optimization target — retrieval (affects §4.5 / M4)
- 2026 consensus: **hybrid multi-signal fusion (semantic + keyword + entity) beats any single signal** (Mem0: "the combined score outperforms any individual signal"). Pure-lexical trigram is the most likely long-tail recall-quality weakness. BM25/trigram catches exact tokens (`auth.ts`); embeddings catch paraphrases ("login system"). (verified 3-0)
- **A/B candidate:** variant A (lexical-only, current) **vs** hybrid = trigram (exact/grep fidelity) + embeddings over the *same* already-indexed file contents. Cheap to add because variant A already stores contents. Keep the "agentic, mid-loop recall, not a pre-inference top-k dump" framing either way — that framing is shared by DiffMem (pure agentic shell exploration: grep/git log/git diff, no vector DB/embeddings/BM25). (verified 3-0; note: "DiffMem proves production viability" was *refuted* — treat as a reasonable bet, not a guarantee.)

### 15.3 Optimization target — maintenance queue (affects §7 / M5)
- A naive `SELECT … FOR UPDATE SKIP LOCKED` single queue table can degrade **super-linearly** under high worker counts (each worker skips dead/locked tuples left by others); one case hit 100% CPU on 80 cores. (verified 3-0) **Caveat:** the worst documented case was a *fixable misconfiguration* (an `ORDER BY` + cross-partition update-chain walking); dropping `ORDER BY` + adding jitter restored millisecond claims at 192 concurrent. So this is a trap to design around, not a Postgres-queue ceiling.
- **Operational levers (adopt when the queue is built):** no `ORDER BY` on the claim query; partition + aggressively autovacuum `memory_jobs`; prefer River (already the M5 plan, uses SKIP LOCKED since PG 9.5) or Que-style advisory-lock fetch (simple non-blocking SELECTs, readers don't block readers). **Single-writer-per-agent keeps write concurrency naturally low** — likely far below the danger zone (open question: does our aggregate commit rate ever approach it?).

### 15.4 Future-risk flag — storage substrate (not now)
- Git's per-file blob/tree object model does **not** scale to large/deep repos (Git-LFS exists for exactly this). But that is about large-file/binary/monorepo workloads and is **largely orthogonal to small prose-markdown memory today** — a future-risk flag, not a current defect. (verified 2-1, corroborated by GitHub/GitLab/Perforce docs)
- **A/B candidate (only if repos grow large or accumulate deep history):** keep the custom go-git Storer over `ObjStore` (current) **vs** a coarser content-addressed Merkle layer behind the *same* `ObjStore` interface — lakeFS-style Range/Meta-Range SSTables (vendor-reported ~500k random GetObject/sec on a 200M-object repo), or **Prolly Trees** (content-addressed B-trees; diff cost O(change) not O(dataset); used by Dolt/Noms). The `ObjStore` interface already isolates this; a migration would not touch upper layers. **Dolt itself is ruled out** as a substrate — it versions relational tables, not free-text; requires data-in-place import; impractical at PB scale. (verified 3-0)

### 15.5 Adjacent pole — structured/temporal-graph memory
- Zep/Graphiti add **temporal validity, fact invalidation, and entity relationships** that prose-markdown forgoes. Use-case-dependent, **not** unconditional SOTA (several vendor superiority claims failed verification — see §15.6). (medium confidence; source vendor-authored)
- **A/B candidate:** prose-markdown only (current) **vs** prose-markdown + a *derived* lightweight temporal/entity index for agents that need temporal reasoning. Weigh against the "memory is agentic prose, not a graph DB" philosophy — likely scope creep unless a use case demands it.

### 15.6 Caveats & refuted claims
- **Vendor-self-reported numbers** (lakeFS throughput, Zep, Mem0, River/Que blogs): principles independently corroborated, but specific figures carry marketing incentive.
- **Refuted in adversarial verification — do NOT cite as support:** Zep 94.8% > MemGPT on DMR (0-3); specific PG queue-library throughput rankings (0-3); pgmq anti-scaling at high workers (1-2); "DiffMem proves production viability" (0-3); specific temporal/multi-hop point gains (0-3); "RAG fundamentally cannot model temporal validity" (1-2).
- **Net:** L1 (MemStore core) is unaffected — its `ObjStore` abstraction and same-tx CAS are exactly what the review recommends. The actionable changes live in M4 (hybrid recall) and M5 (queue hygiene).
- **Sources:** lakeFS versioning-internals & metadata-KV design; Dolt storage-engine blog (Prolly Trees); Mem0 *State of AI Agent Memory 2026*; Zep/Graphiti paper (arXiv 2501.13956); River (brandur.org/river); Que (gist chanks/7585810); Postgres SKIP LOCKED thread (postgrespro 2505440); DiffMem (github Growth-Kinetics/DiffMem).
