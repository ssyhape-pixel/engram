// Package refs is the authoritative mutable pointer: agent_id -> HEAD, guarded
// by a single atomic CAS, plus same-transaction job enqueue. This is the only
// concurrency-control point in the system.
package refs

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrCASConflict   = errors.New("refs: HEAD moved")
	ErrAgentNotFound = errors.New("refs: agent not found")
)

// Job is a maintenance job to enqueue atomically with a commit.
type Job struct {
	Kind string // "reindex" | "reflect" | "defrag" | "gc"
}

// Refs manages the authoritative agent_id -> HEAD pointer in Postgres.
type Refs struct {
	pool *pgxpool.Pool
}

// New constructs a Refs from an existing connection pool.
func New(pool *pgxpool.Pool) *Refs { return &Refs{pool: pool} }

// ResolveHead returns the current HEAD sha for agentID.
// Returns ErrAgentNotFound if the agent has never been bootstrapped.
func (r *Refs) ResolveHead(ctx context.Context, agentID string) (string, error) {
	var head string
	err := r.pool.QueryRow(ctx, `SELECT head FROM agent_refs WHERE agent_id=$1`, agentID).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrAgentNotFound
	}
	if err != nil {
		return "", fmt.Errorf("refs: resolve %s: %w", agentID, err)
	}
	return head, nil
}

// Bootstrap registers a new agent at head. No-op if it already exists.
func (r *Refs) Bootstrap(ctx context.Context, agentID, head string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO agent_refs (agent_id, head) VALUES ($1,$2) ON CONFLICT (agent_id) DO NOTHING`,
		agentID, head)
	if err != nil {
		return fmt.Errorf("refs: bootstrap %s: %w", agentID, err)
	}
	return nil
}

// AllHeads returns every agent's current HEAD — the reachability roots for GC.
func (r *Refs) AllHeads(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT head FROM agent_refs`)
	if err != nil {
		return nil, fmt.Errorf("refs: all heads: %w", err)
	}
	defer rows.Close()
	var heads []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, fmt.Errorf("refs: scan head: %w", err)
		}
		heads = append(heads, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("refs: rows: %w", err)
	}
	return heads, nil
}

// WithGlobalLock tries a session-level Postgres advisory lock on key. If
// acquired it runs fn and releases the lock, returning ran=true; if another
// session already holds it, returns ran=false without running fn. Used so only
// one maintenance worker GCs at a time.
func (r *Refs) WithGlobalLock(ctx context.Context, key int64, fn func(context.Context) error) (bool, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("refs: acquire conn: %w", err)
	}
	defer conn.Release()

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		return false, fmt.Errorf("refs: try advisory lock: %w", err)
	}
	if !got {
		return false, nil
	}
	// Unlock with a background context: the advisory lock is session-scoped and
	// the pooled conn's session outlives this call, so the unlock MUST run even
	// if the request ctx was cancelled — otherwise the lock leaks on the conn.
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, key)
	}()

	if err := fn(ctx); err != nil {
		return true, err
	}
	return true, nil
}

// DequeuedJob is a claimed maintenance job.
type DequeuedJob struct {
	ID       int64
	AgentID  string
	Kind     string
	FromSHA  string
	Attempts int
}

// ClaimJob atomically claims one pending job (FOR UPDATE SKIP LOCKED, no ORDER
// BY — queue hygiene) and marks it 'running'. Returns nil if none pending.
func (r *Refs) ClaimJob(ctx context.Context) (*DequeuedJob, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("refs: begin claim: %w", err)
	}
	defer tx.Rollback(ctx)

	var j DequeuedJob
	var fromSHA *string
	err = tx.QueryRow(ctx,
		`SELECT id, agent_id, kind, from_sha, attempts FROM memory_jobs
		 WHERE state='pending' FOR UPDATE SKIP LOCKED LIMIT 1`).
		Scan(&j.ID, &j.AgentID, &j.Kind, &fromSHA, &j.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("refs: claim select: %w", err)
	}
	if fromSHA != nil {
		j.FromSHA = *fromSHA
	}
	if _, err := tx.Exec(ctx, `UPDATE memory_jobs SET state='running' WHERE id=$1`, j.ID); err != nil {
		return nil, fmt.Errorf("refs: claim mark running: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("refs: claim commit: %w", err)
	}
	return &j, nil
}

// CompleteJob removes a finished job.
func (r *Refs) CompleteJob(ctx context.Context, id int64) error {
	if _, err := r.pool.Exec(ctx, `DELETE FROM memory_jobs WHERE id=$1`, id); err != nil {
		return fmt.Errorf("refs: complete job %d: %w", id, err)
	}
	return nil
}

// RetryJob increments attempts; if attempts reaches maxAttempts the job is
// marked 'failed', otherwise returned to 'pending' for a later round.
func (r *Refs) RetryJob(ctx context.Context, id int64, maxAttempts int) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE memory_jobs
		 SET attempts = attempts + 1,
		     state = CASE WHEN attempts + 1 >= $2 THEN 'failed' ELSE 'pending' END
		 WHERE id=$1`, id, maxAttempts)
	if err != nil {
		return fmt.Errorf("refs: retry job %d: %w", id, err)
	}
	return nil
}

// InsertPendingJob enqueues a pending job directly (test support).
func (r *Refs) InsertPendingJob(ctx context.Context, agentID, kind, fromSHA string) error {
	_, err := r.pool.Exec(ctx, `INSERT INTO memory_jobs (agent_id, kind, from_sha) VALUES ($1,$2,$3)`, agentID, kind, fromSHA)
	if err != nil {
		return fmt.Errorf("refs: insert pending job: %w", err)
	}
	return nil
}

// CountJobs returns the number of memory_jobs rows for an agent (test support).
func (r *Refs) CountJobs(ctx context.Context, agentID string) (int, error) {
	var n int
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM memory_jobs WHERE agent_id=$1`, agentID).Scan(&n); err != nil {
		return 0, fmt.Errorf("refs: count jobs: %w", err)
	}
	return n, nil
}

// EnqueueJob idempotently enqueues a pending job out-of-band (not tied to a
// commit). ON CONFLICT against the partial-unique index makes a duplicate
// enqueue (an existing pending job for the same agent+kind) a no-op.
func (r *Refs) EnqueueJob(ctx context.Context, agentID, kind, fromSHA string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO memory_jobs (agent_id, kind, from_sha) VALUES ($1,$2,$3)
		 ON CONFLICT (agent_id, kind) WHERE state='pending' DO NOTHING`,
		agentID, kind, fromSHA)
	if err != nil {
		return fmt.Errorf("refs: enqueue job: %w", err)
	}
	return nil
}

// AllAgentIDs returns every agent id with a ref (for maintenance scans).
func (r *Refs) AllAgentIDs(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT agent_id FROM agent_refs`)
	if err != nil {
		return nil, fmt.Errorf("refs: all agent ids: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("refs: scan agent id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("refs: iterate agent ids: %w", err)
	}
	return ids, nil
}

// CommitRef atomically advances HEAD parent->next and enqueues jobs in ONE tx.
// Returns ErrCASConflict if HEAD != parent (0 rows updated). Note that a
// nonexistent agent also matches 0 rows and thus returns ErrCASConflict, so
// callers must Bootstrap the agent before the first CommitRef.
func (r *Refs) CommitRef(ctx context.Context, agentID, parent, next string, jobs []Job) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("refs: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	ct, err := tx.Exec(ctx,
		`UPDATE agent_refs SET head=$1, updated_at=now() WHERE agent_id=$2 AND head=$3`,
		next, agentID, parent)
	if err != nil {
		return fmt.Errorf("refs: cas: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrCASConflict
	}
	for _, j := range jobs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO memory_jobs (agent_id, kind, from_sha) VALUES ($1,$2,$3)
			 ON CONFLICT DO NOTHING`,
			agentID, j.Kind, next); err != nil {
			return fmt.Errorf("refs: enqueue %s: %w", j.Kind, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("refs: commit tx: %w", err)
	}
	return nil
}
