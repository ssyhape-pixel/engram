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
