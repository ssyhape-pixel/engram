package maintenance

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

// agentKey hashes an agent id to an advisory-lock key (per-agent reflection
// singleton). Collisions only cause two agents to occasionally serialize — safe.
func agentKey(agentID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(agentID))
	return int64(h.Sum64())
}

// processJob dispatches a single claimed job, always resolving it (CompleteJob or
// RetryJob) so the row never stays stuck in 'running'.
func processJob(ctx context.Context, r *refs.Refs, store memstore.MemStore, c Completer, job *refs.DequeuedJob, maxAttempts int) error {
	switch job.Kind {
	case "reflect":
		ran, err := r.WithGlobalLock(ctx, agentKey(job.AgentID), func(ctx context.Context) error {
			return Reflect(ctx, store, c, job.AgentID, job.FromSHA)
		})
		if !ran {
			// Another worker holds this agent's reflection lock; retry later.
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		if err != nil {
			// ErrConflict (agent advanced) or any reflection error → retry later.
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	case "reindex":
		// No persistent index yet (L4 rebuilds per session); drain to avoid pileup.
		return r.CompleteJob(ctx, job.ID)
	default:
		// Unknown kind: discard so it can't clog the queue.
		return r.CompleteJob(ctx, job.ID)
	}
}

// DrainJobs claims and processes pending jobs until none remain. A job re-claimed
// within the same round (requeued by processJob) is released and the round ends,
// so a perpetually-conflicting job can't busy-loop (it retries next round and
// eventually fails out via maxAttempts).
func DrainJobs(ctx context.Context, r *refs.Refs, store memstore.MemStore, c Completer, maxAttempts int) (int, error) {
	seen := map[int64]struct{}{}
	drained := 0
	for {
		job, err := r.ClaimJob(ctx)
		if err != nil {
			return drained, fmt.Errorf("maintenance: claim: %w", err)
		}
		if job == nil {
			return drained, nil
		}
		if _, dup := seen[job.ID]; dup {
			if err := r.RetryJob(ctx, job.ID, maxAttempts); err != nil {
				return drained, fmt.Errorf("maintenance: requeue %d: %w", job.ID, err)
			}
			return drained, nil
		}
		seen[job.ID] = struct{}{}
		if err := processJob(ctx, r, store, c, job, maxAttempts); err != nil {
			return drained, fmt.Errorf("maintenance: process job %d: %w", job.ID, err)
		}
		drained++
	}
}
