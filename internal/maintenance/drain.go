package maintenance

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/refs"
	"github.com/ssy/engram/internal/search"
)

// Deps carries the per-kind job-handler dependencies for DrainJobs.
type Deps struct {
	Store          memstore.MemStore
	Completer      Completer       // reflect
	Emb            search.Embedder // reindex
	EmbCache       cache.Cache     // reindex
	DefragMaxBytes int             // defrag
}

// agentKey hashes an agent id to an advisory-lock key (per-agent reflection
// singleton). Collisions only cause two agents to occasionally serialize — safe.
func agentKey(agentID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(agentID))
	// int64(h.Sum64()) may be negative; that's fine for an advisory-lock key.
	return int64(h.Sum64())
}

// processJob dispatches a single claimed job, always resolving it (CompleteJob or
// RetryJob) so the row never stays stuck in 'running'.
func processJob(ctx context.Context, r *refs.Refs, deps Deps, job *refs.DequeuedJob, maxAttempts int) error {
	switch job.Kind {
	case "reflect":
		ran, err := r.WithGlobalLock(ctx, agentKey(job.AgentID), func(ctx context.Context) error {
			return Reflect(ctx, deps.Store, deps.Completer, job.AgentID, job.FromSHA)
		})
		if !ran || err != nil {
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	case "reindex":
		if err := Reindex(ctx, deps.Store, deps.Emb, deps.EmbCache, job.AgentID); err != nil {
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	case "defrag":
		ran, err := r.WithGlobalLock(ctx, agentKey(job.AgentID), func(ctx context.Context) error {
			return Defrag(ctx, deps.Store, job.AgentID, deps.DefragMaxBytes)
		})
		if !ran || err != nil {
			return r.RetryJob(ctx, job.ID, maxAttempts)
		}
		return r.CompleteJob(ctx, job.ID)
	default:
		// Unknown kind: discard so it can't clog the queue.
		return r.CompleteJob(ctx, job.ID)
	}
}

// DrainJobs claims and processes pending jobs until none remain, returning the
// number of jobs processed this round (completed or requeued). A job re-claimed
// within the same round (it was requeued by processJob) is RetryJob'd again
// (attempts bumped a second time) and the round ends — so a perpetually-blocked
// job can't busy-loop and fails out via maxAttempts within a few rounds.
func DrainJobs(ctx context.Context, r *refs.Refs, deps Deps, maxAttempts int) (int, error) {
	seen := map[int64]struct{}{}
	processed := 0
	for {
		job, err := r.ClaimJob(ctx)
		if err != nil {
			return processed, fmt.Errorf("maintenance: claim: %w", err)
		}
		if job == nil {
			return processed, nil
		}
		if _, dup := seen[job.ID]; dup {
			if err := r.RetryJob(ctx, job.ID, maxAttempts); err != nil {
				return processed, fmt.Errorf("maintenance: requeue %d: %w", job.ID, err)
			}
			return processed, nil
		}
		seen[job.ID] = struct{}{}
		if err := processJob(ctx, r, deps, job, maxAttempts); err != nil {
			return processed, fmt.Errorf("maintenance: process job %d: %w", job.ID, err)
		}
		processed++
	}
}
