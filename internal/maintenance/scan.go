package maintenance

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

// EnqueueDefrag scans every agent and idempotently enqueues a defrag job for any
// agent whose HEAD tree contains a splittable oversized file. Per-agent errors
// are logged and skipped (best-effort scan; never blocks the round). Enqueue is
// idempotent (EnqueueJob dedups against a pending job).
func EnqueueDefrag(ctx context.Context, r *refs.Refs, store memstore.MemStore, maxBytes int) error {
	ids, err := r.AllAgentIDs(ctx)
	if err != nil {
		return fmt.Errorf("maintenance: defrag scan list agents: %w", err)
	}
	for _, agentID := range ids {
		head, err := store.ResolveHead(ctx, agentID)
		if err != nil {
			log.Printf("defrag scan: resolve head %s: %v", agentID, err)
			continue
		}
		dir, err := os.MkdirTemp("", "engram-defragscan-*")
		if err != nil {
			log.Printf("defrag scan: scratch %s: %v", agentID, err)
			continue
		}
		if err := store.Materialize(ctx, agentID, head, dir); err != nil {
			os.RemoveAll(dir)
			log.Printf("defrag scan: materialize %s: %v", agentID, err)
			continue
		}
		splittable, err := dirHasSplittable(dir, maxBytes)
		os.RemoveAll(dir)
		if err != nil {
			log.Printf("defrag scan: walk %s: %v", agentID, err)
			continue
		}
		if splittable {
			if err := r.EnqueueJob(ctx, agentID, "defrag", string(head)); err != nil {
				log.Printf("defrag scan: enqueue %s: %v", agentID, err)
			}
		}
	}
	return nil
}
