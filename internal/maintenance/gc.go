// Package maintenance holds the off-critical-path maintenance operations. GC is
// a global mark-sweep: delete objects that are both unreachable and older than a
// grace period (the grace guards in-flight commits whose objects are written
// before their ref CAS). It is a pure function over an ObjStore + a precomputed
// reachability set, so it is fully testable with an in-memory store.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ssy/engram/internal/memstore/objstore"
)

// Stats reports the outcome of a GC sweep.
type Stats struct {
	Scanned int
	Swept   int
	Kept    int
}

// GC deletes objects that are unreachable AND older than grace (measured against
// now). Callers MUST pass a COMPLETE reachable set — never call GC if reachability
// computation failed. Per-object Delete failures are best-effort (kept, not
// aborted). Returns an error only if iterating the store itself fails.
func GC(ctx context.Context, objs objstore.ObjStore, reachable map[string]struct{}, grace time.Duration, now time.Time) (Stats, error) {
	var s Stats
	// Snapshot keys before mutating (deleting during Iter is unsafe for some backends).
	var keys []string
	if err := objs.Iter(ctx, func(k string) error {
		keys = append(keys, k)
		return nil
	}); err != nil {
		return s, fmt.Errorf("maintenance: iter objects: %w", err)
	}
	for _, k := range keys {
		s.Scanned++
		if _, ok := reachable[k]; ok {
			s.Kept++
			continue
		}
		mt, err := objs.Stat(ctx, k)
		if err != nil {
			if errors.Is(err, objstore.ErrNotFound) {
				continue // already gone since the snapshot
			}
			s.Kept++ // stat error: be conservative, keep
			continue
		}
		if now.Sub(mt) > grace {
			if err := objs.Delete(ctx, k); err != nil {
				s.Kept++ // delete failed: keep (best effort)
				continue
			}
			s.Swept++
		} else {
			s.Kept++
		}
	}
	return s, nil
}
