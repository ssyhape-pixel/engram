// Command maintenance is the off-critical-path maintenance worker. On a timer it
// acquires a global advisory lock and runs GC (mark-sweep of unreachable objects
// older than the grace period) across all agents, then drains pending memory_jobs
// (reflection, reindex) for each round.
package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/agent"
	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/maintenance"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
	"github.com/ssy/engram/internal/search"
)

// providerCompleter adapts an agent.LLMProvider to maintenance.Completer (a
// single no-tools text completion).
type providerCompleter struct{ prov agent.LLMProvider }

func (p providerCompleter) Complete(ctx context.Context, system, user string) (string, error) {
	resp, err := p.prov.Generate(ctx, agent.Request{
		System:   system,
		Messages: []agent.Message{{Role: agent.RoleUser, Text: user}},
	})
	if err != nil {
		return "", err
	}
	return resp.Text, nil
}

// fixedCompleter is the dev reflection backend: a constant consolidation with
// no per-call limit (unlike a scripted FakeProvider).
type fixedCompleter struct{}

func (fixedCompleter) Complete(ctx context.Context, system, user string) (string, error) {
	return "(fake reflection) consolidated\n", nil
}

var _ maintenance.Completer = fixedCompleter{}
var _ maintenance.Completer = providerCompleter{}

const gcLockKey int64 = 1

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dur(key, def string) time.Duration {
	d, err := time.ParseDuration(env(key, def))
	if err != nil {
		log.Fatalf("invalid duration for %s: %v", key, err)
	}
	return d
}

func main() {
	ctx := context.Background()
	dsn := env("ENGRAM_DB", "postgres://postgres:engram@localhost:5433/engram?sslmode=disable")
	objRoot := env("ENGRAM_OBJ", "./engram-objects")
	interval := dur("ENGRAM_GC_INTERVAL", "5m")
	grace := dur("ENGRAM_GC_GRACE", "1h")

	if err := refs.Migrate(dsn); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	r := refs.New(pool)
	objs := objstore.NewLocal(objRoot)
	store := memstore.New(objs, r)

	maxAttempts := 5
	if v := os.Getenv("ENGRAM_JOB_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("invalid ENGRAM_JOB_MAX_ATTEMPTS: %q", v)
		}
		maxAttempts = n
	}

	var completer maintenance.Completer
	switch env("ENGRAM_PROVIDER", "fake") {
	case "anthropic":
		key := os.Getenv("ANTHROPIC_API_KEY")
		if key == "" {
			log.Fatal("ENGRAM_PROVIDER=anthropic requires ANTHROPIC_API_KEY")
		}
		completer = providerCompleter{prov: agent.NewAnthropic(key)}
	default:
		completer = fixedCompleter{}
	}

	var emb search.Embedder
	switch env("ENGRAM_PROVIDER", "fake") {
	case "anthropic", "voyage":
		key := os.Getenv("VOYAGE_API_KEY")
		if key == "" {
			log.Fatal("ENGRAM_PROVIDER=anthropic|voyage requires VOYAGE_API_KEY for reindex embeddings")
		}
		emb = search.NewVoyage(key)
	default:
		emb = search.NewFakeEmbedder(0)
	}
	embObjRoot := env("ENGRAM_EMB_OBJ", "./engram-embeddings")
	if filepath.Clean(embObjRoot) == filepath.Clean(objRoot) {
		log.Fatal("ENGRAM_EMB_OBJ must differ from ENGRAM_OBJ (GC must never sweep the embedding store)")
	}
	embCache := cache.NewTiered(cache.NewLRU(4096), cache.NewObjCache(objstore.NewLocal(embObjRoot)))

	log.Printf("maintenance worker started: interval=%s grace=%s obj=%s", interval, grace, objRoot)
	for {
		ran, err := r.WithGlobalLock(ctx, gcLockKey, func(ctx context.Context) error {
			heads, err := r.AllHeads(ctx)
			if err != nil {
				return err
			}
			reachable, err := gitfs.ReachableObjects(ctx, objs, heads)
			if err != nil {
				return err // do NOT GC against a partial reachable set
			}
			stats, err := maintenance.GC(ctx, objs, reachable, grace, time.Now())
			if err != nil {
				return err
			}
			log.Printf("gc: agents=%d scanned=%d swept=%d kept=%d statErrors=%d delErrors=%d",
				len(heads), stats.Scanned, stats.Swept, stats.Kept, stats.StatErrors, stats.DelErrors)
			processed, derr := maintenance.DrainJobs(ctx, r, store, completer, emb, embCache, maxAttempts)
			if derr != nil {
				return derr
			}
			log.Printf("jobs: processed=%d", processed)
			return nil
		})
		if err != nil {
			log.Printf("gc round error: %v", err)
		} else if !ran {
			log.Printf("gc: another worker holds the lock; skipping this round")
		}
		time.Sleep(interval)
	}
}
