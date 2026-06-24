// Command api is a dev harness for the Engram agent loop: it wires the L1
// MemStore + L2 Router + an LLM provider and runs a single interactive session
// for one agent, reading user messages from stdin. Not for production.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/agent"
	"github.com/ssy/engram/internal/cache"
	"github.com/ssy/engram/internal/memstore"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
	"github.com/ssy/engram/internal/search"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	dsn := env("ENGRAM_DB", "postgres://postgres:engram@localhost:5433/engram?sslmode=disable")
	objRoot := env("ENGRAM_OBJ", "./engram-objects")
	agentID := env("ENGRAM_AGENT", "demo")

	if err := refs.Migrate(dsn); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	store := memstore.New(objstore.NewLocal(objRoot), refs.New(pool))
	if _, err := store.CreateAgent(ctx, agentID, map[string]string{
		"system/about.md": "---\ndescription: who this agent is\n---\nYou are a memory-keeping agent.\n",
	}); err != nil && !errors.Is(err, memstore.ErrAgentAlreadyExists) {
		log.Fatalf("create agent: %v", err)
	}

	providerName := env("ENGRAM_PROVIDER", "fake")
	var prov agent.LLMProvider
	switch providerName {
	case "anthropic":
		key := os.Getenv("ANTHROPIC_API_KEY")
		if key == "" {
			log.Fatal("ENGRAM_PROVIDER=anthropic requires ANTHROPIC_API_KEY")
		}
		prov = agent.NewAnthropic(key)
	default:
		prov = &agent.FakeProvider{Steps: []func(agent.Request) agent.Response{
			func(r agent.Request) agent.Response { return agent.Response{Text: "(fake) received your message"} },
		}}
	}

	var emb search.Embedder
	switch env("ENGRAM_EMBEDDER", "fake") {
	case "voyage":
		vkey := os.Getenv("VOYAGE_API_KEY")
		if vkey == "" {
			log.Fatal("ENGRAM_EMBEDDER=voyage requires VOYAGE_API_KEY")
		}
		emb = search.NewVoyage(vkey)
	default:
		emb = search.NewFakeEmbedder(256)
	}

	embObjRoot := env("ENGRAM_EMB_OBJ", "./engram-embeddings")
	if filepath.Clean(embObjRoot) == filepath.Clean(objRoot) {
		log.Fatalf("ENGRAM_EMB_OBJ and ENGRAM_OBJ must be different directories (got %q)", embObjRoot)
	}
	embCache := cache.NewTiered(cache.NewLRU(4096), cache.NewObjCache(objstore.NewLocal(embObjRoot)))
	router := agent.NewRouter(store, prov, os.TempDir(), cache.NewLRU(1024), embCache, emb)
	sess, err := router.Open(ctx, agentID)
	if err != nil {
		log.Fatalf("open session: %v", err)
	}
	defer sess.Close()

	fmt.Printf("engram session for agent %q (provider=%s). Type a message, Ctrl-D to exit.\n", agentID, providerName)
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			break
		}
		reply, err := sess.Send(ctx, sc.Text())
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		fmt.Printf("%s\n", reply)
	}
	if err := sc.Err(); err != nil {
		log.Printf("stdin: %v", err)
	}
}
