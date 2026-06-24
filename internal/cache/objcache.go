package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/ssy/engram/internal/memstore/objstore"
)

// ObjCache is a persistent, content-addressed Cache backed by an ObjStore. It is
// for embeddings (expensive to recompute); it MUST use an ObjStore instance that
// is separate from the GC'd git object store. All errors degrade to miss/no-op
// (the cache is best-effort: a failure means recompute, never incorrectness).
type ObjCache struct{ objs objstore.ObjStore }

func NewObjCache(objs objstore.ObjStore) *ObjCache { return &ObjCache{objs: objs} }

// safeKey maps an arbitrary cache key (which may contain ':' and base64 chars)
// to a flat, filesystem/bucket-safe object key.
func safeKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (o *ObjCache) Get(key string) (string, bool) {
	data, err := o.objs.Get(context.Background(), safeKey(key))
	if err != nil {
		return "", false
	}
	return string(data), true
}

func (o *ObjCache) Put(key, val string) {
	_ = o.objs.Put(context.Background(), safeKey(key), []byte(val))
}

var _ Cache = (*ObjCache)(nil)
