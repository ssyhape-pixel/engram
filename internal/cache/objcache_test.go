package cache

import (
	"testing"

	"github.com/ssy/engram/internal/memstore/objstore"
)

func TestObjCacheRoundTrip(t *testing.T) {
	c := NewObjCache(objstore.NewLocal(t.TempDir()))
	if _, ok := c.Get("emb:fake:abc"); ok {
		t.Fatal("empty cache should miss")
	}
	c.Put("emb:fake:abc", "vecdata")
	got, ok := c.Get("emb:fake:abc")
	if !ok || got != "vecdata" {
		t.Fatalf("get = %q %v, want vecdata true", got, ok)
	}
}

func TestObjCacheDistinctKeys(t *testing.T) {
	c := NewObjCache(objstore.NewLocal(t.TempDir()))
	c.Put("emb:fake:aaa", "v1")
	c.Put("emb:fake:bbb", "v2")
	if g, _ := c.Get("emb:fake:aaa"); g != "v1" {
		t.Fatalf("aaa = %q", g)
	}
	if g, _ := c.Get("emb:fake:bbb"); g != "v2" {
		t.Fatalf("bbb = %q", g)
	}
}

func TestObjCacheHandlesUnsafeKeyChars(t *testing.T) {
	c := NewObjCache(objstore.NewLocal(t.TempDir()))
	key := "emb:voyage-3:AB+/cd=="
	c.Put(key, "v")
	if g, ok := c.Get(key); !ok || g != "v" {
		t.Fatalf("unsafe key round-trip = %q %v", g, ok)
	}
}
