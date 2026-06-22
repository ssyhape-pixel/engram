package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestLRUPutGet(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", "1")
	if v, ok := c.Get("a"); !ok || v != "1" {
		t.Fatalf("get a = %q,%v", v, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("missing should miss")
	}
}

func TestLRUEvictsOldest(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", "1")
	c.Put("b", "2")
	c.Put("c", "3") // evicts a (least-recently-used)
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should be evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b should remain")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("c should remain")
	}
}

func TestLRUGetPromotesRecency(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", "1")
	c.Put("b", "2")
	c.Get("a")      // a becomes most-recent
	c.Put("c", "3") // evicts b (now LRU)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted after a was promoted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should remain")
	}
}

func TestLRUDefaultOnNonPositive(t *testing.T) {
	c := NewLRU(0) // <=0 => default cap (1024)
	for i := 0; i < 2000; i++ {
		c.Put(fmt.Sprintf("k%d", i), "v")
	}
	if _, ok := c.Get("k1999"); !ok {
		t.Fatal("latest entry should be present under default cap")
	}
	if _, ok := c.Get("k0"); ok {
		t.Fatal("oldest entry should be evicted under default cap")
	}
}

func TestLRUConcurrent(t *testing.T) {
	c := NewLRU(64)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("k%d", (g*500+i)%128)
				c.Put(k, "v")
				c.Get(k)
			}
		}(g)
	}
	wg.Wait()
}
