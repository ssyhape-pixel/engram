// Package cache is a SHA-keyed, invalidation-free read cache. Keys are content
// hashes (immutable), so a hit is always equivalent to recomputation and
// "invalidation" degrades to LRU eviction for space. One per-pod instance is
// shared across agents/sessions; identical resident content dedups naturally.
package cache

import (
	"container/list"
	"sync"
)

// Cache stores immutable string values keyed by a content hash.
type Cache interface {
	Get(key string) (string, bool)
	Put(key, val string)
}

const defaultMaxEntries = 1024

type entry struct{ key, val string }

// LRU is a size-bounded (by entry count), mutex-guarded Cache.
type LRU struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List // front = most-recently-used
	items      map[string]*list.Element
}

// NewLRU builds an LRU holding at most maxEntries entries. maxEntries <= 0 uses
// a default cap.
func NewLRU(maxEntries int) *LRU {
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	return &LRU{maxEntries: maxEntries, ll: list.New(), items: make(map[string]*list.Element)}
}

func (l *LRU) Get(key string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		l.ll.MoveToFront(el)
		return el.Value.(*entry).val, true
	}
	return "", false
}

func (l *LRU) Put(key, val string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		l.ll.MoveToFront(el)
		el.Value.(*entry).val = val
		return
	}
	el := l.ll.PushFront(&entry{key: key, val: val})
	l.items[key] = el
	if l.ll.Len() > l.maxEntries {
		if oldest := l.ll.Back(); oldest != nil {
			l.ll.Remove(oldest)
			delete(l.items, oldest.Value.(*entry).key)
		}
	}
}

var _ Cache = (*LRU)(nil)
