package cache

// Tiered composes two caches: a fast front (e.g. in-process LRU) over a durable
// back (e.g. ObjCache). Get checks front then back, promoting a back hit into
// front. Put writes both. Used for embeddings: per-process LRU over the
// persistent content-addressed store.
type Tiered struct{ front, back Cache }

func NewTiered(front, back Cache) *Tiered { return &Tiered{front: front, back: back} }

func (t *Tiered) Get(key string) (string, bool) {
	if v, ok := t.front.Get(key); ok {
		return v, true
	}
	if v, ok := t.back.Get(key); ok {
		t.front.Put(key, v)
		return v, true
	}
	return "", false
}

func (t *Tiered) Put(key, val string) {
	t.front.Put(key, val)
	t.back.Put(key, val)
}

var _ Cache = (*Tiered)(nil)
