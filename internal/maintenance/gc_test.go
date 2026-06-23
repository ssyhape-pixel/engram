package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/ssy/engram/internal/memstore/objstore"
)

// memObj is an in-memory ObjStore with settable mtimes, for GC tests.
type memObj struct {
	data map[string][]byte
	mt   map[string]time.Time
}

func newMemObj() *memObj { return &memObj{data: map[string][]byte{}, mt: map[string]time.Time{}} }

func (m *memObj) Has(ctx context.Context, k string) (bool, error) { _, ok := m.data[k]; return ok, nil }
func (m *memObj) Get(ctx context.Context, k string) ([]byte, error) {
	d, ok := m.data[k]
	if !ok {
		return nil, objstore.ErrNotFound
	}
	return d, nil
}
func (m *memObj) Put(ctx context.Context, k string, d []byte) error {
	m.data[k] = d
	if m.mt[k].IsZero() {
		m.mt[k] = time.Now()
	}
	return nil
}
func (m *memObj) Iter(ctx context.Context, fn func(string) error) error {
	for k := range m.data {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}
func (m *memObj) Stat(ctx context.Context, k string) (time.Time, error) {
	t, ok := m.mt[k]
	if !ok {
		return time.Time{}, objstore.ErrNotFound
	}
	return t, nil
}
func (m *memObj) Delete(ctx context.Context, k string) error {
	delete(m.data, k)
	delete(m.mt, k)
	return nil
}

var _ objstore.ObjStore = (*memObj)(nil)

func TestGCSweepsOldOrphansKeepsReachableAndFresh(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	m := newMemObj()
	put := func(k string, age time.Duration) { m.data[k] = []byte("x"); m.mt[k] = now.Add(-age) }
	put("reach", 48*time.Hour)        // reachable + very old -> KEEP
	put("oldorphan", 2*time.Hour)     // unreachable + older than grace -> SWEEP
	put("freshorphan", 5*time.Minute) // unreachable + within grace -> KEEP

	reachable := map[string]struct{}{"reach": {}}
	stats, err := GC(ctx, m, reachable, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Scanned != 3 || stats.Swept != 1 || stats.Kept != 2 {
		t.Fatalf("stats = %+v want {Scanned:3 Swept:1 Kept:2}", stats)
	}
	if ok, _ := m.Has(ctx, "oldorphan"); ok {
		t.Fatal("old orphan must be swept")
	}
	if ok, _ := m.Has(ctx, "reach"); !ok {
		t.Fatal("reachable object must be kept")
	}
	if ok, _ := m.Has(ctx, "freshorphan"); !ok {
		t.Fatal("fresh orphan must be kept (within grace)")
	}
}

func TestGCEmptyStore(t *testing.T) {
	stats, err := GC(context.Background(), newMemObj(), map[string]struct{}{}, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stats != (Stats{}) {
		t.Fatalf("empty store stats = %+v want zero", stats)
	}
}
