package objstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Local is a filesystem-backed ObjStore. Objects live at
// <root>/objects/<key[:2]>/<key>, sharded to avoid huge directories.
type Local struct {
	root string
}

func NewLocal(root string) *Local { return &Local{root: root} }

func (l *Local) path(key string) string {
	if len(key) < 2 {
		return filepath.Join(l.root, "objects", "_short", key)
	}
	return filepath.Join(l.root, "objects", key[:2], key)
}

func (l *Local) Has(ctx context.Context, key string) (bool, error) {
	_, err := os.Stat(l.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("objstore: stat %s: %w", key, err)
	}
	return true, nil
}

func (l *Local) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := os.ReadFile(l.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("objstore: read %s: %w", key, err)
	}
	return data, nil
}

// Put writes data idempotently. If the key already exists it is a no-op
// (objects are immutable and content-addressed, so identical key => identical
// bytes). Writes go to a temp file then rename for atomic visibility.
func (l *Local) Put(ctx context.Context, key string, data []byte) error {
	p := l.path(key)
	// Check-then-write race is safe: objects are content-addressed and
	// immutable, so an identical key implies identical bytes; the last atomic
	// rename always wins with the correct contents.
	if _, err := os.Stat(p); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("objstore: mkdir %s: %w", key, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return fmt.Errorf("objstore: temp %s: %w", key, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("objstore: write %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("objstore: close %s: %w", key, err)
	}
	if err := os.Rename(tmpName, p); err != nil {
		return fmt.Errorf("objstore: rename %s: %w", key, err)
	}
	return nil
}

func (l *Local) Iter(ctx context.Context, fn func(key string) error) error {
	base := filepath.Join(l.root, "objects")
	return filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("objstore: walk: %w", err)
		}
		if d.IsDir() {
			return nil
		}
		// With the <key[:2]>/<key> layout the file basename equals the key.
		return fn(d.Name())
	})
}

func (l *Local) Stat(ctx context.Context, key string) (time.Time, error) {
	fi, err := os.Stat(l.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return time.Time{}, ErrNotFound
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("objstore: stat %s: %w", key, err)
	}
	return fi.ModTime(), nil
}

func (l *Local) Delete(ctx context.Context, key string) error {
	if err := os.Remove(l.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("objstore: delete %s: %w", key, err)
	}
	return nil
}

var _ ObjStore = (*Local)(nil)
