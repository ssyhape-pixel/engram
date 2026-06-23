// Package objstore is the content-addressed object backend. Keys are object
// hashes; values are immutable bytes. Implementations must treat Put as
// idempotent.
package objstore

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Get when a key is absent.
var ErrNotFound = errors.New("objstore: not found")

// ObjStore stores immutable, content-addressed objects.
type ObjStore interface {
	Has(ctx context.Context, key string) (bool, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error // idempotent
	Iter(ctx context.Context, fn func(key string) error) error
	// Stat returns the object's last-modified time (creation time for our
	// immutable objects); missing key -> ErrNotFound.
	Stat(ctx context.Context, key string) (time.Time, error)
	// Delete removes an object; a missing key is not an error (idempotent).
	Delete(ctx context.Context, key string) error
}
