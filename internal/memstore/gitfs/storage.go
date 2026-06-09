// Package gitfs adapts go-git to Engram's content-addressed ObjStore: a custom
// EncodedObjectStorer writes blob/tree/commit objects natively into ObjStore,
// keyed by their git hash. Reference/config/index storage is in-memory and
// per-session — the authoritative ref lives in Postgres, not git.
package gitfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/ssy/engram/internal/memstore/objstore"
)

// Storage is a go-git storage.Storer whose objects live in an ObjStore.
type Storage struct {
	*memory.Storage // ref/config/index/shallow/module (object methods overridden below)
	objs            objstore.ObjStore
	ctx             context.Context
}

// NewStorage builds a per-session Storage. ctx scopes the underlying ObjStore
// I/O for this session.
func NewStorage(ctx context.Context, objs objstore.ObjStore) *Storage {
	return &Storage{Storage: memory.NewStorage(), objs: objs, ctx: ctx}
}

// NewEncodedObject returns a new MemoryObject ready for content to be written.
func (s *Storage) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{}
}

// frame encodes a git object in the loose-object format:
// "<type> <size>\x00<content>" — this is the byte sequence that git hashes,
// so sha1(frame(o)) == o.Hash().
func frame(o plumbing.EncodedObject) ([]byte, error) {
	r, err := o.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString(o.Type().String())
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatInt(o.Size(), 10))
	buf.WriteByte(0)
	buf.Write(content)
	return buf.Bytes(), nil
}

// unframe decodes a framed object back to (type, content).
func unframe(data []byte) (plumbing.ObjectType, []byte, error) {
	sp := bytes.IndexByte(data, ' ')
	nul := bytes.IndexByte(data, 0)
	if sp < 0 || nul < 0 || nul < sp {
		return plumbing.InvalidObject, nil, errors.New("gitfs: malformed object header")
	}
	t, err := plumbing.ParseObjectType(string(data[:sp]))
	if err != nil {
		return plumbing.InvalidObject, nil, err
	}
	return t, data[nul+1:], nil
}

// SetEncodedObject computes the object hash, frames the object, and stores it
// in ObjStore keyed by the hex hash.
func (s *Storage) SetEncodedObject(o plumbing.EncodedObject) (plumbing.Hash, error) {
	if o.Type() == plumbing.OFSDeltaObject || o.Type() == plumbing.REFDeltaObject {
		return plumbing.ZeroHash, plumbing.ErrInvalidType
	}
	h := o.Hash()
	data, err := frame(o)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("gitfs: frame %s: %w", h, err)
	}
	if err := s.objs.Put(s.ctx, h.String(), data); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("gitfs: put %s: %w", h, err)
	}
	return h, nil
}

// EncodedObject fetches an object by hash. Returns plumbing.ErrObjectNotFound
// if the hash is absent or the type does not match (unless t is AnyObject).
func (s *Storage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	data, err := s.objs.Get(s.ctx, h.String())
	if errors.Is(err, objstore.ErrNotFound) {
		return nil, plumbing.ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gitfs: get %s: %w", h, err)
	}
	typ, content, err := unframe(data)
	if err != nil {
		return nil, err
	}
	if t != plumbing.AnyObject && t != typ {
		return nil, plumbing.ErrObjectNotFound
	}
	o := &plumbing.MemoryObject{}
	o.SetType(typ)
	o.SetSize(int64(len(content)))
	if _, err := o.Write(content); err != nil {
		return nil, err
	}
	return o, nil
}

// HasEncodedObject returns nil if the object exists, plumbing.ErrObjectNotFound
// otherwise.
func (s *Storage) HasEncodedObject(h plumbing.Hash) error {
	ok, err := s.objs.Has(s.ctx, h.String())
	if err != nil {
		return fmt.Errorf("gitfs: has %s: %w", h, err)
	}
	if !ok {
		return plumbing.ErrObjectNotFound
	}
	return nil
}

// EncodedObjectSize returns the size of the object content (not including the
// frame header).
func (s *Storage) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	data, err := s.objs.Get(s.ctx, h.String())
	if errors.Is(err, objstore.ErrNotFound) {
		return 0, plumbing.ErrObjectNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("gitfs: size %s: %w", h, err)
	}
	_, content, err := unframe(data)
	if err != nil {
		return 0, err
	}
	return int64(len(content)), nil
}

// IterEncodedObjects returns an iterator over all objects of the given type.
// If t is AnyObject, all objects are returned.
func (s *Storage) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	var objs []plumbing.EncodedObject
	err := s.objs.Iter(s.ctx, func(key string) error {
		o, err := s.EncodedObject(t, plumbing.NewHash(key))
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			return nil // type filtered out or key not a valid hash
		}
		if err != nil {
			return err
		}
		objs = append(objs, o)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return storer.NewEncodedObjectSliceIter(objs), nil
}
