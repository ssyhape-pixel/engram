package gitfs

import (
	"context"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/ssy/engram/internal/memstore/objstore"
)

func TestStorageBlobRoundTrip(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	st := NewStorage(ctx, objs)

	o := st.NewEncodedObject()
	o.SetType(plumbing.BlobObject)
	content := []byte("hello memory")
	o.SetSize(int64(len(content)))
	w, err := o.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatal(err)
	}
	w.Close()

	h, err := st.SetEncodedObject(o)
	if err != nil {
		t.Fatalf("set: %v", err)
	}

	st2 := NewStorage(ctx, objs)
	got, err := st2.EncodedObject(plumbing.BlobObject, h)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Type() != plumbing.BlobObject {
		t.Fatalf("type = %v", got.Type())
	}
	r, _ := got.Reader()
	buf := make([]byte, len(content))
	r.Read(buf)
	if string(buf) != string(content) {
		t.Fatalf("content = %q want %q", buf, content)
	}
	if got.Hash() != h {
		t.Fatalf("hash mismatch: %v vs %v", got.Hash(), h)
	}
}

func TestStorageHasAndMissing(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())
	st := NewStorage(ctx, objs)

	missing := plumbing.NewHash("0000000000000000000000000000000000000000")
	if err := st.HasEncodedObject(missing); err != plumbing.ErrObjectNotFound {
		t.Fatalf("HasEncodedObject(missing) = %v want ErrObjectNotFound", err)
	}
	if _, err := st.EncodedObject(plumbing.AnyObject, missing); err != plumbing.ErrObjectNotFound {
		t.Fatalf("EncodedObject(missing) = %v want ErrObjectNotFound", err)
	}
}
