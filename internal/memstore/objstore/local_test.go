package objstore

import (
	"context"
	"errors"
	"testing"
)

func newLocal(t *testing.T) *Local {
	t.Helper()
	return NewLocal(t.TempDir())
}

func TestLocalPutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	if err := s.Put(ctx, "abc123", []byte("hello")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.Get(ctx, "abc123")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q want %q", got, "hello")
	}
}

func TestLocalPutIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	if err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("second put should be idempotent: %v", err)
	}
}

func TestLocalHas(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	ok, err := s.Has(ctx, "missing")
	if err != nil || ok {
		t.Fatalf("Has(missing) = %v,%v want false,nil", ok, err)
	}
	if err := s.Put(ctx, "present", []byte("x")); err != nil {
		t.Fatal(err)
	}
	ok, err = s.Has(ctx, "present")
	if err != nil || !ok {
		t.Fatalf("Has(present) = %v,%v want true,nil", ok, err)
	}
}

func TestLocalGetNotFound(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	_, err := s.Get(ctx, "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v want ErrNotFound", err)
	}
}

func TestLocalIter(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	if err := s.Put(ctx, "aa", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "bb", []byte("2")); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	err := s.Iter(ctx, func(key string) error { seen[key] = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !seen["aa"] || !seen["bb"] {
		t.Fatalf("iter saw %v", seen)
	}
}
