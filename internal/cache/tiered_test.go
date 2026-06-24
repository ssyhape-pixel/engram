package cache

import "testing"

func TestTieredFrontHit(t *testing.T) {
	front, back := NewLRU(8), NewLRU(8)
	front.Put("k", "front")
	tier := NewTiered(front, back)
	if g, ok := tier.Get("k"); !ok || g != "front" {
		t.Fatalf("front hit = %q %v", g, ok)
	}
}

func TestTieredBackHitPromotes(t *testing.T) {
	front, back := NewLRU(8), NewLRU(8)
	back.Put("k", "back")
	tier := NewTiered(front, back)
	if g, ok := tier.Get("k"); !ok || g != "back" {
		t.Fatalf("back hit = %q %v", g, ok)
	}
	if g, ok := front.Get("k"); !ok || g != "back" {
		t.Fatalf("front after promote = %q %v", g, ok)
	}
}

func TestTieredPutWritesBoth(t *testing.T) {
	front, back := NewLRU(8), NewLRU(8)
	NewTiered(front, back).Put("k", "v")
	if g, _ := front.Get("k"); g != "v" {
		t.Fatalf("front = %q", g)
	}
	if g, _ := back.Get("k"); g != "v" {
		t.Fatalf("back = %q", g)
	}
}

func TestTieredMiss(t *testing.T) {
	if _, ok := NewTiered(NewLRU(8), NewLRU(8)).Get("nope"); ok {
		t.Fatal("should miss")
	}
}
