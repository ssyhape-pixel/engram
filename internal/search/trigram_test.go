package search

import "testing"

func TestTrigramFindsLine(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{
		"a.md": []byte("first line\nhas needle here\nlast\n"),
		"b.md": []byte("nothing\n"),
	})
	hits := idx.Search("needle", 10)
	if len(hits) != 1 || hits[0].Path != "a.md" || hits[0].LineStart != 2 || hits[0].LineEnd != 2 {
		t.Fatalf("hits = %+v", hits)
	}
}

func TestTrigramShortQueryFallback(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{"a.md": []byte("ab\nxy\nab cd\n")})
	hits := idx.Search("ab", 10) // <3 chars: no trigrams -> scan all lines
	if len(hits) != 2 {
		t.Fatalf("short-query fallback hits = %+v", hits)
	}
}

func TestTrigramNoMatch(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{"a.md": []byte("abc\n")})
	if hits := idx.Search("zzz", 10); len(hits) != 0 {
		t.Fatalf("want no hits, got %+v", hits)
	}
}

func TestTrigramKTruncation(t *testing.T) {
	idx := BuildTrigram(map[string][]byte{"a.md": []byte("needle\nneedle\nneedle\n")})
	if hits := idx.Search("needle", 2); len(hits) != 2 {
		t.Fatalf("k truncation: got %d", len(hits))
	}
}
