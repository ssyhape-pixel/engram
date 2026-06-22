package search

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/ssy/engram/internal/cache"
)

type chunk struct {
	path      string
	lineStart int // 1-based inclusive
	lineEnd   int
	snippet   string
	vec       []float32
}

// SemanticIndex holds per-section vectors for brute-force cosine retrieval.
type SemanticIndex struct {
	emb    Embedder
	chunks []chunk
}

type section struct {
	start, end int // 1-based inclusive line range
	text       string
}

func isHeading(line string) bool { return strings.HasPrefix(strings.TrimSpace(line), "#") }

// sectionize splits content into markdown sections: each heading line ("#...")
// after line 1 starts a new section; content before the first heading is its
// own section. No heading => one section covering the whole file.
func sectionize(content string) []section {
	lines := strings.Split(content, "\n")
	var secs []section
	start := 0 // 0-based index of current section start
	for i := 1; i < len(lines); i++ {
		if isHeading(lines[i]) {
			secs = append(secs, section{start: start + 1, end: i, text: strings.Join(lines[start:i], "\n")})
			start = i
		}
	}
	secs = append(secs, section{start: start + 1, end: len(lines), text: strings.Join(lines[start:], "\n")})
	return secs
}

func firstNonEmptyLine(text string) string {
	for _, l := range strings.Split(text, "\n") {
		if strings.TrimSpace(l) != "" {
			return l
		}
	}
	return ""
}

func embKey(model, text string) string {
	sum := sha256.Sum256([]byte(text))
	return "emb:" + model + ":" + base64.RawStdEncoding.EncodeToString(sum[:])
}

func encodeVec(v []float32) string {
	buf := make([]byte, 4*len(v))
	for i, x := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(x))
	}
	return base64.RawStdEncoding.EncodeToString(buf)
}

func decodeVec(s string) []float32 {
	buf, err := base64.RawStdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	v := make([]float32, len(buf)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
	}
	return v
}

func cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// BuildSemantic chunks files into sections and fills each section's vector,
// reusing the content-addressed embedding cache (c may be nil). Missing vectors
// are fetched in a single batched Embed call.
func BuildSemantic(ctx context.Context, emb Embedder, c cache.Cache, files map[string][]byte) (*SemanticIndex, error) {
	si := &SemanticIndex{emb: emb}
	type pending struct {
		ci   int
		text string
		key  string
	}
	var pend []pending

	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths) // stable chunk order

	for _, p := range paths {
		for _, sec := range sectionize(string(files[p])) {
			ci := len(si.chunks)
			si.chunks = append(si.chunks, chunk{path: p, lineStart: sec.start, lineEnd: sec.end, snippet: firstNonEmptyLine(sec.text)})
			key := embKey(emb.Model(), sec.text)
			if c != nil {
				if enc, ok := c.Get(key); ok {
					si.chunks[ci].vec = decodeVec(enc)
					continue
				}
			}
			pend = append(pend, pending{ci: ci, text: sec.text, key: key})
		}
	}

	if len(pend) > 0 {
		texts := make([]string, len(pend))
		for i, pp := range pend {
			texts[i] = pp.text
		}
		vecs, err := emb.Embed(ctx, texts)
		if err != nil {
			return nil, fmt.Errorf("search: embed sections: %w", err)
		}
		if len(vecs) != len(pend) {
			return nil, fmt.Errorf("search: embed returned %d vectors for %d sections", len(vecs), len(pend))
		}
		for i, pp := range pend {
			si.chunks[pp.ci].vec = vecs[i]
			if c != nil {
				c.Put(pp.key, encodeVec(vecs[i]))
			}
		}
	}
	return si, nil
}

// Search embeds the query and returns the top-k sections by cosine similarity.
func (s *SemanticIndex) Search(ctx context.Context, query string, k int) ([]Hit, error) {
	if k <= 0 {
		k = 8
	}
	qv, err := s.emb.Embed(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("search: embed query: %w", err)
	}
	if len(qv) != 1 {
		return nil, fmt.Errorf("search: embed query returned %d vectors", len(qv))
	}
	q := qv[0]
	scores := make([]float64, len(s.chunks))
	for i := range s.chunks {
		scores[i] = cosine(q, s.chunks[i].vec)
	}
	idxs := make([]int, len(s.chunks))
	for i := range s.chunks {
		idxs[i] = i
	}
	sort.SliceStable(idxs, func(i, j int) bool { return scores[idxs[i]] > scores[idxs[j]] })
	var hits []Hit
	for _, ci := range idxs {
		if len(hits) >= k {
			break
		}
		ch := s.chunks[ci]
		hits = append(hits, Hit{Path: ch.path, LineStart: ch.lineStart, LineEnd: ch.lineEnd, Snippet: ch.snippet})
	}
	return hits, nil
}
