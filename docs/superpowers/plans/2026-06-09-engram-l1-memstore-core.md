# Engram L1 — MemStore 核心 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Engram 的权威存储核心——内容寻址 ObjStore + go-git 自定义 Storer + Postgres refs(CAS) + 组装出 MemStore，能通过 Go API 提交/物化/CAS，历史可 diff。

**Architecture:** blob/tree/commit 经自定义 go-git `EncodedObjectStorer` 原生落到内容寻址 ObjStore（本地 FS 后端）；唯一可变指针 `agent_id -> HEAD` 在 Postgres，并发收口到单点 CAS；commit 时**先写对象后 CAS ref**，CAS 与 job 入队在同一 Postgres 事务。

**Tech Stack:** Go 1.23；`github.com/go-git/go-git/v5`（+ go-billy/v5/osfs）；`github.com/jackc/pgx/v5`（+ pgxpool）；`github.com/golang-migrate/migrate/v4`（+ source/iofs + database/pgx/v5）。

**依赖关系（包 DAG，无环）：** `objstore`（叶）← `gitfs` ；`refs`（叶）；`memstore` 组装 `objstore + gitfs + refs`。`memstore` 是公开面：声明 `CommitHash`、`MemStore` 接口、`var ErrCASConflict = refs.ErrCASConflict`、`type Job = refs.Job`；`refs`/`gitfs` 内部用 `string` 哈希，不反向 import `memstore`。

**Postgres 测试前提：** 需要一个本地 Postgres。启动：
```bash
docker run --rm -d --name engram-pg -e POSTGRES_PASSWORD=engram -e POSTGRES_DB=engram -p 5433:5432 postgres:16
export ENGRAM_TEST_DB="postgres://postgres:engram@localhost:5433/engram?sslmode=disable"
```
需要 Postgres 的测试在 `ENGRAM_TEST_DB` 未设置时 `t.Skip`。

---

### Task 0: 项目脚手架

**Files:**
- Create: `go.mod`
- Create: `.gitignore`

- [ ] **Step 1: 初始化 module**

Run:
```bash
cd /Users/ssy/Engram
go mod init github.com/ssy/engram
```
Expected: 生成 `go.mod`，内容含 `module github.com/ssy/engram` 与 `go 1.23`（或当前版本）。

- [ ] **Step 2: 拉取依赖**

Run:
```bash
go get github.com/go-git/go-git/v5@latest
go get github.com/go-git/go-billy/v5@latest
go get github.com/jackc/pgx/v5@latest
go get github.com/golang-migrate/migrate/v4@latest
```
Expected: `go.mod` 出现上述 require，`go.sum` 生成。

- [ ] **Step 3: 写 .gitignore**

```
/objects-test/
*.test
/tmp/
```

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum .gitignore
git commit -m "chore: scaffold engram go module"
```
（若仓库尚未 `git init`，先 `git init` 再提交。）

---

### Task 1: ObjStore 接口 + 本地 FS 后端

**Files:**
- Create: `internal/memstore/objstore/objstore.go`
- Create: `internal/memstore/objstore/local.go`
- Test: `internal/memstore/objstore/local_test.go`

- [ ] **Step 1: 写失败测试**

`internal/memstore/objstore/local_test.go`:
```go
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
	_ = s.Put(ctx, "present", []byte("x"))
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
	_ = s.Put(ctx, "aa", []byte("1"))
	_ = s.Put(ctx, "bb", []byte("2"))
	seen := map[string]bool{}
	err := s.Iter(ctx, func(key string) error { seen[key] = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !seen["aa"] || !seen["bb"] {
		t.Fatalf("iter saw %v", seen)
	}
}
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `go test ./internal/memstore/objstore/`
Expected: 编译失败（`NewLocal`/`Local`/`ErrNotFound` 未定义）。

- [ ] **Step 3: 写接口**

`internal/memstore/objstore/objstore.go`:
```go
// Package objstore is the content-addressed object backend. Keys are object
// hashes; values are immutable bytes. Implementations must treat Put as
// idempotent.
package objstore

import (
	"context"
	"errors"
)

// ErrNotFound is returned by Get when a key is absent.
var ErrNotFound = errors.New("objstore: not found")

// ObjStore stores immutable, content-addressed objects.
type ObjStore interface {
	Has(ctx context.Context, key string) (bool, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error // idempotent
	Iter(ctx context.Context, fn func(key string) error) error
}
```

- [ ] **Step 4: 写本地 FS 后端**

`internal/memstore/objstore/local.go`:
```go
package objstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
			return err
		}
		if d.IsDir() {
			return nil
		}
		return fn(d.Name())
	})
}

var _ ObjStore = (*Local)(nil)
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `go test ./internal/memstore/objstore/`
Expected: PASS（全部 5 个用例）。

- [ ] **Step 6: Commit**

```bash
git add internal/memstore/objstore/
git commit -m "feat(objstore): content-addressed ObjStore with local FS backend"
```

---

### Task 2: gitfs — 自定义 go-git Storage（EncodedObjectStorer over ObjStore）

**Files:**
- Create: `internal/memstore/gitfs/storage.go`
- Test: `internal/memstore/gitfs/storage_test.go`

**说明：** `Storage` 嵌入 `*memory.Storage` 复用 ref/config/index/shallow/module 实现，仅覆盖 6 个 `EncodedObjectStorer` 方法把对象读写打到 ObjStore。对象编码用 git loose 对象的**未压缩**帧：`"<type> <size>\x00" + content`——这正是 git 计算哈希的字节，故 key 与 `obj.Hash()` 一致。

- [ ] **Step 1: 写失败测试**

`internal/memstore/gitfs/storage_test.go`:
```go
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

	// 同一内容用第二个 Storage 读回，验证内容寻址持久化。
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
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `go test ./internal/memstore/gitfs/`
Expected: 编译失败（`NewStorage` 未定义）。

- [ ] **Step 3: 写 Storage 实现**

`internal/memstore/gitfs/storage.go`:
```go
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

func (s *Storage) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{}
}

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

func (s *Storage) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	var objs []plumbing.EncodedObject
	err := s.objs.Iter(s.ctx, func(key string) error {
		o, err := s.EncodedObject(t, plumbing.NewHash(key))
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			return nil // type filtered out
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
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `go test ./internal/memstore/gitfs/`
Expected: PASS（2 个用例）。若 `MemoryObject.Write` / `Reader` API 签名有出入，按编译器报错微调（这是 TDD 收敛点）。

- [ ] **Step 5: Commit**

```bash
git add internal/memstore/gitfs/storage.go internal/memstore/gitfs/storage_test.go
git commit -m "feat(gitfs): custom go-git EncodedObjectStorer over ObjStore"
```

---

### Task 3: gitfs — Materialize 与 Commit

**Files:**
- Create: `internal/memstore/gitfs/repo.go`
- Test: `internal/memstore/gitfs/repo_test.go`

**说明：** 权威 ref 在 Postgres，故每次会话用 Storage 临时种入 `refs/heads/main -> parent` 与符号 `HEAD`，再 `git.Open` 物化/提交；新 agent（parent 空）用 `git.Init`。提交对象经 Storage 自然落入 ObjStore。作者署名固定为 engram-agent。

- [ ] **Step 1: 写失败测试**

`internal/memstore/gitfs/repo_test.go`:
```go
package gitfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssy/engram/internal/memstore/objstore"
)

func TestInitialCommitAndMaterialize(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())

	// 第一次提交（无 parent）：在工作树写一个文件并提交。
	work1 := t.TempDir()
	if err := os.WriteFile(filepath.Join(work1, "hello.md"), []byte("v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h1, err := Commit(ctx, objs, "", work1, "init")
	if err != nil {
		t.Fatalf("initial commit: %v", err)
	}
	if h1 == "" {
		t.Fatal("empty hash")
	}

	// 物化 h1 到新目录，应看到 hello.md=v1。
	mat := t.TempDir()
	if err := Materialize(ctx, objs, h1, mat); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mat, "hello.md"))
	if err != nil || string(got) != "v1\n" {
		t.Fatalf("materialized content = %q,%v want v1", got, err)
	}
}

func TestSecondCommitHasParentAndDiff(t *testing.T) {
	ctx := context.Background()
	objs := objstore.NewLocal(t.TempDir())

	w1 := t.TempDir()
	os.WriteFile(filepath.Join(w1, "f.md"), []byte("one\n"), 0o644)
	h1, err := Commit(ctx, objs, "", w1, "c1")
	if err != nil {
		t.Fatal(err)
	}

	// 物化 h1，改文件，基于 h1 提交 h2。
	w2 := t.TempDir()
	if err := Materialize(ctx, objs, h1, w2); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(w2, "f.md"), []byte("one\ntwo\n"), 0o644)
	h2, err := Commit(ctx, objs, h1, w2, "c2")
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	if h2 == h1 {
		t.Fatal("h2 should differ from h1")
	}

	// 物化 h2 应看到新内容。
	m := t.TempDir()
	if err := Materialize(ctx, objs, h2, m); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(m, "f.md"))
	if string(got) != "one\ntwo\n" {
		t.Fatalf("h2 content = %q", got)
	}
}
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `go test ./internal/memstore/gitfs/ -run Commit`
Expected: 编译失败（`Commit`/`Materialize` 未定义）。

- [ ] **Step 3: 写 repo.go**

`internal/memstore/gitfs/repo.go`:
```go
package gitfs

import (
	"context"
	"fmt"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/ssy/engram/internal/memstore/objstore"
)

const branchRef = plumbing.ReferenceName("refs/heads/main")

func author() *object.Signature {
	return &object.Signature{Name: "engram-agent", Email: "agent@engram", When: time.Now()}
}

// seedRefs points the session Storage's HEAD at parent so go-git can build a
// child commit / checkout on top of it.
func seedRefs(st *Storage, parent string) error {
	h := plumbing.NewHash(parent)
	if err := st.SetReference(plumbing.NewHashReference(branchRef, h)); err != nil {
		return err
	}
	return st.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branchRef))
}

// Materialize checks out the tree at `at` into dir. `at` empty means an empty
// tree (nothing written).
func Materialize(ctx context.Context, objs objstore.ObjStore, at string, dir string) error {
	if at == "" {
		return nil
	}
	st := NewStorage(ctx, objs)
	if err := seedRefs(st, at); err != nil {
		return fmt.Errorf("gitfs: seed refs: %w", err)
	}
	repo, err := git.Open(st, osfs.New(dir))
	if err != nil {
		return fmt.Errorf("gitfs: open: %w", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("gitfs: worktree: %w", err)
	}
	if err := wt.Checkout(&git.CheckoutOptions{Hash: plumbing.NewHash(at)}); err != nil {
		return fmt.Errorf("gitfs: checkout %s: %w", at, err)
	}
	return nil
}

// Commit stages everything under dir and builds a commit on `parent` (empty =>
// initial commit). New objects are written through Storage into ObjStore.
func Commit(ctx context.Context, objs objstore.ObjStore, parent string, dir string, msg string) (string, error) {
	st := NewStorage(ctx, objs)
	fs := osfs.New(dir)

	var repo *git.Repository
	var err error
	if parent == "" {
		repo, err = git.Init(st, fs)
		if err != nil {
			return "", fmt.Errorf("gitfs: init: %w", err)
		}
	} else {
		if err := seedRefs(st, parent); err != nil {
			return "", fmt.Errorf("gitfs: seed refs: %w", err)
		}
		repo, err = git.Open(st, fs)
		if err != nil {
			return "", fmt.Errorf("gitfs: open: %w", err)
		}
	}

	wt, err := repo.Worktree()
	if err != nil {
		return "", fmt.Errorf("gitfs: worktree: %w", err)
	}
	if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		return "", fmt.Errorf("gitfs: add: %w", err)
	}
	h, err := wt.Commit(msg, &git.CommitOptions{Author: author(), AllowEmptyCommits: true})
	if err != nil {
		return "", fmt.Errorf("gitfs: commit: %w", err)
	}
	return h.String(), nil
}
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `go test ./internal/memstore/gitfs/`
Expected: PASS（含 Task 2 的 2 个 + 本任务 2 个）。

> 注：若 `git.Init` 默认分支名非 `main`（go-git 历史默认 `master`），`AddWithOptions`/`Commit` 仍可用——提交走 HEAD，不依赖分支名匹配。若 checkout/commit 报 worktree 相关错误，按报错微调（例如需要先 `repo.Storer` 已含 HEAD）。

- [ ] **Step 5: Commit**

```bash
git add internal/memstore/gitfs/repo.go internal/memstore/gitfs/repo_test.go
git commit -m "feat(gitfs): materialize and commit working trees via go-git"
```

---

### Task 4: Postgres migrations

**Files:**
- Create: `db/migrations/000001_init.up.sql`
- Create: `db/migrations/000001_init.down.sql`

- [ ] **Step 1: 写 up migration**

`db/migrations/000001_init.up.sql`:
```sql
CREATE TABLE agent_refs (
  agent_id   text PRIMARY KEY,
  head       text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE memory_jobs (
  id         bigserial PRIMARY KEY,
  agent_id   text NOT NULL,
  kind       text NOT NULL,
  from_sha   text,
  state      text NOT NULL DEFAULT 'pending',
  attempts   int  NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- per-agent singleton: at most one pending job per (agent_id, kind).
CREATE UNIQUE INDEX memory_jobs_pending_uniq
  ON memory_jobs (agent_id, kind) WHERE state = 'pending';

CREATE TABLE maintenance_cursor (
  agent_id      text NOT NULL,
  kind          text NOT NULL,
  processed_sha text NOT NULL,
  PRIMARY KEY (agent_id, kind)
);
```

- [ ] **Step 2: 写 down migration**

`db/migrations/000001_init.down.sql`:
```sql
DROP TABLE IF EXISTS maintenance_cursor;
DROP TABLE IF EXISTS memory_jobs;
DROP TABLE IF EXISTS agent_refs;
```

- [ ] **Step 3: Commit**

```bash
git add db/migrations/
git commit -m "feat(db): initial migration for refs and job queue"
```

---

### Task 5: refs — Postgres refs + CAS + 同 tx 入队

**Files:**
- Create: `internal/memstore/refs/refs.go`
- Create: `internal/memstore/refs/migrate.go`
- Test: `internal/memstore/refs/refs_test.go`

**说明：** `refs` 内部用 `string` 哈希、自有 `Job` 与错误，不 import `memstore`，避免环。`migrate.go` 用 golang-migrate iofs 源 + pgx5 驱动，通过 `embed.FS` 嵌入 `db/migrations`，供测试与 dev 装配复用。

- [ ] **Step 1: 写失败测试**

`internal/memstore/refs/refs_test.go`:
```go
package refs

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set; skipping Postgres test")
	}
	ctx := context.Background()
	if err := Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() {
		// 清表，保证用例隔离。
		pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor")
		pool.Close()
	})
	pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor")
	return pool
}

func TestBootstrapAndResolve(t *testing.T) {
	ctx := context.Background()
	r := New(testPool(t))
	if err := r.Bootstrap(ctx, "a1", "deadbeef"); err != nil {
		t.Fatal(err)
	}
	h, err := r.ResolveHead(ctx, "a1")
	if err != nil || h != "deadbeef" {
		t.Fatalf("resolve = %q,%v", h, err)
	}
}

func TestResolveUnknownAgent(t *testing.T) {
	ctx := context.Background()
	r := New(testPool(t))
	_, err := r.ResolveHead(ctx, "ghost")
	if !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("err = %v want ErrAgentNotFound", err)
	}
}

func TestCASSuccessAndEnqueue(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	r := New(pool)
	r.Bootstrap(ctx, "a1", "p0")
	if err := r.CommitRef(ctx, "a1", "p0", "p1", []Job{{Kind: "reindex"}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	h, _ := r.ResolveHead(ctx, "a1")
	if h != "p1" {
		t.Fatalf("head = %q want p1", h)
	}
	var n int
	pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs WHERE agent_id='a1' AND kind='reindex'").Scan(&n)
	if n != 1 {
		t.Fatalf("jobs = %d want 1", n)
	}
}

func TestCASConflict(t *testing.T) {
	ctx := context.Background()
	r := New(testPool(t))
	r.Bootstrap(ctx, "a1", "p0")
	// 第一次成功 p0->p1。
	if err := r.CommitRef(ctx, "a1", "p0", "p1", nil); err != nil {
		t.Fatal(err)
	}
	// 用过期 parent p0 再提交 -> 冲突，且不入队。
	err := r.CommitRef(ctx, "a1", "p0", "pX", []Job{{Kind: "reindex"}})
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("err = %v want ErrCASConflict", err)
	}
	var n int
	pool.QueryRow := r.pool // 仅示意；用下方独立查询
	_ = pool
	var cnt int
	r.pool.QueryRow(ctx, "SELECT count(*) FROM memory_jobs").Scan(&cnt)
	if cnt != 0 {
		t.Fatalf("conflicting commit must not enqueue; jobs=%d", cnt)
	}
}
```

> 注：上面 `TestCASConflict` 里 `pool.QueryRow := r.pool` 是笔误占位，实现时删掉该两行，直接用 `r.pool.QueryRow(...)`（`refs_test.go` 与 `refs.go` 同包，可访问未导出字段 `pool`）。

- [ ] **Step 2: 运行测试，确认失败**

Run: `go test ./internal/memstore/refs/`
Expected: 编译失败（`New`/`Migrate`/`Job`/错误未定义）。

- [ ] **Step 3: 写 migrate.go**

`internal/memstore/refs/migrate.go`:
```go
package refs

import (
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed all:migrations
var migrationsFS embed.FS

// Migrate applies all up migrations to the database at dsn. Idempotent.
func Migrate(dsn string) error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("refs: migration source: %w", err)
	}
	// pgx5 驱动要求 scheme 为 pgx5://
	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+stripScheme(dsn))
	if err != nil {
		return fmt.Errorf("refs: migrate init: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("refs: migrate up: %w", err)
	}
	return nil
}

// stripScheme turns "postgres://..." / "postgresql://..." into "user:pass@host/db?..."
// so it can be reattached as pgx5://.
func stripScheme(dsn string) string {
	for _, p := range []string{"postgres://", "postgresql://", "pgx5://"} {
		if len(dsn) >= len(p) && dsn[:len(p)] == p {
			return dsn[len(p):]
		}
	}
	return dsn
}
```

> 注：迁移文件需在包内可嵌入。实现时把 `db/migrations/*.sql` 复制或软链到 `internal/memstore/refs/migrations/`（`//go:embed` 不能跨越包目录向上）。把 Task 4 的目标目录直接改为 `internal/memstore/refs/migrations/`，并在 Task 4 里 commit 该路径；`db/migrations` 不再单列。dev CLI 用同一路径。

- [ ] **Step 4: 写 refs.go**

`internal/memstore/refs/refs.go`:
```go
// Package refs is the authoritative mutable pointer: agent_id -> HEAD, guarded
// by a single atomic CAS, plus same-transaction job enqueue. This is the only
// concurrency-control point in the system.
package refs

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrCASConflict   = errors.New("refs: HEAD moved")
	ErrAgentNotFound = errors.New("refs: agent not found")
)

// Job is a maintenance job to enqueue atomically with a commit.
type Job struct {
	Kind string // "reindex" | "reflect" | "defrag" | "gc"
}

type Refs struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Refs { return &Refs{pool: pool} }

func (r *Refs) ResolveHead(ctx context.Context, agentID string) (string, error) {
	var head string
	err := r.pool.QueryRow(ctx, `SELECT head FROM agent_refs WHERE agent_id=$1`, agentID).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrAgentNotFound
	}
	if err != nil {
		return "", fmt.Errorf("refs: resolve %s: %w", agentID, err)
	}
	return head, nil
}

// Bootstrap registers a new agent at head. No-op if it already exists.
func (r *Refs) Bootstrap(ctx context.Context, agentID, head string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO agent_refs (agent_id, head) VALUES ($1,$2) ON CONFLICT (agent_id) DO NOTHING`,
		agentID, head)
	if err != nil {
		return fmt.Errorf("refs: bootstrap %s: %w", agentID, err)
	}
	return nil
}

// CommitRef atomically advances HEAD parent->new and enqueues jobs in ONE tx.
// Returns ErrCASConflict if HEAD != parent (0 rows updated).
func (r *Refs) CommitRef(ctx context.Context, agentID, parent, new string, jobs []Job) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("refs: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	ct, err := tx.Exec(ctx,
		`UPDATE agent_refs SET head=$1, updated_at=now() WHERE agent_id=$2 AND head=$3`,
		new, agentID, parent)
	if err != nil {
		return fmt.Errorf("refs: cas: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrCASConflict
	}
	for _, j := range jobs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO memory_jobs (agent_id, kind, from_sha) VALUES ($1,$2,$3)
			 ON CONFLICT (agent_id, kind) WHERE state='pending' DO NOTHING`,
			agentID, j.Kind, new); err != nil {
			return fmt.Errorf("refs: enqueue %s: %w", j.Kind, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("refs: commit tx: %w", err)
	}
	return nil
}
```

> 注：partial-unique-index 的 `INSERT ... ON CONFLICT ... WHERE` 语法在 PG 中需匹配该部分索引谓词。若 pgx 报谓词不匹配，改用 `INSERT ... ON CONFLICT DO NOTHING`（依赖该唯一索引自动命中）即可。

- [ ] **Step 5: 运行测试，确认通过**

先确保 Postgres 在跑并已 `export ENGRAM_TEST_DB=...`。
Run: `go test ./internal/memstore/refs/`
Expected: PASS（4 个用例；未设 DB 则全部 Skip——CI 必须设 DB）。

- [ ] **Step 6: Commit**

```bash
git add internal/memstore/refs/
git commit -m "feat(refs): Postgres HEAD CAS with same-tx job enqueue"
```

---

### Task 6: MemStore — 组装公开面

**Files:**
- Create: `internal/memstore/memstore.go`
- Test: `internal/memstore/memstore_test.go`

**说明：** `memstore` 是对外公开面，声明 `CommitHash`、`MemStore` 接口、`Job` 别名、`ErrCASConflict` 重导出，把 `gitfs`(string) 与 `refs`(string) 适配成 `CommitHash`。`CreateAgent` 做初始提交 + Bootstrap。`CommitWithCAS` 保证**先对象后 ref**。

- [ ] **Step 1: 写失败测试**

`internal/memstore/memstore_test.go`:
```go
package memstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

func newStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("ENGRAM_TEST_DB")
	if dsn == "" {
		t.Skip("ENGRAM_TEST_DB not set")
	}
	ctx := context.Background()
	if err := refs.Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool.Exec(ctx, "TRUNCATE agent_refs, memory_jobs, maintenance_cursor")
	t.Cleanup(func() { pool.Close() })
	return New(objstore.NewLocal(t.TempDir()), refs.New(pool)), ctx
}

func TestCreateMaterializeCommit(t *testing.T) {
	s, ctx := newStore(t)
	head, err := s.CreateAgent(ctx, "a1", map[string]string{"system/about.md": "---\ndescription: who\n---\nhi\n"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	dir := t.TempDir()
	if err := s.Materialize(ctx, "a1", head, dir); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "system", "about.md"))
	if len(got) == 0 {
		t.Fatal("seed file missing after materialize")
	}

	// 编辑并提交。
	os.WriteFile(filepath.Join(dir, "note.md"), []byte("note\n"), 0o644)
	newHead, err := s.CommitWithCAS(ctx, "a1", head, dir, []Job{{Kind: "reindex"}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if newHead == head {
		t.Fatal("head did not advance")
	}
	resolved, _ := s.ResolveHead(ctx, "a1")
	if resolved != newHead {
		t.Fatalf("resolved %q want %q", resolved, newHead)
	}
}

func TestCommitWithStaleParentConflicts(t *testing.T) {
	s, ctx := newStore(t)
	head, _ := s.CreateAgent(ctx, "a1", map[string]string{"system/x.md": "x"})

	dir := t.TempDir()
	s.Materialize(ctx, "a1", head, dir)
	os.WriteFile(filepath.Join(dir, "a.md"), []byte("a"), 0o644)
	// 第一次提交成功，HEAD 前进。
	if _, err := s.CommitWithCAS(ctx, "a1", head, dir, nil); err != nil {
		t.Fatal(err)
	}
	// 用过期 parent 再提交 -> 冲突。
	os.WriteFile(filepath.Join(dir, "b.md"), []byte("b"), 0o644)
	_, err := s.CommitWithCAS(ctx, "a1", head, dir, nil)
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("err = %v want ErrCASConflict", err)
	}
}
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `go test ./internal/memstore/`
Expected: 编译失败（`New`/`Store`/`CreateAgent` 等未定义）。

- [ ] **Step 3: 写 memstore.go**

`internal/memstore/memstore.go`:
```go
// Package memstore is the authoritative store: content-addressed objects
// (objstore) + a CAS-guarded HEAD ref (refs), glued through a custom go-git
// storer (gitfs). All durable writes go through it.
package memstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ssy/engram/internal/memstore/gitfs"
	"github.com/ssy/engram/internal/memstore/objstore"
	"github.com/ssy/engram/internal/memstore/refs"
)

type CommitHash string

// Job aliases refs.Job so callers depend only on memstore.
type Job = refs.Job

// ErrCASConflict is returned when HEAD moved under a commit.
var ErrCASConflict = refs.ErrCASConflict

// MemStore is the authoritative store interface (see docs/architecture.md §11).
type MemStore interface {
	ResolveHead(ctx context.Context, agentID string) (CommitHash, error)
	Materialize(ctx context.Context, agentID string, at CommitHash, dir string) error
	CommitWithCAS(ctx context.Context, agentID string, parent CommitHash, dir string, jobs []Job) (CommitHash, error)
}

type Store struct {
	objs objstore.ObjStore
	refs *refs.Refs
}

func New(objs objstore.ObjStore, r *refs.Refs) *Store {
	return &Store{objs: objs, refs: r}
}

func (s *Store) ResolveHead(ctx context.Context, agentID string) (CommitHash, error) {
	h, err := s.refs.ResolveHead(ctx, agentID)
	return CommitHash(h), err
}

func (s *Store) Materialize(ctx context.Context, agentID string, at CommitHash, dir string) error {
	return gitfs.Materialize(ctx, s.objs, string(at), dir)
}

// CommitWithCAS writes objects for the working tree (objects FIRST), then
// atomically advances HEAD parent->new and enqueues jobs (ref SECOND).
func (s *Store) CommitWithCAS(ctx context.Context, agentID string, parent CommitHash, dir string, jobs []Job) (CommitHash, error) {
	newHash, err := gitfs.Commit(ctx, s.objs, string(parent), dir, "agent commit")
	if err != nil {
		return "", fmt.Errorf("memstore: write objects: %w", err)
	}
	if err := s.refs.CommitRef(ctx, agentID, string(parent), newHash, jobs); err != nil {
		return "", err // ErrCASConflict propagates as-is
	}
	return CommitHash(newHash), nil
}

// CreateAgent seeds an agent's initial commit from `seed` (path->content) and
// registers its HEAD. Returns the initial commit hash.
func (s *Store) CreateAgent(ctx context.Context, agentID string, seed map[string]string) (CommitHash, error) {
	dir, err := os.MkdirTemp("", "engram-seed-*")
	if err != nil {
		return "", fmt.Errorf("memstore: seed dir: %w", err)
	}
	defer os.RemoveAll(dir)
	for p, content := range seed {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			return "", err
		}
	}
	h, err := gitfs.Commit(ctx, s.objs, "", dir, "init")
	if err != nil {
		return "", fmt.Errorf("memstore: seed commit: %w", err)
	}
	if err := s.refs.Bootstrap(ctx, agentID, h); err != nil {
		return "", err
	}
	return CommitHash(h), nil
}

var _ MemStore = (*Store)(nil)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `go test ./internal/memstore/`
Expected: PASS（2 个用例）。

- [ ] **Step 5: 全量构建 + 测试**

Run: `go build ./... && go test ./...`
Expected: 全绿（需 `ENGRAM_TEST_DB`，否则 Postgres 相关 Skip）。

- [ ] **Step 6: Commit**

```bash
git add internal/memstore/memstore.go internal/memstore/memstore_test.go
git commit -m "feat(memstore): assemble objstore+gitfs+refs into authoritative MemStore"
```

---

## Self-Review

**Spec coverage（对照 spec §3.1–3.4、§4、§5）:**
- §3.1 ObjStore + 本地 FS → Task 1 ✅
- §3.2 gitfs 自定义 Storer（EncodedObjectStorer + ReferenceStorer 种入 + Materialize）→ Task 2、3 ✅
- §3.3 refs（表、ResolveHead、CommitRef 单 tx CAS+入队、Bootstrap、River 切换点注释）→ Task 4、5 ✅
- §3.4 MemStore（§11 接口、先对象后 ref）→ Task 6 ✅
- §4 错误处理（`%w` 包裹、`ErrCASConflict`、`ErrNotFound`、ctx 首参）→ 贯穿各 Task ✅
- §5 测试（objstore 往返/幂等、gitfs 往返+materialize/commit/diff、refs CAS 冲突、memstore 端到端）→ 各 Task 测试 ✅
- §3.5 LLMProvider/工具集、§3.6 上下文装配、§3.7 cache/search 接口 → **不在本计划**，属 Plan 2（agent loop）。

**Placeholder 扫描:** Task 5 测试内 `pool.QueryRow := r.pool` 两行是**故意标注的笔误占位**，已在紧随其后的「注」中要求实现时删除并改用 `r.pool.QueryRow`。其余无 TODO/TBD。

**Type 一致性:** `Commit(ctx, objs, parent, dir, msg) string` / `Materialize(ctx, objs, at, dir) error` 在 gitfs 定义并在 memstore 调用，签名一致；`refs.New/ResolveHead/Bootstrap/CommitRef/Migrate/Job/ErrCASConflict/ErrAgentNotFound` 跨 Task 5/6 一致；`memstore.New(objstore.ObjStore, *refs.Refs)` 与测试构造一致。

**已知收敛点（TDD 中按编译器/PG 报错微调，不算占位）:**
1. go-git `MemoryObject` 的 `Write/Reader/SetSize` 具体签名。
2. `git.Init` 默认分支名、worktree commit 行为。
3. golang-migrate pgx5 驱动的 DSN scheme（`pgx5://`）。
4. partial-unique-index 的 `ON CONFLICT` 谓词写法。
5. **migrations 嵌入路径**：`//go:embed` 不能向上跨目录，故迁移文件实际落在 `internal/memstore/refs/migrations/`（见 Task 5 Step 3 注），Task 4 的路径相应调整为该目录。
