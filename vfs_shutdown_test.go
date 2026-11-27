//go:build vfs
// +build vfs

package litestream

import (
	"errors"
	"testing"
	"time"

	"github.com/psanford/sqlite3vfs"
)

// TestVFSFile_BeginShutdown_RejectsWrites verifies that once an ordered
// shutdown begins, new write-intent operations fail with a distinct
// errors.Is-able shutdown cause instead of being silently dropped.
func TestVFSFile_BeginShutdown_RejectsWrites(t *testing.T) {
	client := newWriteTestReplicaClient()
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	copy(initialPage, "initial data")
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	f := setupWriteableVFSFile(t, client)
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// A write works before shutdown.
	if _, err := f.WriteAt([]byte("before"), 0); err != nil {
		t.Fatalf("write before shutdown: %v", err)
	}

	f.BeginShutdown()
	if !f.IsShuttingDown() {
		t.Fatal("expected IsShuttingDown to be true")
	}

	// WriteAt is rejected with the distinct shutdown cause.
	if _, err := f.WriteAt([]byte("after"), 0); !errors.Is(err, sqlite3vfs.ReadOnlyError) {
		t.Fatalf("WriteAt after shutdown: got %v, want a read-only-wrapped shutdown error", err)
	}

	// Truncate is rejected as well.
	if err := f.Truncate(int64(pageSize)); !errors.Is(err, sqlite3vfs.ReadOnlyError) {
		t.Fatalf("Truncate after shutdown: got %v, want read-only-wrapped shutdown error", err)
	}

	// A new write-intent RESERVED lock is rejected rather than queued.
	if err := f.Lock(sqlite3vfs.LockReserved); !errors.Is(err, sqlite3vfs.ReadOnlyError) {
		t.Fatalf("RESERVED lock after shutdown: got %v, want read-only-wrapped shutdown error", err)
	}
}

// TestVFSFile_CloseWithDirtyPagesRunsOnce exercises the full close sequence
// (BeginShutdown, final flush under f.mu, handle release) after writes have
// produced dirty pages, and confirms repeated Close calls do not re-run it.
func TestVFSFile_CloseWithDirtyPagesRunsOnce(t *testing.T) {
	client := newWriteTestReplicaClient()
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	copy(initialPage, "initial data")
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	f := setupWriteableVFSFile(t, client)
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}

	if _, err := f.WriteAt([]byte("dirty data"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(f.dirty) == 0 {
		t.Fatal("expected dirty pages before close")
	}

	done := make(chan error, 1)
	go func() { done <- f.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close with dirty pages: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not complete")
	}

	// Repeated Close stays idempotent and non-blocking.
	if err := f.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestVFSFile_BeginShutdown_Idempotent verifies repeated calls do not panic and
// that Close remains idempotent under repeated invocation.
func TestVFSFile_BeginShutdown_Idempotent(t *testing.T) {
	client := newWriteTestReplicaClient()
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	f := setupWriteableVFSFile(t, client)
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}

	f.BeginShutdown()
	f.BeginShutdown() // must not panic or flip state back

	if !f.IsShuttingDown() {
		t.Fatal("expected shutdown state to persist")
	}

	// Repeated Close must run the close sequence exactly once and not panic on
	// a second close of syncStop or other handles.
	if err := f.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("second Close must be idempotent, got: %v", err)
	}
}
