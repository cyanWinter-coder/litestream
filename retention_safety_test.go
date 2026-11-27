package litestream_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

// assertRestorable verifies that every TXID up to the head still has a
// complete, gap-free restore plan after retention ran.
func assertRestorable(t *testing.T, ctx context.Context, client litestream.ReplicaClient, logger *slog.Logger) {
	t.Helper()
	infos, err := litestream.CalcRestorePlan(ctx, client, 0, time.Time{}, logger)
	if err != nil {
		t.Fatalf("restore chain broken after retention: %v", err)
	}
	if len(infos) == 0 {
		t.Fatal("no restore plan available after retention")
	}
	// Contiguity: each file after the first must attach to the previous max.
	prevMax := infos[0].MaxTXID
	for _, info := range infos[1:] {
		if info.MinTXID != prevMax+1 {
			t.Fatalf("non-contiguous restore plan: prevMax=%s nextMin=%s", prevMax, info.MinTXID)
		}
		prevMax = info.MaxTXID
	}
}

// TestRetention_WindowTimePointsRemainRecoverable is the core regression for the
// reported incident: cleanup must not delete an incremental file that the
// retained time points still depend on.
func TestRetention_WindowTimePointsRemainRecoverable(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	compactor := litestream.NewCompactor(client, slog.Default())
	now := time.Now()

	// Two snapshots. The older one anchors time points between the two
	// snapshots; L1 files extend the chain to the current head.
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 100, now.Add(-2*time.Hour))
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 200, now.Add(-30*time.Minute))
	createTestLTXFileWithTimestamp(t, client, 1, 101, 150, now.Add(-90*time.Minute))
	createTestLTXFileWithTimestamp(t, client, 1, 151, 200, now.Add(-60*time.Minute))
	createTestLTXFileWithTimestamp(t, client, 1, 201, 250, now.Add(-10*time.Minute))

	// One-hour retention window.
	if _, err := compactor.EnforceSnapshotRetention(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := compactor.EnforceRetentionByTXID(ctx, 1, 200); err != nil {
		t.Fatal(err)
	}

	// The newest snapshot and the full chain must still restore to the head.
	assertRestorable(t, ctx, client, slog.Default())
}

// TestRetention_DoesNotDeleteFilesProducedDuringRun ensures a backup writing
// concurrently cannot have a freshly produced file removed merely because the
// retention process already observed it.
func TestRetention_DoesNotDeleteFilesProducedDuringRun(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	compactor := litestream.NewCompactor(client, slog.Default())

	// A snapshot that fully covers the two L1 files, making them candidates.
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 10, time.Now())
	createTestLTXFile(t, client, 1, 1, 5)
	// This file is back-dated but we model an in-flight upload by giving it a
	// future CreatedAt relative to the enforcement instant.
	fresh := createLTXInfo(1, 6, 10)
	fresh.CreatedAt = time.Now().Add(time.Minute)
	writeLTXInfo(t, client, fresh)

	if err := compactor.EnforceRetentionByTXID(ctx, 1, 6); err != nil {
		t.Fatal(err)
	}

	// The in-flight file must survive.
	if rc, err := client.OpenLTXFile(ctx, 1, 6, 10, 0, 0); err != nil {
		t.Fatalf("in-flight LTX file was deleted: %v", err)
	} else {
		_ = rc.Close()
	}
}

// TestRetention_ConcurrentRunsDoNotDeleteSameBatch verifies the per-target lock
// serializes duplicate enforcement runs.
func TestRetention_ConcurrentRunsDoNotDeleteSameBatch(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	compactor := litestream.NewCompactor(client, slog.Default())

	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 5, time.Now().Add(-3*time.Hour))
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 10, time.Now().Add(-2*time.Hour))
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 15, time.Now())

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := compactor.EnforceSnapshotRetention(ctx, time.Hour)
			if err != nil && !errors.Is(err, litestream.ErrRetentionInProgress) {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent enforcement failed: %v", err)
	}

	// Whatever ran, the remaining snapshots must still restore.
	assertRestorable(t, ctx, client, slog.Default())
}

// TestRetention_FailedDeletionLeavesStateRecoverable uses a client whose delete
// fails partway; the run must return the error and leave the surviving set
// recoverable.
func TestRetention_FailedDeletionLeavesStateRecoverable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	inner := file.NewReplicaClient(filepath.Join(dir, "replica"))
	failing := &failingDeleteClient{ReplicaClient: inner, failAfter: 0}
	compactor := litestream.NewCompactor(failing, slog.Default())

	createTestLTXFileWithTimestamp(t, failing, litestream.SnapshotLevel, 1, 5, time.Now().Add(-4*time.Hour))
	createTestLTXFileWithTimestamp(t, failing, litestream.SnapshotLevel, 1, 10, time.Now().Add(-2*time.Hour))
	createTestLTXFileWithTimestamp(t, failing, litestream.SnapshotLevel, 1, 15, time.Now())

	if _, err := compactor.EnforceSnapshotRetention(ctx, time.Hour); err == nil {
		t.Fatal("expected deletion error to propagate")
	}

	// The surviving files must still form a valid restore plan.
	assertRestorable(t, ctx, inner, slog.Default())
}

// failingDeleteClient wraps a client and fails DeleteLTXFiles after a number of
// successful single-file deletions.
type failingDeleteClient struct {
	litestream.ReplicaClient
	mu        sync.Mutex
	deletions int
	failAfter int
}

func (c *failingDeleteClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletions += len(a)
	if c.deletions > c.failAfter {
		return errors.New("simulated storage outage")
	}
	return c.ReplicaClient.DeleteLTXFiles(ctx, a)
}

func createLTXInfo(level int, lo, hi uint64) *ltx.FileInfo {
	return &ltx.FileInfo{Level: level, MinTXID: ltx.TXID(lo), MaxTXID: ltx.TXID(hi), CreatedAt: time.Now()}
}

func writeLTXInfo(t *testing.T, client litestream.ReplicaClient, info *ltx.FileInfo) {
	t.Helper()
	createTestLTXFileWithTimestamp(t, client, info.Level, info.MinTXID, info.MaxTXID, info.CreatedAt)
}

// TestRetention_StoreCascadeKeepsChainRecoverable drives the full Store
// orchestration (snapshot retention then TXID cascade across lower levels) and
// asserts the surviving files still restore to the head.
func TestRetention_StoreCascadeKeepsChainRecoverable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	client := file.NewReplicaClient(filepath.Join(dir, "replica"))

	// Snapshot anchors plus a multi-level LTX chain with a gap that only L0
	// bridges, so the cascade must protect the bridging L0 files.
	now := time.Now()
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 5, now.Add(-2*time.Hour))
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 20, now)
	createTestLTXFileWithTimestamp(t, client, 1, 1, 5, now.Add(-2*time.Hour))
	createTestLTXFileWithTimestamp(t, client, 1, 9, 15, now.Add(-30*time.Minute))
	createTestLTXFileWithTimestamp(t, client, 0, 6, 8, now.Add(-90*time.Minute))
	createTestLTXFileWithTimestamp(t, client, 0, 16, 20, now.Add(-10*time.Minute))

	db := newStoreBackedDB(t, dir, client)
	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: time.Hour},
	})
	store.SnapshotRetention = time.Hour

	if err := store.EnforceSnapshotRetention(ctx, db); err != nil {
		t.Fatalf("store retention: %v", err)
	}

	assertRestorable(t, ctx, client, db.Logger)
}

// newStoreBackedDB builds an opened DB wired to the given file client without
// starting its background WAL monitor.
func newStoreBackedDB(t *testing.T, dir string, client litestream.ReplicaClient) *litestream.DB {
	t.Helper()
	db := litestream.NewDB(filepath.Join(dir, "db"))
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(t.Context()) })
	return db
}
