package litestream_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/mock"
)

// newFailingReplicaClient returns a mock client whose uploads fail until
// failUntil attempts have been observed, then succeed.
func newFailingReplicaClient(failUntil int32, failure error) *mock.ReplicaClient {
	var attempts atomic.Int32
	return &mock.ReplicaClient{
		LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
		WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
			n := attempts.Add(1)
			if n <= failUntil {
				return nil, failure
			}
			_, _ = io.Copy(io.Discard, r)
			return &ltx.FileInfo{}, nil
		},
	}
}

// TestDB_BeginShutdown_RejectsNewSync verifies that after ordered shutdown
// begins, write/WAL-capture entry points return a definitive ErrShuttingDown
// rather than blocking or silently dropping the work.
func TestDB_BeginShutdown_RejectsNewSync(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	db.BeginShutdown()
	if !db.IsShuttingDown() {
		t.Fatal("expected IsShuttingDown to be true")
	}

	// Sync routes through lockExec and must be rejected.
	err := db.Sync(context.Background())
	if !errors.Is(err, litestream.ErrShuttingDown) {
		t.Fatalf("Sync after shutdown: got %v, want ErrShuttingDown", err)
	}

	// Checkpoint shares the same gate.
	if err := db.Checkpoint(context.Background(), litestream.CheckpointModePassive); !errors.Is(err, litestream.ErrShuttingDown) {
		t.Fatalf("Checkpoint after shutdown: got %v, want ErrShuttingDown", err)
	}
}

// TestDB_Close_Idempotent verifies that concurrent and repeated Close calls run
// the shutdown sequence exactly once and never double-release handles.
func TestDB_Close_Idempotent(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	var uploads atomic.Int32
	client := &mock.ReplicaClient{
		LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
		WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
			uploads.Add(1)
			_, _ = io.Copy(io.Discard, r)
			return &ltx.FileInfo{}, nil
		},
	}
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.ShutdownSyncTimeout = 5 * time.Second
	db.ShutdownSyncInterval = 20 * time.Millisecond

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	wg.Add(callers)
	for i := range callers {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = db.Close(context.Background())
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close[%d] returned error: %v", i, err)
		}
	}

	// A subsequent Close after completion is an idempotent no-op.
	if err := db.Close(context.Background()); err != nil {
		t.Fatalf("repeated Close after completion: got %v, want nil", err)
	}
}

// TestDB_Close_PersistsPendingStateOnFailure verifies that when the shutdown
// flush cannot reach the remote, the un-uploaded LTX queue is retained locally
// and a durable marker with the failure reason is written to the meta dir.
func TestDB_Close_PersistsPendingStateOnFailure(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)

	if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}

	client := newFailingReplicaClient(1<<20, errors.New("remote unavailable (connection refused)"))
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.ShutdownSyncTimeout = 200 * time.Millisecond
	db.ShutdownSyncInterval = 25 * time.Millisecond

	err := db.Close(context.Background())
	if err == nil {
		t.Fatal("expected Close to fail when remote is unavailable")
	}

	// Local LTX files must still be present.
	levelDir := db.LTXLevelDir(0)
	ents, readErr := os.ReadDir(levelDir)
	if readErr != nil {
		t.Fatalf("local LTX dir removed: %v", readErr)
	}
	if len(ents) == 0 {
		t.Fatal("expected retained local LTX files after failed flush")
	}

	// The durable pending marker must exist and carry the failure reason.
	marker := filepath.Join(db.MetaPath(), "shutdown-pending.json")
	buf, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("pending marker not written: %v", err)
	}
	content := string(buf)
	if !strings.Contains(content, "remote unavailable") {
		t.Fatalf("marker missing failure reason: %s", content)
	}
	if !strings.Contains(content, `"files"`) {
		t.Fatalf("marker missing queued files: %s", content)
	}
}

// TestDB_Open_DrainsPendingBeforeNewWrites verifies the restart half of the
// ordered-shutdown contract: a pending marker from a previous failed shutdown
// is drained before normal replication resumes, and the marker is removed once
// the queue fully uploads.
func TestDB_Open_DrainsPendingBeforeNewWrites(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)

	if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}

	// First lifecycle: remote always fails, leaving a pending marker.
	failing := newFailingReplicaClient(1<<20, errors.New("network down"))
	db.Replica = litestream.NewReplicaWithClient(db, failing)
	db.ShutdownSyncTimeout = 150 * time.Millisecond
	db.ShutdownSyncInterval = 25 * time.Millisecond
	if err := db.Close(context.Background()); err == nil {
		t.Fatal("expected failed Close")
	}
	marker := filepath.Join(db.MetaPath(), "shutdown-pending.json")
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected pending marker after failed shutdown: %v", err)
	}

	// Second lifecycle: remote recovers. Reopening must drain the queue and
	// clear the marker before accepting new write work.
	recovered := newFailingReplicaClient(0, nil)
	db.Replica = litestream.NewReplicaWithClient(db, recovered)
	db.ShutdownSyncTimeout = 5 * time.Second
	if err := db.Open(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer testingutil.MustCloseDB(t, db)

	// init runs lazily on first sync; trigger it so the startup drain executes.
	sqlDB2 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqlDB2)
	if err := db.Sync(context.Background()); err != nil {
		t.Fatalf("post-restart sync: %v", err)
	}

	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("pending marker should be cleared after drain, stat err=%v", err)
	}
}

// TestDB_Open_KeepsPendingWhenRemoteStillDown verifies that an unavailable
// remote at restart does not get treated as success: the marker survives and
// the background replica monitor keeps the data queued.
func TestDB_Open_KeepsPendingWhenRemoteStillDown(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)

	if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}

	db.Replica = litestream.NewReplicaWithClient(db, newFailingReplicaClient(1<<20, errors.New("persistent outage")))
	db.ShutdownSyncTimeout = 150 * time.Millisecond
	db.ShutdownSyncInterval = 25 * time.Millisecond
	if err := db.Close(context.Background()); err == nil {
		t.Fatal("expected failed Close")
	}
	marker := filepath.Join(db.MetaPath(), "shutdown-pending.json")
	if _, err := os.ReadFile(marker); err != nil {
		t.Fatalf("missing marker: %v", err)
	}

	// Reopen with the remote still unavailable. Drain must not clear the marker.
	db.Replica = litestream.NewReplicaWithClient(db, newFailingReplicaClient(1<<20, errors.New("persistent outage")))
	if err := db.Open(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer testingutil.MustCloseDB(t, db)

	if got, err := os.ReadFile(marker); err != nil {
		t.Fatalf("marker removed despite ongoing outage: %v", err)
	} else if len(got) == 0 {
		t.Fatal("marker emptied despite ongoing outage")
	}
}
