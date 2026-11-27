package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/mock"
)

// TestReplicateCommand_ClosePersistsPendingOnFlushFailure verifies the
// orchestration-level contract: when the shutdown flush cannot reach the
// remote, ReplicateCommand.Close returns a non-nil error (driving a non-zero
// process exit code) and the un-uploaded LTX queue is durably marked in the
// database metadata directory for the next process.
func TestReplicateCommand_ClosePersistsPendingOnFlushFailure(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)

	if _, err := sqldb.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}

	// Remote always fails.
	client := &mock.ReplicaClient{
		LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
		WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
			return nil, errors.New("remote unavailable")
		},
	}
	db.Replica = litestream.NewReplicaWithClient(db, client)

	c := NewReplicateCommand()
	c.Store = litestream.NewStore([]*litestream.DB{db}, litestream.DefaultCompactionLevels)
	// NewStore resets per-DB shutdown settings, so configure them on the store.
	c.Store.SetShutdownSyncTimeout(200 * time.Millisecond)
	c.Store.SetShutdownSyncInterval(25 * time.Millisecond)
	c.Store.CompactionMonitorEnabled = false
	c.Store.RetentionEnabled = false
	if err := c.Store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A non-nil Close error is what main() maps to a non-zero exit code, so
	// orchestration can tell the flush did not finish.
	if err := c.Close(context.Background()); err == nil {
		t.Fatal("expected Close error when flush fails, got nil")
	}

	marker := filepath.Join(db.MetaPath(), "shutdown-pending.json")
	buf, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("pending marker not persisted: %v", err)
	}
	if !bytes.Contains(buf, []byte("remote unavailable")) {
		t.Fatalf("marker missing failure reason: %s", buf)
	}

	// Local LTX files must remain so the next process can resume.
	ents, err := os.ReadDir(db.LTXLevelDir(0))
	if err != nil {
		t.Fatalf("local LTX dir missing: %v", err)
	}
	if len(ents) == 0 {
		t.Fatal("expected local LTX files to be retained")
	}
}
