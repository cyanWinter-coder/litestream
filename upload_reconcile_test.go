package litestream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

type reconcileFakeClient struct {
	mu sync.Mutex

	objects map[string][]byte
	writes  int
	resumes int
	deletes int

	writeErr  error
	resumeErr error
}

func newReconcileFakeClient() *reconcileFakeClient {
	return &reconcileFakeClient{objects: make(map[string][]byte)}
}

func (c *reconcileFakeClient) Type() string               { return "reconcile-fake" }
func (c *reconcileFakeClient) Init(context.Context) error { return nil }
func (c *reconcileFakeClient) SetLogger(*slog.Logger)     {}

func reconcileObjectKey(level int, minTXID, maxTXID ltx.TXID) string {
	return UploadRecordID(level, minTXID, maxTXID)
}

func (c *reconcileFakeClient) LTXFiles(_ context.Context, level int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	infos := make([]*ltx.FileInfo, 0)
	for key, b := range c.objects {
		var gotLevel int
		var min, max uint64
		if _, err := fmt.Sscanf(key, "ltx-L%d-%x-%x", &gotLevel, &min, &max); err != nil || gotLevel != level {
			continue
		}
		infos = append(infos, &ltx.FileInfo{Level: gotLevel, MinTXID: ltx.TXID(min), MaxTXID: ltx.TXID(max), Size: int64(len(b))})
	}
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *reconcileFakeClient) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := reconcileObjectKey(level, minTXID, maxTXID)
	b, ok := c.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if offset > int64(len(b)) {
		return nil, io.ErrUnexpectedEOF
	}
	b = b[offset:]
	if size > 0 && size < int64(len(b)) {
		b = b[:size]
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (c *reconcileFakeClient) WriteLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	key := reconcileObjectKey(level, minTXID, maxTXID)
	c.objects[key] = b
	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(b))}, nil
}

func (c *reconcileFakeClient) ResumeLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, _ string, offset int64, r io.Reader) (*ltx.FileInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumes++
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	if c.resumeErr != nil {
		return nil, c.resumeErr
	}
	key := reconcileObjectKey(level, minTXID, maxTXID)
	b := c.objects[key]
	if int64(len(b)) != offset {
		return nil, io.ErrUnexpectedEOF
	}
	suffix, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b = append(b, suffix...)
	c.objects[key] = b
	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(b))}, nil
}

func (c *reconcileFakeClient) DeleteLTXFiles(_ context.Context, items []*ltx.FileInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes += len(items)
	for _, info := range items {
		delete(c.objects, reconcileObjectKey(info.Level, info.MinTXID, info.MaxTXID))
	}
	return nil
}

func (c *reconcileFakeClient) DeleteAll(context.Context) error { return nil }

func newReconcileTestDB(t *testing.T, client ReplicaClient) *DB {
	t.Helper()
	meta := t.TempDir()
	db := NewDB(filepath.Join(t.TempDir(), "snapshot.db"))
	db.SetMetaPath(meta)
	db.Replica = NewReplicaWithClient(db, client)
	return db
}

func writeReconcileSource(t *testing.T, db *DB, level int, minTXID, maxTXID ltx.TXID, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.ltx")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	record := &UploadRecord{
		ID:           UploadRecordID(level, minTXID, maxTXID),
		Kind:         "ltx",
		Level:        level,
		MinTXID:      uint64(minTXID),
		MaxTXID:      uint64(maxTXID),
		ObjectKey:    uploadObjectKey(level, minTXID, maxTXID),
		LocalPath:    path,
		ExpectedSize: int64(len(data)),
		Status:       UploadRecordCompleted,
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := db.UploadJournal().Save(record); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDB_OpenReconcilesInterruptedUpload(t *testing.T) {
	client := newReconcileFakeClient()
	db := newReconcileTestDB(t, client)
	db.MonitorInterval = 0
	db.Replica.MonitorEnabled = false

	data := mustBuildIncrementalLTX(t, 1, 11, 512, 1, 'e')
	writeReconcileSource(t, db, 0, 1, 11, data)

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })

	if client.writes != 1 || len(client.objects) != 1 {
		t.Fatalf("startup writes=%d objects=%d, want one recovery copy", client.writes, len(client.objects))
	}
	report := db.LastUploadReconciliation()
	if report == nil || report.Recovered != 1 {
		t.Fatalf("startup report=%+v", report)
	}
	persisted, err := db.ReadUploadReconciliationReport()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Items[0].LastRecoveryDecision != UploadDecisionRebuild {
		t.Fatalf("recovery decision=%q, want rebuild", persisted.Items[0].LastRecoveryDecision)
	}
}

func TestDB_OpenReturnsAndPersistsRecoveryFailure(t *testing.T) {
	client := newReconcileFakeClient()
	db := newReconcileTestDB(t, client)
	db.MonitorInterval = 0
	db.Replica.MonitorEnabled = false

	data := mustBuildIncrementalLTX(t, 6, 6, 512, 1, 'f')
	writeReconcileSource(t, db, 0, 6, 6, data)
	client.objects[reconcileObjectKey(0, 6, 6)] = append([]byte(nil), data[:3]...)
	client.writeErr = errors.New("remote unavailable")

	err := db.Open()
	if err == nil {
		_ = db.Close(context.Background())
		t.Fatal("expected Open to surface reconciliation failure")
	}

	report, readErr := db.ReadUploadReconciliationReport()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if report.Failed != 1 || report.Items[0].Diagnosis != SnapshotSelfCheckRecoveryFailed {
		t.Fatalf("report=%+v", report)
	}
	record, loadErr := db.UploadJournal().Load(UploadRecordID(0, 6, 6))
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if record.Status != UploadRecordFailed || record.FailedAt == nil {
		t.Fatalf("failure state not persisted: %+v", record)
	}
}

func TestUploadReconcile_MissingRemoteRebuildsIdempotently(t *testing.T) {
	ctx := context.Background()
	client := newReconcileFakeClient()
	db := newReconcileTestDB(t, client)
	data := mustBuildIncrementalLTX(t, 1, 9, 512, 1, 'a')
	writeReconcileSource(t, db, 0, 1, 9, data)

	inspection, err := db.InspectUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Missing != 1 || inspection.Items[0].Decision != UploadDecisionRebuild {
		t.Fatalf("inspection=%+v, want missing/rebuild", inspection)
	}
	if client.writes != 0 || client.deletes != 0 {
		t.Fatalf("self-check modified remote: writes=%d deletes=%d", client.writes, client.deletes)
	}

	report, err := db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if report.Recovered != 1 || report.Failed != 0 {
		t.Fatalf("report=%+v", report)
	}
	if client.writes != 1 || len(client.objects) != 1 {
		t.Fatalf("writes=%d objects=%d, want one deterministic copy", client.writes, len(client.objects))
	}

	// Simulate repeated startups. The verified record must not upload again or
	// create another object.
	report, err = db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Healthy != 1 || report.Recovered != 0 {
		t.Fatalf("second startup report=%+v", report)
	}
	if client.writes != 1 || len(client.objects) != 1 {
		t.Fatalf("duplicate recovery: writes=%d objects=%d", client.writes, len(client.objects))
	}

	persisted, err := db.ReadUploadReconciliationReport()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Items[0].ID != UploadRecordID(0, 1, 9) || persisted.Items[0].ObjectKey == "" {
		t.Fatalf("report lacks shared identity: %+v", persisted.Items[0])
	}
}

func TestUploadReconcile_PartialPrefixResumes(t *testing.T) {
	ctx := context.Background()
	client := newReconcileFakeClient()
	db := newReconcileTestDB(t, client)
	data := mustBuildSnapshotLTX(t, 1, 7, 512, [][]byte{bytes.Repeat([]byte{'x'}, 512)})
	writeReconcileSource(t, db, SnapshotLevel, 1, 7, data)

	// A killed upload left exactly a valid prefix behind.
	prefix := len(data) / 2
	client.objects[reconcileObjectKey(SnapshotLevel, 1, 7)] = append([]byte(nil), data[:prefix]...)

	report, err := db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Corrupt != 0 || report.Recovered != 1 {
		t.Fatalf("report=%+v, want corrupt classified then recovered", report)
	}
	item := report.Items[0]
	if item.Record.Decision != UploadDecisionResume {
		t.Fatalf("decision=%q reason=%q, want resume", item.Decision, item.Reason)
	}
	if client.resumes != 1 || client.writes != 0 {
		t.Fatalf("resumes=%d writes=%d, want one resume and no rebuild", client.resumes, client.writes)
	}
	if got := client.objects[reconcileObjectKey(SnapshotLevel, 1, 7)]; !bytes.Equal(got, data) {
		t.Fatalf("resumed object len=%d, want %d", len(got), len(data))
	}

	_, err = db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if client.resumes != 1 {
		t.Fatalf("repeated startup resumed again: %d", client.resumes)
	}
}

func TestUploadReconcile_CorruptPrefixWithoutResumableClientRebuilds(t *testing.T) {
	ctx := context.Background()
	base := newReconcileFakeClient()
	db := newReconcileTestDB(t, base)
	data := mustBuildIncrementalLTX(t, 2, 3, 512, 1, 'b')
	writeReconcileSource(t, db, 0, 2, 3, data)

	// Standard ReplicaClient cannot resume; a shorter-than-expected object is
	// rebuilt through the same deterministic key.
	half := append([]byte(nil), data[:len(data)/2]...)
	base.objects[reconcileObjectKey(0, 2, 3)] = half
	standard := &nonResumableFakeClient{ReplicaClient: base}
	db.Replica.Client = standard

	report, err := db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Recovered != 1 {
		t.Fatalf("report=%+v", report)
	}
	if report.Items[0].Record.Decision != UploadDecisionRebuild {
		t.Fatalf("decision=%q, want rebuild", report.Items[0].Record.Decision)
	}
	if base.writes != 1 || base.resumes != 0 {
		t.Fatalf("writes=%d resumes=%d", base.writes, base.resumes)
	}
}

type nonResumableFakeClient struct {
	ReplicaClient
}

func TestUploadReconcile_EqualSizeCorruptObjectRebuilds(t *testing.T) {
	ctx := context.Background()
	client := newReconcileFakeClient()
	db := newReconcileTestDB(t, client)
	data := mustBuildIncrementalLTX(t, 8, 8, 512, 1, 'g')
	writeReconcileSource(t, db, 0, 8, 8, data)

	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 0xff
	client.objects[reconcileObjectKey(0, 8, 8)] = corrupt

	inspection, err := db.InspectUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Corrupt != 1 || inspection.Items[0].Decision != UploadDecisionRebuild {
		t.Fatalf("inspection=%+v, want corrupt/rebuild", inspection)
	}

	report, err := db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Recovered != 1 || client.writes != 1 {
		t.Fatalf("report=%+v writes=%d", report, client.writes)
	}
	if got := client.objects[reconcileObjectKey(0, 8, 8)]; !bytes.Equal(got, data) {
		t.Fatal("rebuilt object does not match local source")
	}
}

func TestUploadReconcile_ExpiredRecordWithValidRemoteRefreshesOnly(t *testing.T) {
	ctx := context.Background()
	client := newReconcileFakeClient()
	db := newReconcileTestDB(t, client)
	data := mustBuildIncrementalLTX(t, 4, 4, 512, 1, 'c')
	writeReconcileSource(t, db, 0, 4, 4, data)
	client.objects[reconcileObjectKey(0, 4, 4)] = data

	record, err := db.UploadJournal().Load(UploadRecordID(0, 4, 4))
	if err != nil {
		t.Fatal(err)
	}
	record.ExpiresAt = time.Now().Add(-time.Minute)
	if err := db.UploadJournal().Save(record); err != nil {
		t.Fatal(err)
	}

	inspection, err := db.InspectUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Stale != 1 || inspection.Items[0].Decision != UploadDecisionRefreshRecord {
		t.Fatalf("inspection=%+v, want stale/refresh", inspection)
	}

	report, err := db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Stale != 1 || report.Healthy != 0 {
		t.Fatalf("report=%+v", report)
	}
	if client.writes != 0 || client.resumes != 0 || client.deletes != 0 {
		t.Fatalf("expired valid record modified remote: %+v", client)
	}
	updated, err := db.UploadJournal().Load(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != UploadRecordReconciled || time.Until(updated.ExpiresAt) <= 0 {
		t.Fatalf("record not refreshed: %+v", updated)
	}
}

func TestUploadReconcile_FailurePreservesEvidenceAndLaterRestartRecovers(t *testing.T) {
	ctx := context.Background()
	client := newReconcileFakeClient()
	db := newReconcileTestDB(t, client)
	data := mustBuildIncrementalLTX(t, 5, 5, 512, 1, 'd')
	path := writeReconcileSource(t, db, 0, 5, 5, data)
	remoteHalf := append([]byte(nil), data[:7]...)
	client.objects[reconcileObjectKey(0, 5, 5)] = remoteHalf
	client.writeErr = errors.New("remote unavailable")

	report, err := db.ReconcileUploads(ctx)
	if err == nil {
		t.Fatal("expected reconciliation error")
	}
	if report.Failed != 1 || report.Items[0].Diagnosis != SnapshotSelfCheckRecoveryFailed {
		t.Fatalf("report=%+v", report)
	}
	failed, loadErr := db.UploadJournal().Load(UploadRecordID(0, 5, 5))
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if failed.Status != UploadRecordFailed || failed.LastError == "" || failed.FailedAt == nil {
		t.Fatalf("failure not persisted: %+v", failed)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("local evidence not preserved: %v", statErr)
	}
	if got := client.objects[reconcileObjectKey(0, 5, 5)]; !bytes.Equal(got, remoteHalf) {
		t.Fatal("remote partial evidence changed during failed recovery")
	}

	// Restart with remote writes restored. The same ID/key is reused.
	client.writeErr = nil
	report, err = db.ReconcileUploads(ctx)
	if err != nil {
		t.Fatalf("second restart reconcile: %v", err)
	}
	if report.Recovered != 1 || client.writes != 1 {
		t.Fatalf("report=%+v writes=%d", report, client.writes)
	}
	if len(client.objects) != 1 {
		t.Fatalf("expected one object, got %d", len(client.objects))
	}
}
