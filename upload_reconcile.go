package litestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

// Upload journal and startup reconciliation states.
const (
	UploadJournalDirName         = "uploads"
	UploadReconciliationFileName = "upload-reconciliation.json"
	DefaultUploadRecordTTL       = 7 * 24 * time.Hour

	uploadRecordFileSuffix = ".json"
)

// UploadRecordStatus is the lifecycle state of an upload record.
type UploadRecordStatus string

const (
	UploadRecordPending    UploadRecordStatus = "pending"
	UploadRecordUploading  UploadRecordStatus = "uploading"
	UploadRecordCompleted  UploadRecordStatus = "completed"
	UploadRecordReconciled UploadRecordStatus = "reconciled"
	UploadRecordRecovering UploadRecordStatus = "recovering"
	UploadRecordFailed     UploadRecordStatus = "failed"
	UploadRecordSuperseded UploadRecordStatus = "superseded"
)

// SnapshotSelfCheckDiagnosis classifies a startup self-check result.
type SnapshotSelfCheckDiagnosis string

const (
	SnapshotSelfCheckHealthy          SnapshotSelfCheckDiagnosis = "healthy"
	SnapshotSelfCheckRemoteMissing    SnapshotSelfCheckDiagnosis = "remote_missing"
	SnapshotSelfCheckRemoteCorrupt    SnapshotSelfCheckDiagnosis = "remote_corrupt"
	SnapshotSelfCheckRecordExpired    SnapshotSelfCheckDiagnosis = "local_record_expired"
	SnapshotSelfCheckInspectionFailed SnapshotSelfCheckDiagnosis = "inspection_failed"
	SnapshotSelfCheckRecoveryFailed   SnapshotSelfCheckDiagnosis = "recovery_failed"
)

// RemoteObjectCondition describes the object named by an upload record.
type RemoteObjectCondition string

const (
	RemoteObjectUnknown RemoteObjectCondition = "unknown"
	RemoteObjectValid   RemoteObjectCondition = "valid"
	RemoteObjectMissing RemoteObjectCondition = "missing"
	RemoteObjectCorrupt RemoteObjectCondition = "corrupt"
)

// UploadRecoveryDecision is the persisted recovery decision.
type UploadRecoveryDecision string

const (
	UploadDecisionNone          UploadRecoveryDecision = "none"
	UploadDecisionRefreshRecord UploadRecoveryDecision = "refresh_record"
	UploadDecisionResume        UploadRecoveryDecision = "resume"
	UploadDecisionRebuild       UploadRecoveryDecision = "rebuild"
	UploadDecisionManual        UploadRecoveryDecision = "manual_intervention"
)

// Reconciliation errors.
var (
	ErrUploadRecordIDInvalid        = errors.New("invalid upload record ID")
	ErrUploadReconciliationFailed   = errors.New("upload reconciliation failed")
	ErrUploadRecoveryNotImplemented = errors.New("upload recovery source is unavailable")
)

// UploadRecord is the persistent local record for one identifiable upload.
// ID and ObjectKey provide the shared reconciliation identity.
type UploadRecord struct {
	ID        string `json:"id"`
	Kind      string `json:"kind,omitempty"`
	ObjectKey string `json:"object_key,omitempty"`

	Level   int    `json:"level,omitempty"`
	MinTXID uint64 `json:"min_txid,omitempty"`
	MaxTXID uint64 `json:"max_txid,omitempty"`

	LocalPath string `json:"local_path,omitempty"`

	ExpectedSize     int64  `json:"expected_size,omitempty"`
	ExpectedChecksum string `json:"expected_checksum_sha256,omitempty"`
	UploadToken      string `json:"upload_token,omitempty"`

	Status       UploadRecordStatus `json:"status"`
	AttemptCount int                `json:"attempt_count,omitempty"`
	ExpiresAt    time.Time          `json:"expires_at,omitempty"`

	RemoteCondition RemoteObjectCondition `json:"remote_condition,omitempty"`
	RemoteSize      int64                 `json:"remote_size,omitempty"`
	RemoteETag      string                `json:"remote_etag,omitempty"`
	ValidationError string                `json:"validation_error,omitempty"`

	Diagnosis SnapshotSelfCheckDiagnosis `json:"diagnosis,omitempty"`
	Decision  UploadRecoveryDecision     `json:"decision,omitempty"`
	Reason    string                     `json:"reason,omitempty"`

	// LastRecoveryDecision/Reason preserve the most recent repair action for
	// operators after the current startup's Decision returns to "none".
	LastRecoveryDecision UploadRecoveryDecision `json:"last_recovery_decision,omitempty"`
	LastRecoveryReason   string                 `json:"last_recovery_reason,omitempty"`

	LastError     string     `json:"last_error,omitempty"`
	FailureCount  int        `json:"failure_count,omitempty"`
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	FailedAt      *time.Time `json:"failed_at,omitempty"`
	RecoveredAt   *time.Time `json:"recovered_at,omitempty"`
	ReconciledAt  *time.Time `json:"reconciled_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Expired reports whether the local completion claim has expired.
func (r *UploadRecord) Expired(now time.Time) bool {
	return r != nil && !r.ExpiresAt.IsZero() && now.After(r.ExpiresAt)
}

// UploadRecordID returns the deterministic ID shared by journal and report.
func UploadRecordID(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("ltx-L%d-%016x-%016x", level, uint64(minTXID), uint64(maxTXID))
}

func uploadObjectKey(level int, minTXID, maxTXID ltx.TXID) string {
	return LTXFilePath("", level, minTXID, maxTXID)
}

// UploadJournal stores one atomic JSON record per upload ID.
type UploadJournal struct {
	dir string
	mu  sync.Mutex
	now func() time.Time
}

// NewUploadJournal creates a journal rooted at metaPath/uploads.
func NewUploadJournal(metaPath string) *UploadJournal {
	return &UploadJournal{dir: filepath.Join(metaPath, UploadJournalDirName), now: time.Now}
}

// Dir returns the journal directory.
func (j *UploadJournal) Dir() string { return j.dir }

func uploadRecordFileName(id string) (string, error) {
	if id == "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return "", fmt.Errorf("%w: %q", ErrUploadRecordIDInvalid, id)
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return "", fmt.Errorf("%w: %q", ErrUploadRecordIDInvalid, id)
	}
	return id + uploadRecordFileSuffix, nil
}

// Path returns the on-disk path for an ID.
func (j *UploadJournal) Path(id string) (string, error) {
	name, err := uploadRecordFileName(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(j.dir, name), nil
}

// Save atomically persists record.
func (j *UploadJournal) Save(record *UploadRecord) error {
	if record == nil {
		return errors.New("upload record is nil")
	}
	name, err := uploadRecordFileName(record.ID)
	if err != nil {
		return err
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	now := j.now()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now

	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal upload record %s: %w", record.ID, err)
	}
	b = append(b, '\n')

	if err := internal.MkdirAll(j.dir, nil); err != nil {
		return fmt.Errorf("create upload journal: %w", err)
	}

	finalPath := filepath.Join(j.dir, name)
	tmpPath := finalPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("create upload record %s: %w", record.ID, err)
	}
	_, writeErr := f.Write(b)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write upload record %s: %w", record.ID, writeErr)
	}
	if syncErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync upload record %s: %w", record.ID, syncErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close upload record %s: %w", record.ID, closeErr)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("publish upload record %s: %w", record.ID, err)
	}
	if err := internal.FsyncDir(j.dir); err != nil {
		return fmt.Errorf("sync upload journal directory: %w", err)
	}
	return nil
}

// Load reads one record.
func (j *UploadJournal) Load(id string) (*UploadRecord, error) {
	path, err := j.Path(id)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var record UploadRecord
	if err := json.Unmarshal(b, &record); err != nil {
		return nil, fmt.Errorf("parse upload record %s: %w", id, err)
	}
	return &record, nil
}

// List reads all records. Malformed JSON is returned, not skipped, so startup
// cannot silently ignore an unknown upload state.
func (j *UploadJournal) List() ([]*UploadRecord, error) {
	entries, err := os.ReadDir(j.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read upload journal: %w", err)
	}

	records := make([]*UploadRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), uploadRecordFileSuffix) {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), uploadRecordFileSuffix)
		record, err := j.Load(id)
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}
	return records, nil
}

// UploadReconciliationItem is one reconciliation report row.
type UploadReconciliationItem struct {
	ID                   string                     `json:"id"`
	ObjectKey            string                     `json:"object_key,omitempty"`
	Status               UploadRecordStatus         `json:"status"`
	Diagnosis            SnapshotSelfCheckDiagnosis `json:"diagnosis"`
	RemoteCondition      RemoteObjectCondition      `json:"remote_condition"`
	Decision             UploadRecoveryDecision     `json:"decision"`
	Reason               string                     `json:"reason"`
	LastRecoveryDecision UploadRecoveryDecision     `json:"last_recovery_decision,omitempty"`
	LastRecoveryReason   string                     `json:"last_recovery_reason,omitempty"`
	RemoteSize           int64                      `json:"remote_size,omitempty"`
	ExpectedSize         int64                      `json:"expected_size,omitempty"`
	Recovered            bool                       `json:"recovered"`
	Error                string                     `json:"error,omitempty"`
	Record               *UploadRecord              `json:"record,omitempty"`
}

// UploadReconciliationReport is visible at startup and via status output.
type UploadReconciliationReport struct {
	StartedAt  time.Time                  `json:"started_at"`
	FinishedAt time.Time                  `json:"finished_at"`
	FatalError string                     `json:"fatal_error,omitempty"`
	Total      int                        `json:"total"`
	Healthy    int                        `json:"healthy"`
	Missing    int                        `json:"remote_missing"`
	Corrupt    int                        `json:"remote_corrupt"`
	Stale      int                        `json:"local_record_expired"`
	Recovered  int                        `json:"recovered"`
	Failed     int                        `json:"failed"`
	Items      []UploadReconciliationItem `json:"items"`
}

// Err returns a joined error for failed items.
func (r *UploadReconciliationReport) Err() error {
	if r == nil {
		return nil
	}
	if r.FatalError != "" && r.Failed == 0 {
		return fmt.Errorf("%w: %s", ErrUploadReconciliationFailed, r.FatalError)
	}
	if r.Failed == 0 {
		return nil
	}
	errs := make([]error, 0, r.Failed)
	for _, item := range r.Items {
		if item.Diagnosis == SnapshotSelfCheckInspectionFailed || item.Diagnosis == SnapshotSelfCheckRecoveryFailed {
			if item.Error != "" {
				errs = append(errs, fmt.Errorf("%s: %s", item.ID, item.Error))
			}
		}
	}
	if len(errs) == 0 {
		errs = append(errs, fmt.Errorf("%d upload reconciliation item(s) failed", r.Failed))
	}
	return fmt.Errorf("%w: %w", ErrUploadReconciliationFailed, errors.Join(errs...))
}

// ResumableLTXReplicaClient is an optional backend capability for completing a
// verified partial prefix instead of rebuilding the object.
type ResumableLTXReplicaClient interface {
	ResumeLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, uploadID string, resumeOffset int64, r io.Reader) (*ltx.FileInfo, error)
}

func (db *DB) journalLogger() *slog.Logger {
	if db.Logger != nil {
		return db.Logger
	}
	return slog.Default()
}

func (db *DB) getUploadJournal() *UploadJournal {
	db.uploadJournalMu.Lock()
	defer db.uploadJournalMu.Unlock()
	dir := filepath.Join(db.metaPath, UploadJournalDirName)
	if db.uploadJournal == nil || db.uploadJournal.dir != dir {
		db.uploadJournal = NewUploadJournal(db.metaPath)
	}
	return db.uploadJournal
}

// UploadJournal returns the DB-owned upload journal.
func (db *DB) UploadJournal() *UploadJournal { return db.getUploadJournal() }

// UploadReconciliationReportPath returns the persisted startup report path.
func (db *DB) UploadReconciliationReportPath() string {
	return filepath.Join(db.metaPath, UploadReconciliationFileName)
}

// LastUploadReconciliation returns the latest in-memory report.
func (db *DB) LastUploadReconciliation() *UploadReconciliationReport {
	db.uploadReconcileMu.RLock()
	defer db.uploadReconcileMu.RUnlock()
	return db.lastUploadReconciliation
}

// ReadUploadReconciliationReport reads the startup report from disk.
func (db *DB) ReadUploadReconciliationReport() (*UploadReconciliationReport, error) {
	b, err := os.ReadFile(db.UploadReconciliationReportPath())
	if err != nil {
		return nil, err
	}
	var report UploadReconciliationReport
	if err := json.Unmarshal(b, &report); err != nil {
		return nil, fmt.Errorf("parse upload reconciliation report: %w", err)
	}
	return &report, nil
}

func (db *DB) saveUploadReconciliationReport(report *UploadReconciliationReport) error {
	if report == nil {
		return nil
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	path := db.UploadReconciliationReportPath()
	if err := internal.MkdirAll(filepath.Dir(path), db.dirInfo); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(b)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = os.Remove(tmp)
		return writeErr
	}
	if syncErr != nil {
		_ = os.Remove(tmp)
		return syncErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := internal.FsyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	db.uploadReconcileMu.Lock()
	db.lastUploadReconciliation = report
	db.uploadReconcileMu.Unlock()
	return nil
}

// InspectUploads runs the read-only startup self-check. It only lists/reads
// remote data; it never writes, deletes, or repairs remote objects.
func (db *DB) InspectUploads(ctx context.Context) (*UploadReconciliationReport, error) {
	records, err := db.getUploadJournal().List()
	if err != nil {
		now := time.Now().UTC()
		report := &UploadReconciliationReport{
			StartedAt:  now,
			FinishedAt: now,
			FatalError: err.Error(),
			Items:      []UploadReconciliationItem{},
		}
		return report, err
	}
	report := &UploadReconciliationReport{
		StartedAt: time.Now().UTC(),
		Items:     make([]UploadReconciliationItem, 0, len(records)),
	}
	for _, record := range records {
		item := db.inspectUploadRecord(ctx, record)
		report.Items = append(report.Items, item)
	}
	finishUploadReport(report)
	return report, nil
}

func finishUploadReport(report *UploadReconciliationReport) {
	report.FinishedAt = time.Now().UTC()
	report.Total = len(report.Items)
	report.Healthy, report.Missing, report.Corrupt, report.Stale, report.Recovered, report.Failed = 0, 0, 0, 0, 0, 0
	for _, item := range report.Items {
		switch item.Diagnosis {
		case SnapshotSelfCheckHealthy:
			report.Healthy++
		case SnapshotSelfCheckRemoteMissing:
			report.Missing++
		case SnapshotSelfCheckRemoteCorrupt:
			report.Corrupt++
		case SnapshotSelfCheckRecordExpired:
			report.Stale++
		case SnapshotSelfCheckInspectionFailed, SnapshotSelfCheckRecoveryFailed:
			report.Failed++
		}
		if item.Recovered {
			report.Recovered++
		}
	}
}

func (db *DB) inspectUploadRecord(ctx context.Context, record *UploadRecord) UploadReconciliationItem {
	item := UploadReconciliationItem{
		ID:                   record.ID,
		ObjectKey:            record.ObjectKey,
		Status:               record.Status,
		Decision:             record.Decision,
		Reason:               record.Reason,
		LastRecoveryDecision: record.LastRecoveryDecision,
		LastRecoveryReason:   record.LastRecoveryReason,
		ExpectedSize:         record.ExpectedSize,
		Record:               record,
	}
	stale := record.Expired(time.Now())

	if db.Replica == nil || db.Replica.Client == nil {
		err := errors.New("replica client is not configured")
		item.Diagnosis = SnapshotSelfCheckInspectionFailed
		item.Error = err.Error()
		record.Diagnosis = item.Diagnosis
		record.LastError = err.Error()
		return item
	}

	info, condition, validationErr := db.checkRemoteLTX(ctx, record)
	record.RemoteCondition = condition
	if info != nil {
		record.RemoteSize = info.Size
		item.RemoteSize = info.Size
	}
	item.RemoteCondition = condition

	switch {
	case validationErr != nil && condition == RemoteObjectUnknown:
		item.Diagnosis = SnapshotSelfCheckInspectionFailed
		item.Error = validationErr.Error()
		record.Diagnosis = item.Diagnosis
		record.LastError = validationErr.Error()
		return item
	case condition == RemoteObjectMissing:
		item.Diagnosis = SnapshotSelfCheckRemoteMissing
		item.Decision = UploadDecisionRebuild
		item.Reason = "local record names upload " + record.ID + " but remote object is missing; re-upload the same deterministic key"
	case condition == RemoteObjectCorrupt:
		item.Diagnosis = SnapshotSelfCheckRemoteCorrupt
		if validationErr != nil {
			record.ValidationError = validationErr.Error()
		}
		item.Reason = "remote object exists but is incomplete or corrupt: " + record.ValidationError
		item.Decision = db.corruptUploadDecision(ctx, record, info)
	case stale:
		item.Diagnosis = SnapshotSelfCheckRecordExpired
		if condition == RemoteObjectValid {
			item.Decision = UploadDecisionRefreshRecord
			item.Reason = "local upload record has expired but remote object passed validation; adopt remote object and refresh record"
		} else {
			item.Decision = UploadDecisionRebuild
			item.Reason = "local upload record has expired and remote object is not valid; do not reuse its resumable token"
		}
	default:
		item.Diagnosis = SnapshotSelfCheckHealthy
		item.Decision = UploadDecisionNone
		item.Reason = "local record and remote object reconcile under ID " + record.ID
	}

	if stale && item.Diagnosis != SnapshotSelfCheckRecordExpired {
		item.Reason = "local record is expired; " + item.Reason
	}

	record.Diagnosis = item.Diagnosis
	record.Decision = item.Decision
	record.Reason = item.Reason
	item.Status = record.Status
	return item
}

// checkRemoteLTX only performs list/open/read calls.
func (db *DB) checkRemoteLTX(ctx context.Context, record *UploadRecord) (*ltx.FileInfo, RemoteObjectCondition, error) {
	minTXID, maxTXID := ltx.TXID(record.MinTXID), ltx.TXID(record.MaxTXID)
	itr, err := db.Replica.Client.LTXFiles(ctx, record.Level, minTXID, false)
	if err != nil {
		return nil, RemoteObjectUnknown, fmt.Errorf("list remote ltx files: %w", err)
	}
	defer func() { _ = itr.Close() }()

	var info *ltx.FileInfo
	for itr.Next() {
		candidate := itr.Item()
		if candidate.Level == record.Level && candidate.MinTXID == minTXID && candidate.MaxTXID == maxTXID {
			info = candidate
			break
		}
	}
	if err := itr.Err(); err != nil {
		return nil, RemoteObjectUnknown, fmt.Errorf("iterate remote ltx files: %w", err)
	}
	if err := itr.Close(); err != nil {
		return nil, RemoteObjectUnknown, fmt.Errorf("close remote ltx iterator: %w", err)
	}
	if info == nil {
		return nil, RemoteObjectMissing, nil
	}

	if record.ExpectedSize > 0 && info.Size != record.ExpectedSize {
		err := fmt.Errorf("size mismatch: remote=%d expected=%d", info.Size, record.ExpectedSize)
		return info, RemoteObjectCorrupt, err
	}

	rc, err := db.Replica.Client.OpenLTXFile(ctx, record.Level, minTXID, maxTXID, 0, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return info, RemoteObjectMissing, nil
		}
		return info, RemoteObjectCorrupt, fmt.Errorf("open remote ltx file: %w", err)
	}
	defer func() { _ = rc.Close() }()

	dec := ltx.NewDecoder(rc)
	if err := dec.Verify(); err != nil {
		return info, RemoteObjectCorrupt, fmt.Errorf("verify remote ltx file: %w", err)
	}
	if err := rc.Close(); err != nil {
		return info, RemoteObjectCorrupt, fmt.Errorf("close validated remote ltx file: %w", err)
	}
	return info, RemoteObjectValid, nil
}

func (db *DB) corruptUploadDecision(ctx context.Context, record *UploadRecord, info *ltx.FileInfo) UploadRecoveryDecision {
	if record.Expired(time.Now()) {
		record.Reason = "expired record cannot safely identify a resumable prefix"
		return UploadDecisionRebuild
	}
	if info == nil || info.Size <= 0 || record.ExpectedSize <= 0 || info.Size >= record.ExpectedSize {
		record.Reason = "corrupt object is not a shorter resumable prefix"
		return UploadDecisionRebuild
	}
	if _, ok := db.Replica.Client.(ResumableLTXReplicaClient); !ok {
		record.Reason = "remote object is a partial prefix but client does not support resumable LTX upload"
		return UploadDecisionRebuild
	}
	if record.LocalPath == "" {
		record.Reason = "remote object is a partial prefix but local source path is unavailable"
		return UploadDecisionRebuild
	}
	matches, err := db.remotePrefixMatchesLocal(ctx, record, info.Size)
	if err != nil || !matches {
		record.Reason = "remote prefix does not byte-match local source; resume would append at the wrong offset"
		return UploadDecisionRebuild
	}
	record.Reason = "verified remote prefix can be completed in place"
	return UploadDecisionResume
}

func (db *DB) remotePrefixMatchesLocal(ctx context.Context, record *UploadRecord, prefixSize int64) (bool, error) {
	local, err := os.Open(record.LocalPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = local.Close() }()

	remote, err := db.Replica.Client.OpenLTXFile(ctx, record.Level, ltx.TXID(record.MinTXID), ltx.TXID(record.MaxTXID), 0, prefixSize)
	if err != nil {
		return false, err
	}
	defer func() { _ = remote.Close() }()

	const bufSize = 32 * 1024
	localBuf := make([]byte, bufSize)
	remoteBuf := make([]byte, bufSize)
	remaining := prefixSize
	for remaining > 0 {
		n := int64(bufSize)
		if remaining < n {
			n = remaining
		}
		if _, err := io.ReadFull(local, localBuf[:n]); err != nil {
			return false, err
		}
		if _, err := io.ReadFull(remote, remoteBuf[:n]); err != nil {
			return false, err
		}
		if string(localBuf[:n]) != string(remoteBuf[:n]) {
			return false, nil
		}
		remaining -= n
	}
	return true, nil
}

// ReconcileUploads inspects all records and executes their persisted decisions.
func (db *DB) ReconcileUploads(ctx context.Context) (*UploadReconciliationReport, error) {
	report, inspectErr := db.InspectUploads(ctx)
	if inspectErr != nil {
		if report == nil {
			now := time.Now().UTC()
			report = &UploadReconciliationReport{StartedAt: now, FinishedAt: now}
		}
		report.FatalError = inspectErr.Error()
		finishUploadReport(report)
		if saveErr := db.saveUploadReconciliationReport(report); saveErr != nil {
			return report, errors.Join(inspectErr, fmt.Errorf("save upload reconciliation report: %w", saveErr))
		}
		db.logUploadReconciliationReport(report)
		return report, report.Err()
	}

	for i := range report.Items {
		item := &report.Items[i]
		record := item.Record
		if record == nil {
			continue
		}

		if item.Diagnosis == SnapshotSelfCheckInspectionFailed {
			if stateErr := db.failUploadRecord(record, errors.New(item.Error)); stateErr != nil {
				item.Error = stateErr.Error()
			}
			item.Status = record.Status
			continue
		}

		if item.Decision == UploadDecisionNone || item.Decision == UploadDecisionRefreshRecord {
			now := time.Now().UTC()
			record.Status = UploadRecordReconciled
			record.ReconciledAt = &now
			record.ExpiresAt = now.Add(DefaultUploadRecordTTL)
			record.LastError = ""
			if err := db.getUploadJournal().Save(record); err != nil {
				item.Diagnosis = SnapshotSelfCheckRecoveryFailed
				item.Error = err.Error()
			}
			item.Status = record.Status
			continue
		}

		if err := db.recoverUploadRecord(ctx, record); err != nil {
			if stateErr := db.failUploadRecord(record, err); stateErr != nil {
				err = stateErr
			}
			item.Diagnosis = SnapshotSelfCheckRecoveryFailed
			item.Decision = record.Decision
			item.Reason = record.Reason
			item.LastRecoveryDecision = record.LastRecoveryDecision
			item.LastRecoveryReason = record.LastRecoveryReason
			item.Error = err.Error()
			item.Status = record.Status
			db.journalLogger().Error("snapshot upload recovery failed; evidence preserved",
				"upload_id", record.ID, "object_key", record.ObjectKey,
				"decision", record.Decision, "error", err)
			continue
		}

		item.Recovered = true
		item.Error = ""
		item.Diagnosis = SnapshotSelfCheckHealthy
		item.RemoteCondition = RemoteObjectValid
		item.Status = UploadRecordReconciled
		item.Decision = record.Decision
		item.Reason = record.Reason
		item.LastRecoveryDecision = record.LastRecoveryDecision
		item.LastRecoveryReason = record.LastRecoveryReason
	}

	finishUploadReport(report)
	if err := db.saveUploadReconciliationReport(report); err != nil {
		return report, fmt.Errorf("save upload reconciliation report: %w", err)
	}
	db.logUploadReconciliationReport(report)
	return report, report.Err()
}

func (db *DB) startupReconcileUploads() error {
	ctx := db.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	records, err := db.getUploadJournal().List()
	if err != nil {
		// Reconcile persists a fatal startup report instead of only returning
		// the parse/read error to DB.Open.
		if _, reconcileErr := db.ReconcileUploads(ctx); reconcileErr != nil {
			return reconcileErr
		}
		return fmt.Errorf("load upload journal: %w", err)
	}
	if len(records) == 0 {
		return nil
	}

	// Snapshot rebuilds need an initialized page size. Initialize through the
	// normal executor choke point and release it before reconciliation, which
	// calls Snapshot and acquires the executor lock itself.
	if _, statErr := os.Stat(db.path); statErr == nil && db.PageSize() == 0 {
		if err := db.lockExec(ctx); err != nil {
			return fmt.Errorf("lock database for reconciliation init: %w", err)
		}
		exec, initErr := db.newSyncExecutor(ctx)
		db.execSem.Release(1)
		if initErr != nil {
			return fmt.Errorf("initialize database for upload reconciliation: %w", initErr)
		}
		_ = exec
	}

	report, err := db.ReconcileUploads(ctx)
	if err != nil {
		return err
	}
	if report.Total > 0 {
		db.journalLogger().Info("startup upload self-check complete", "total", report.Total)
	}
	return nil
}

func (db *DB) logUploadReconciliationReport(report *UploadReconciliationReport) {
	if report == nil || report.Total == 0 {
		return
	}
	logger := db.Logger
	if logger == nil {
		logger = slog.Default()
	}
	level := slog.LevelInfo
	if report.Failed > 0 || report.FatalError != "" {
		level = slog.LevelError
	}
	logger.Log(context.Background(), level, "startup upload reconciliation complete",
		"total", report.Total, "healthy", report.Healthy,
		"remote_missing", report.Missing, "remote_corrupt", report.Corrupt,
		"local_record_expired", report.Stale, "recovered", report.Recovered,
		"failed", report.Failed, "fatal_error", report.FatalError,
		"report", db.UploadReconciliationReportPath())
	for _, item := range report.Items {
		logger.Log(context.Background(), level, "upload reconciliation item",
			"upload_id", item.ID, "object_key", item.ObjectKey,
			"diagnosis", item.Diagnosis, "decision", item.Decision,
			"reason", item.Reason, "recovered", item.Recovered, "error", item.Error)
	}
}

func (db *DB) recoverUploadRecord(ctx context.Context, record *UploadRecord) error {
	now := time.Now().UTC()
	record.Status = UploadRecordRecovering
	record.LastAttemptAt = &now
	record.AttemptCount++
	record.LastRecoveryDecision = record.Decision
	record.LastRecoveryReason = record.Reason
	if err := db.getUploadJournal().Save(record); err != nil {
		return fmt.Errorf("persist recovery decision: %w", err)
	}

	switch record.Decision {
	case UploadDecisionResume:
		if err := db.resumeUploadRecord(ctx, record); err != nil {
			record.Decision = UploadDecisionRebuild
			record.Reason = "resume failed (" + err.Error() + "); rebuilding same deterministic object"
			record.LastRecoveryDecision = record.Decision
			record.LastRecoveryReason = record.Reason
			if saveErr := db.getUploadJournal().Save(record); saveErr != nil {
				return errors.Join(err, fmt.Errorf("persist rebuild decision: %w", saveErr))
			}
			if err := db.rebuildUploadRecord(ctx, record); err != nil {
				return err
			}
		}
	case UploadDecisionRebuild:
		if err := db.rebuildUploadRecord(ctx, record); err != nil {
			return err
		}
	default:
		unsupported := record.Decision
		record.Decision = UploadDecisionManual
		record.Reason = "unsupported recovery decision " + string(unsupported)
		return fmt.Errorf("%w: decision %q", ErrUploadRecoveryNotImplemented, unsupported)
	}
	return db.verifyAndCompleteUploadRecord(ctx, record)
}

func (db *DB) resumeUploadRecord(ctx context.Context, record *UploadRecord) error {
	client, ok := db.Replica.Client.(ResumableLTXReplicaClient)
	if !ok {
		return errors.New("client does not implement ResumableLTXReplicaClient")
	}
	if record.RemoteSize <= 0 {
		return errors.New("resume offset is zero")
	}
	f, err := os.Open(record.LocalPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(record.RemoteSize, io.SeekStart); err != nil {
		return err
	}
	_, err = client.ResumeLTXFile(ctx, record.Level, ltx.TXID(record.MinTXID), ltx.TXID(record.MaxTXID), record.ID, record.RemoteSize, f)
	return err
}

func (db *DB) rebuildUploadRecord(ctx context.Context, record *UploadRecord) error {
	if record.LocalPath != "" {
		f, err := os.Open(record.LocalPath)
		if err == nil {
			defer func() { _ = f.Close() }()
			info, writeErr := db.Replica.Client.WriteLTXFile(ctx, record.Level, ltx.TXID(record.MinTXID), ltx.TXID(record.MaxTXID), f)
			if writeErr != nil {
				return fmt.Errorf("re-upload local ltx source: %w", writeErr)
			}
			db.applyUploadInfo(record, info)
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("open local source: %w", err)
		}
	}

	if record.Level == SnapshotLevel && db.db != nil {
		info, err := db.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("regenerate snapshot: %w", err)
		}
		db.applyUploadInfo(record, info)
		if info != nil && (info.MinTXID != ltx.TXID(record.MinTXID) || info.MaxTXID != ltx.TXID(record.MaxTXID)) {
			now := time.Now().UTC()
			record.Status = UploadRecordSuperseded
			record.Diagnosis = SnapshotSelfCheckHealthy
			record.Reason = fmt.Sprintf("current snapshot is %s-%s; stale upload %s preserved", info.MinTXID, info.MaxTXID, record.ID)
			record.ReconciledAt = &now
			return db.getUploadJournal().Save(record)
		}
		return nil
	}

	record.Decision = UploadDecisionManual
	record.Reason = "no local LTX source or live snapshot generator is available; remote/local evidence preserved for operator intervention"
	record.LastRecoveryDecision = record.Decision
	record.LastRecoveryReason = record.Reason
	_ = db.getUploadJournal().Save(record)
	return fmt.Errorf("%w: %s", ErrUploadRecoveryNotImplemented, record.ID)
}

func (db *DB) verifyAndCompleteUploadRecord(ctx context.Context, record *UploadRecord) error {
	info, condition, err := db.checkRemoteLTX(ctx, record)
	if err != nil && condition == RemoteObjectUnknown {
		return err
	}
	if condition != RemoteObjectValid {
		if err == nil {
			err = fmt.Errorf("remote condition %s", condition)
		}
		return fmt.Errorf("recovered upload failed verification: %w", err)
	}

	now := time.Now().UTC()
	db.applyUploadInfo(record, info)
	record.Status = UploadRecordReconciled
	record.RemoteCondition = RemoteObjectValid
	record.Diagnosis = SnapshotSelfCheckHealthy
	// Keep Decision as the completed repair action for this startup. The
	// persistent LastRecovery* fields survive later healthy self-checks.
	record.LastRecoveryDecision = record.Decision
	record.LastRecoveryReason = record.Reason
	record.Reason = record.Reason + "; recovery verified under ID " + record.ID
	record.LastError = ""
	record.FailedAt = nil
	record.ReconciledAt = &now
	record.RecoveredAt = &now
	record.ExpiresAt = now.Add(DefaultUploadRecordTTL)
	return db.getUploadJournal().Save(record)
}

func (db *DB) applyUploadInfo(record *UploadRecord, info *ltx.FileInfo) {
	if info == nil {
		return
	}
	record.Level = info.Level
	record.MinTXID = uint64(info.MinTXID)
	record.MaxTXID = uint64(info.MaxTXID)
	record.RemoteSize = info.Size
	if record.ExpectedSize == 0 {
		record.ExpectedSize = info.Size
	}
	if record.ObjectKey == "" {
		record.ObjectKey = uploadObjectKey(info.Level, info.MinTXID, info.MaxTXID)
	}
}

func (db *DB) failUploadRecord(record *UploadRecord, cause error) error {
	now := time.Now().UTC()
	record.Status = UploadRecordFailed
	record.Diagnosis = SnapshotSelfCheckRecoveryFailed
	record.LastError = cause.Error()
	record.FailureCount++
	record.FailedAt = &now
	if saveErr := db.getUploadJournal().Save(record); saveErr != nil {
		logger := db.journalLogger()
		logger.Error("failed to persist upload failure state", "upload_id", record.ID, "error", saveErr)
		return errors.Join(cause, fmt.Errorf("persist failure state: %w", saveErr))
	}
	return cause
}

func (db *DB) beginLTXUploadRecord(level int, minTXID, maxTXID ltx.TXID, localPath string) (*UploadRecord, error) {
	id := UploadRecordID(level, minTXID, maxTXID)
	record, err := db.getUploadJournal().Load(id)
	if err != nil {
		record = &UploadRecord{ID: id, Kind: "ltx"}
	}
	record.Kind = "ltx"
	record.Level = level
	record.MinTXID = uint64(minTXID)
	record.MaxTXID = uint64(maxTXID)
	if record.ObjectKey == "" {
		record.ObjectKey = uploadObjectKey(level, minTXID, maxTXID)
	}
	if localPath != "" {
		record.LocalPath = localPath
		if fi, statErr := os.Stat(localPath); statErr == nil {
			record.ExpectedSize = fi.Size()
		}
	}
	record.Status = UploadRecordUploading
	record.AttemptCount++
	now := time.Now().UTC()
	record.LastAttemptAt = &now
	if record.ExpiresAt.IsZero() {
		record.ExpiresAt = now.Add(DefaultUploadRecordTTL)
	}
	if err := db.getUploadJournal().Save(record); err != nil {
		return record, fmt.Errorf("persist upload start record %s: %w", id, err)
	}
	return record, nil
}

func (db *DB) completeLTXUploadRecord(record *UploadRecord, info *ltx.FileInfo) error {
	if record == nil {
		return errors.New("complete nil upload record")
	}
	now := time.Now().UTC()
	db.applyUploadInfo(record, info)
	record.Status = UploadRecordCompleted
	record.CompletedAt = &now
	record.ExpiresAt = now.Add(DefaultUploadRecordTTL)
	return db.getUploadJournal().Save(record)
}
