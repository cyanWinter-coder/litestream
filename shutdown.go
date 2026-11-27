package litestream

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"
)

// shutdownPendingFile is the name of the file persisted under the database
// metadata directory when a process exits without flushing every acknowledged
// write to the replica. Its presence on startup means the local LTX queue must
// be drained before the database accepts new write work.
const shutdownPendingFile = "shutdown-pending.json"

// defaultStartupDrainTimeout bounds the synchronous startup drain. A remote
// that is still unavailable does not block startup forever: the pending state
// stays on disk and the replica monitor keeps retrying in the background.
const defaultStartupDrainTimeout = 30 * time.Second

// PendingLTXFile describes a local LTX file that had not been confirmed on the
// replica when the previous process stopped.
type PendingLTXFile struct {
	Level   int      `json:"level"`
	MinTXID ltx.TXID `json:"min_txid"`
	MaxTXID ltx.TXID `json:"max_txid"`
	// Reason records the last upload failure, if known. Entries without a
	// reason represent work interrupted before an upload completed.
	Reason string `json:"reason,omitempty"`
	// Attempts is the number of failed upload attempts seen so far.
	Attempts int `json:"attempts,omitempty"`
	// LastAttemptAt records when the most recent failure happened.
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`
}

// ShutdownPendingState is the on-disk record of an unfinished shutdown flush.
// It is written only while the local LTX queue is ahead of the replica and is
// removed once the queue has fully drained.
type ShutdownPendingState struct {
	// UpdatedAt is the last time this record was written.
	UpdatedAt time.Time `json:"updated_at"`
	// Reason is the overall shutdown outcome that left work pending
	// (e.g., sync timeout, interrupted signal, remote unavailable).
	Reason string `json:"reason"`
	// Files is the queue of LTX files still awaiting upload, in TXID order.
	Files []PendingLTXFile `json:"files"`
}

// shutdownPendingPath returns the location of the pending-state marker.
func (db *DB) shutdownPendingPath() string {
	return filepath.Join(db.metaPath, shutdownPendingFile)
}

// BeginShutdown flips the database into the rejecting half of the ordered
// shutdown sequence. After it returns, write and WAL-capture entry points fail
// with ErrShuttingDown so callers receive a definitive rejection they can retry
// against the next process instead of having work silently dropped.
//
// It is safe to call repeatedly and from multiple goroutines; only the first
// call transitions the state. It does not flush or close anything itself.
func (db *DB) BeginShutdown() {
	db.shutdownMu.Lock()
	already := db.shuttingDown
	db.shuttingDown = true
	db.shutdownMu.Unlock()

	if !already {
		db.Logger.Info("shutdown initiated, rejecting new writes")
	}
}

// IsShuttingDown reports whether BeginShutdown has been called.
func (db *DB) IsShuttingDown() bool {
	db.shutdownMu.Lock()
	defer db.shutdownMu.Unlock()
	return db.shuttingDown
}

// checkShutting returns ErrShuttingDown once ordered shutdown has begun.
func (db *DB) checkShutting() error {
	if db.IsShuttingDown() {
		return ErrShuttingDown
	}
	return nil
}

// loadShutdownPending reads a pending-state marker. A missing marker returns
// nil with no error so a clean start needs no special-casing.
func (db *DB) loadShutdownPending() (*ShutdownPendingState, error) {
	buf, err := os.ReadFile(db.shutdownPendingPath())
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read shutdown pending state: %w", err)
	}

	var state ShutdownPendingState
	if err := json.Unmarshal(buf, &state); err != nil {
		return nil, fmt.Errorf("parse shutdown pending state: %w", err)
	}
	return &state, nil
}

// saveShutdownPending atomically writes the pending-state marker. The marker is
// the durable claim that local LTX data is not yet safely on the replica.
func (db *DB) saveShutdownPending(state *ShutdownPendingState) error {
	if err := os.MkdirAll(db.metaPath, 0o750); err != nil {
		return fmt.Errorf("create meta directory for shutdown pending state: %w", err)
	}
	state.UpdatedAt = time.Now().Truncate(time.Second)

	buf, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode shutdown pending state: %w", err)
	}

	tmp, err := os.CreateTemp(db.metaPath, "."+shutdownPendingFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create shutdown pending temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(buf); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write shutdown pending state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close shutdown pending temp file: %w", err)
	}
	if err := os.Rename(tmpName, db.shutdownPendingPath()); err != nil {
		return fmt.Errorf("commit shutdown pending state: %w", err)
	}
	return nil
}

// clearShutdownPending removes the pending-state marker after the LTX queue is
// fully uploaded. A missing marker is treated as success.
func (db *DB) clearShutdownPending() error {
	if err := os.Remove(db.shutdownPendingPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove shutdown pending state: %w", err)
	}
	return nil
}

// pendingLTXFiles returns the local level-0 LTX files at or after the replica's
// current position, which are precisely the files still awaiting upload.
func (db *DB) pendingLTXFiles() ([]PendingLTXFile, error) {
	dir := db.LTXLevelDir(0)
	ents, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read local ltx directory: %w", err)
	}

	var fromTXID ltx.TXID = 1
	if db.Replica != nil {
		fromTXID = db.Replica.Pos().TXID + 1
	}

	var pending []PendingLTXFile
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		minTXID, maxTXID, err := ltx.ParseFilename(ent.Name())
		if err != nil {
			continue // not an LTX file
		}
		if maxTXID < fromTXID {
			continue // already confirmed on the replica
		}
		pending = append(pending, PendingLTXFile{
			Level:   0,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
		})
	}

	// ParseFilename yields unordered directory entries; present in TXID order
	// so the persisted queue reads as the upload order.
	for i := 1; i < len(pending); i++ {
		for j := i; j > 0 && pending[j-1].MinTXID > pending[j].MinTXID; j-- {
			pending[j-1], pending[j] = pending[j], pending[j-1]
		}
	}
	return pending, nil
}

// recordShutdownPending snapshots the current upload queue together with the
// reason the flush did not finish and durably persists it. It never treats the
// data as uploaded: the marker survives until a later process drains the queue.
func (db *DB) recordShutdownPending(reason string, cause error) error {
	files, err := db.pendingLTXFiles()
	if err != nil {
		return err
	}
	if len(files) == 0 {
		// Nothing local left to upload; any earlier marker is now stale.
		return db.clearShutdownPending()
	}

	message := reason
	if cause != nil {
		message = fmt.Sprintf("%s: %s", reason, cause.Error())
	}
	now := time.Now().Truncate(time.Second)

	// Preserve failure history from a pre-existing marker (e.g., a crash
	// followed by another failed shutdown) rather than overwriting reasons.
	if prev, err := db.loadShutdownPending(); err == nil && prev != nil {
		mergePendingFailureHistory(prev, files, &now)
		files = prev.Files
	}

	state := &ShutdownPendingState{
		UpdatedAt: now,
		Reason:    message,
		Files:     files,
	}
	if err := db.saveShutdownPending(state); err != nil {
		return err
	}
	db.Logger.Warn("shutdown flush incomplete; local LTX files retained for next startup",
		"pending_files", len(files),
		"reason", message)
	return nil
}

// mergePendingFailureHistory carries per-file failure reasons and attempt
// counts from a previous marker into the freshly computed queue.
func mergePendingFailureHistory(prev *ShutdownPendingState, fresh []PendingLTXFile, now *time.Time) {
	byName := make(map[ltx.TXID]PendingLTXFile, len(prev.Files))
	for _, f := range prev.Files {
		byName[f.MaxTXID] = f
	}
	for i := range fresh {
		if old, ok := byName[fresh[i].MaxTXID]; ok {
			fresh[i].Reason = old.Reason
			fresh[i].Attempts = old.Attempts
			fresh[i].LastAttemptAt = old.LastAttemptAt
			continue
		}
		// Newly queued file that failed during this shutdown attempt.
		fresh[i].Attempts = 1
		fresh[i].LastAttemptAt = now
	}
}

// recordUploadFailure annotates the pending marker with the reason a specific
// LTX upload failed, keeping the failed entry at the front of the queue. It is
// a no-op during normal operation so a transient upload failure does not
// create a shutdown marker: only an unfinished shutdown (or a pre-existing
// marker being retried) is durably recorded here.
func (db *DB) recordUploadFailure(level int, minTXID, maxTXID ltx.TXID, cause error) error {
	state, err := db.loadShutdownPending()
	if err != nil {
		return err
	}
	// No marker yet and we are not shutting down: this is an ordinary
	// transient failure that the replica monitor retries. Do not turn it into
	// a durable shutdown-pending record.
	if state == nil && !db.IsShuttingDown() {
		return nil
	}
	if state == nil {
		files, err := db.pendingLTXFiles()
		if err != nil {
			return err
		}
		state = &ShutdownPendingState{Files: files}
	}

	now := time.Now().Truncate(time.Second)
	matched := false
	for i := range state.Files {
		f := &state.Files[i]
		if f.Level == level && f.MinTXID == minTXID && f.MaxTXID == maxTXID {
			f.Reason = cause.Error()
			f.Attempts++
			f.LastAttemptAt = &now
			matched = true
			break
		}
	}
	if !matched {
		state.Files = append([]PendingLTXFile{{
			Level:         level,
			MinTXID:       minTXID,
			MaxTXID:       maxTXID,
			Reason:        cause.Error(),
			Attempts:      1,
			LastAttemptAt: &now,
		}}, state.Files...)
	}
	state.Reason = "upload failures pending retry"
	return db.saveShutdownPending(state)
}

// drainPendingUploads uploads LTX files left behind by a previous process
// before the database takes new write work. It runs synchronously at startup so
// failed entries are retried ahead of any freshly captured data. A remote that
// remains unavailable leaves the marker in place and returns an error; the
// replica monitor continues retrying without ever treating the data as synced.
func (db *DB) drainPendingUploads(ctx context.Context) error {
	state, err := db.loadShutdownPending()
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if db.Replica == nil {
		return fmt.Errorf("shutdown pending state exists but no replica is configured")
	}

	db.Logger.Info("draining pending LTX uploads from previous process before accepting writes",
		"pending_files", len(state.Files),
		"reason", state.Reason)

	drainCtx, cancel := context.WithTimeout(ctx, defaultStartupDrainTimeout)
	defer cancel()

	if err := db.Replica.Sync(drainCtx); err != nil {
		// Keep the marker and surface the failure reason on the still-queued
		// entries; background monitoring will keep trying.
		_ = db.notePendingFailureFromSync(err)
		return fmt.Errorf("drain pending uploads: %w", err)
	}

	// Replica.Sync advances through every local L0 file, so a clean return
	// means the queue is empty relative to the local position.
	if err := db.clearShutdownPending(); err != nil {
		return err
	}
	db.Logger.Info("pending LTX uploads drained")
	return nil
}

// notePendingFailureFromSync refreshes the persisted marker after a failed
// startup drain so the failure reason is retained with the queue.
func (db *DB) notePendingFailureFromSync(cause error) error {
	files, err := db.pendingLTXFiles()
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	now := time.Now().Truncate(time.Second)
	for i := range files {
		files[i].Reason = cause.Error()
		files[i].LastAttemptAt = &now
		if files[i].Attempts == 0 {
			files[i].Attempts = 1
		}
	}
	return db.saveShutdownPending(&ShutdownPendingState{
		Reason: "startup drain incomplete: " + cause.Error(),
		Files:  files,
	})
}

// HasPendingUploads reports whether a durable pending-state marker exists.
func (db *DB) HasPendingUploads() bool {
	state, err := db.loadShutdownPending()
	if err != nil {
		db.Logger.Warn("cannot read shutdown pending state", "error", err)
		return false
	}
	return state != nil && len(state.Files) > 0
}
