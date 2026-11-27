package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/integrity"
	"github.com/benbjohnson/litestream/internal"
)

const integrityDirName = ".integrity"

func integrityFileID(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("ltx:L%d:%016x-%016x", level, uint64(minTXID), uint64(maxTXID))
}

func (c *ReplicaClient) integrityRoot() string {
	return filepath.Join(c.path, integrityDirName)
}

// IntegrityPath returns the directory containing manifests, caches, markers,
// and quarantine data for the file replica client.
func (c *ReplicaClient) IntegrityPath() string { return c.integrityRoot() }

// QuarantineDir returns the quarantine directory.
func (c *ReplicaClient) QuarantineDir() string {
	return filepath.Join(c.integrityRoot(), "quarantine")
}

// QuarantinePath is an alias for QuarantineDir.
func (c *ReplicaClient) QuarantinePath() string { return c.QuarantineDir() }

func (c *ReplicaClient) manifestStore() *integrity.JSONManifestStore {
	return integrity.NewJSONManifestStore(filepath.Join(c.integrityRoot(), "manifests"))
}

func (c *ReplicaClient) quarantine() *integrity.Quarantine {
	return integrity.NewQuarantine(filepath.Join(c.integrityRoot(), "quarantine"))
}

type badMarker struct {
	FileID      string           `json:"file_id"`
	Path        string           `json:"path"`
	MarkedAt    time.Time        `json:"marked_at"`
	Quarantined []string         `json:"quarantined,omitempty"`
	Error       *integrity.Error `json:"error,omitempty"`
}

func (c *ReplicaClient) badMarkerPath(fileID string) string {
	return filepath.Join(c.integrityRoot(), "bad", url.PathEscape(fileID)+".json")
}

func (c *ReplicaClient) hasBadMarker(fileID string) bool {
	_, err := os.Stat(c.badMarkerPath(fileID))
	return err == nil
}

func (c *ReplicaClient) markBadFile(fileID, path string, ierr *integrity.Error, preserved ...string) {
	markerPath := c.badMarkerPath(fileID)
	_ = os.MkdirAll(filepath.Dir(markerPath), 0o755)
	var existing badMarker
	if b, err := os.ReadFile(markerPath); err == nil {
		_ = json.Unmarshal(b, &existing)
	}
	existing.FileID = fileID
	existing.Path = path
	if existing.MarkedAt.IsZero() {
		existing.MarkedAt = time.Now().UTC()
	}
	existing.Error = ierr
	existing.Quarantined = append(existing.Quarantined, preserved...)
	b, err := json.MarshalIndent(existing, "", "  ")
	if err != nil {
		return
	}
	tmp := markerPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err == nil {
		_ = os.Rename(tmp, markerPath)
	}
}

// deleteLTXFile honors integrity failure markers. A marked live file is left in
// place so retention/cleanup cannot destroy the evidence; a unique quarantine
// copy is also refreshed before returning.
func (c *ReplicaClient) deleteLTXFile(info *ltx.FileInfo) error {
	filename := c.LTXFilePath(info.Level, info.MinTXID, info.MaxTXID)
	fileID := integrityFileID(info.Level, info.MinTXID, info.MaxTXID)
	if c.hasBadMarker(fileID) {
		// Avoid making another full copy on every retention pass. The marker's
		// existing unique quarantine blob is enough if it is still present.
		markerData, _ := os.ReadFile(c.badMarkerPath(fileID))
		var marker badMarker
		_ = json.Unmarshal(markerData, &marker)
		preservedExists := false
		for _, p := range marker.Quarantined {
			if _, err := os.Stat(p); err == nil {
				preservedExists = true
				break
			}
		}
		if !preservedExists {
			if _, statErr := os.Stat(filename); statErr == nil {
				if preserved, qerr := c.quarantine().PreserveFile(filename, &integrity.Error{
					FileID: fileID, ID: fileID, Path: filename,
					Side: integrity.SideRemote, Fault: integrity.SideRemote, Op: integrity.OpRead,
					Status: integrity.StatusFailed, Kind: "marked_bad", ChunkIndex: -1,
					Err: integrity.ErrMismatch,
				}); qerr == nil {
					c.markBadFile(fileID, filename, nil, preserved)
				}
			}
		}
		c.logger.Warn("skipping deletion of integrity-failed LTX file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", filename)
		return nil
	}
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// preserveBadFileBeforeReplace keeps a unique copy of a marked bad canonical
// path before a retry rename replaces it.
func (c *ReplicaClient) preserveBadFileBeforeReplace(fileID, filename string) {
	if !c.hasBadMarker(fileID) {
		return
	}
	if _, err := os.Stat(filename); err != nil {
		return
	}
	preserved, err := c.quarantine().PreserveFile(filename, &integrity.Error{
		FileID: fileID, ID: fileID, Path: filename,
		Side: integrity.SideRemote, Fault: integrity.SideRemote, Op: integrity.OpWrite,
		Status: integrity.StatusFailed, Kind: "replaced_after_failure", ChunkIndex: -1,
		Err: integrity.ErrMismatch,
	})
	if err != nil {
		c.logger.Error("failed to preserve bad LTX file before replacement", "file_id", fileID, "path", filename, "error", err)
		return
	}
	c.markBadFile(fileID, filename, nil, preserved)
}

func (c *ReplicaClient) chunkCache() *integrity.ChunkCache {
	cachePath := filepath.Join(c.integrityRoot(), "chunk-cache.json")
	cache, err := integrity.LoadChunkCache(cachePath)
	if err != nil {
		// A lost cache only costs a streaming hash on a later read. It must not
		// prevent access to the replica or mask a content failure.
		c.logger.Warn("integrity cache unreadable; starting empty", "path", c.integrityRoot(), "error", err)
		return integrity.NewChunkCache(cachePath)
	}
	return cache
}

// saveWriteManifest records the streaming generator's result. Writer-side
// cache records are local-scoped and do not let a reader skip its own check.
func (c *ReplicaClient) saveWriteManifest(ctx context.Context, fileID string, g *integrity.Generator, timestamp time.Time) (*integrity.Manifest, error) {
	m := g.Finish()
	m.VersionInfo.ModTime = timestamp
	if err := c.manifestStore().SaveManifest(ctx, m); err != nil {
		return m, fmt.Errorf("save integrity manifest for %s: %w", fileID, err)
	}
	cache := c.chunkCache()
	cache.MarkManifest(m)
	if err := cache.Save(); err != nil {
		// The manifest is the source of truth; the cache only avoids repeated
		// work, so persistence failure is non-fatal.
		c.logger.Warn("save integrity cache failed", "file_id", fileID, "error", err)
	}
	return m, nil
}

type statusReadCloser struct {
	io.ReadCloser
	status   integrity.Status
	manifest *integrity.Manifest
}

func (r *statusReadCloser) IntegrityStatus() integrity.Status { return r.status }

func (r *statusReadCloser) IntegrityManifest() *integrity.Manifest {
	return r.manifest
}

func (r *statusReadCloser) CacheHits() int {
	type cacheHitter interface{ CacheHits() int }
	if h, ok := r.ReadCloser.(cacheHitter); ok {
		return h.CacheHits()
	}
	return 0
}

// wrapIntegrityReadCloser applies manifest verification for full-object reads.
// Range reads intentionally remain raw because chunk digests cover complete
// chunks and callers use ranges for header/page-index probes.
func (c *ReplicaClient) wrapIntegrityReadCloser(path, fileID string, f io.ReadCloser, offset, size int64) (io.ReadCloser, error) {
	ctx := context.Background()
	m, err := c.manifestStore().LoadManifest(ctx, fileID, integrity.FileVersion{})
	if err != nil {
		if errors.Is(err, integrity.ErrNoManifest) {
			if c.RequireIntegrityManifest {
				_ = f.Close()
				return nil, fmt.Errorf("%w: %s", integrity.ErrUnverified, fileID)
			}
			// Backwards compatibility: pre-integrity replicas remain readable,
			// but are explicitly unverified rather than claimed good.
			var rc io.ReadCloser = f
			if size > 0 {
				rc = internal.LimitReadCloser(f, size)
			}
			return &statusReadCloser{ReadCloser: rc, status: integrity.StatusUnverified}, nil
		}
		_ = f.Close()
		return nil, &integrity.Error{
			FileID:     fileID,
			ID:         fileID,
			Path:       path,
			Side:       integrity.SideRemote,
			Fault:      integrity.SideRemote,
			Op:         integrity.OpRead,
			Status:     integrity.StatusFailed,
			Kind:       "invalid_manifest",
			ChunkIndex: -1,
			Err:        err,
		}
	}

	// Range requests and bounded partial probes cannot be checked against
	// complete chunks without reading adjacent bytes. They stay raw; full
	// restore reads use offset=0,size=0.
	if offset != 0 || (size > 0 && size != m.Size) {
		var rc io.ReadCloser = f
		if size > 0 {
			rc = internal.LimitReadCloser(f, size)
		}
		return &statusReadCloser{ReadCloser: rc, status: integrity.StatusUnverified, manifest: m}, nil
	}

	cache := c.chunkCache()
	q := c.quarantine()
	hook := func(verr *integrity.Error) {
		verr.Path = path
		if verr.FileID == "" {
			verr.FileID = fileID
			verr.ID = fileID
		}
		preserved, qerr := q.PreserveFile(path, verr)
		if qerr != nil {
			c.logger.Error("failed to quarantine checksum-invalid replica file", "file_id", fileID, "path", path, "error", qerr)
			c.markBadFile(fileID, path, verr)
		} else {
			c.markBadFile(fileID, path, verr, preserved)
			c.logger.Error("quarantined checksum-invalid replica file", "file_id", fileID, "path", path, "quarantine", preserved, "failure", verr.Error())
		}
	}

	rc, err := integrity.NewReadCloser(f, m, integrity.SideRemote, integrity.WithCache(cache), integrity.WithPersistCache(), integrity.WithMismatchHook(hook))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &statusReadCloser{ReadCloser: rc, status: integrity.StatusVerified, manifest: m}, nil
}
