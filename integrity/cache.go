package integrity

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// DigestCache is an alternate name for ChunkCache.
type DigestCache = ChunkCache

// NewDigestCache is an alias for NewChunkCache.
func NewDigestCache(path string) *DigestCache { return NewChunkCache(path) }

// LoadDigestCache is an alias for LoadChunkCache.
func LoadDigestCache(path string) (*DigestCache, error) { return LoadChunkCache(path) }

// ChunkRecord is a previously verified chunk digest bound to a file version.
type ChunkRecord struct {
	FileID     string `json:"file_id"`
	Version    string `json:"version"`
	ChunkIndex int    `json:"chunk_index"`
	Offset     int64  `json:"offset"`
	Size       int    `json:"size"`
	Digest     string `json:"digest"`
	Status     Status `json:"status"`
	// Side records which endpoint actually observed these bytes. Writer-side
	// cache entries must not cause a reader to skip its first remote read.
	Side       Side      `json:"side,omitempty"`
	VerifiedAt time.Time `json:"verified_at"`
}

// CacheKey returns the exact lookup key for a chunk record. The version is part
// of the key, so records for an updated file cannot hit.
func CacheKey(fileID, version string, chunkIndex int, digest string) string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%s", fileID, version, chunkIndex, normalizeDigest(digest))
}

// ChunkCache is a concurrency-safe map of verified chunk digests. It may be
// persisted as JSON.
type ChunkCache struct {
	mu      sync.RWMutex
	path    string
	records map[string]ChunkRecord
}

// NewChunkCache returns an empty in-memory cache. If path is non-empty, Load
// can populate it and Save can atomically persist it.
func NewChunkCache(path string) *ChunkCache {
	return &ChunkCache{path: path, records: make(map[string]ChunkRecord)}
}

// LoadChunkCache reads a persisted cache. A missing file yields an empty cache
// so existing replicas with no verification state start unverified.
func LoadChunkCache(path string) (*ChunkCache, error) {
	c := NewChunkCache(path)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	} else if err != nil {
		return nil, err
	}
	var doc struct {
		Records []ChunkRecord `json:"records"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("parse integrity cache %s: %w", path, err)
	}
	for _, rec := range doc.Records {
		if rec.Status == "" {
			rec.Status = StatusVerified
		}
		c.records[CacheKey(rec.FileID, rec.Version, rec.ChunkIndex, rec.Digest)] = rec
	}
	return c, nil
}

// Load populates c from disk.
func (c *ChunkCache) Load() error {
	if c.path == "" {
		return nil
	}
	loaded, err := LoadChunkCache(c.path)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = loaded.records
	return nil
}

// Has returns whether an exact version-bound chunk digest was verified by
// either side.
func (c *ChunkCache) Has(fileID, version string, chunkIndex int, digest string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec, ok := c.records[CacheKey(fileID, version, chunkIndex, digest)]
	return ok && rec.Status != StatusFailed
}

// HasOnSide returns a cache hit only when the named side performed the
// verification. This prevents a writer's streaming cache from causing the
// reader to trust a remote replica without its own first verification.
func (c *ChunkCache) HasOnSide(fileID, version string, chunkIndex int, digest string, side Side) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec, ok := c.records[CacheKey(fileID, version, chunkIndex, digest)]
	return ok && rec.Status != StatusFailed && (side == "" || rec.Side == side)
}

// IsChunkVerified is a descriptive alias for Has.
func (c *ChunkCache) IsChunkVerified(fileID, version string, chunkIndex int, digest string) bool {
	return c.Has(fileID, version, chunkIndex, digest)
}

// IsHit reports whether a version-bound chunk digest is cached.
func (c *ChunkCache) IsHit(fileID, version string, chunk ChunkDigest) bool {
	return c.Has(fileID, version, chunk.Index, chunk.Digest)
}

// Hit is an alias for IsHit.
func (c *ChunkCache) Hit(fileID, version string, chunk ChunkDigest) bool {
	return c.IsHit(fileID, version, chunk)
}

// Get returns a record and whether it exists.
func (c *ChunkCache) Get(fileID, version string, chunkIndex int, digest string) (ChunkRecord, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec, ok := c.records[CacheKey(fileID, version, chunkIndex, digest)]
	return rec, ok
}

// Remember adds or replaces one verified chunk record.
func (c *ChunkCache) Remember(rec ChunkRecord) {
	if c == nil {
		return
	}
	if rec.Digest != "" {
		rec.Digest = normalizeDigest(rec.Digest)
	}
	if rec.Status == "" {
		rec.Status = StatusVerified
	}
	if rec.VerifiedAt.IsZero() {
		rec.VerifiedAt = time.Now().UTC()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.records == nil {
		c.records = make(map[string]ChunkRecord)
	}
	c.records[CacheKey(rec.FileID, rec.Version, rec.ChunkIndex, rec.Digest)] = rec
}

// Add is an alias for Remember.
func (c *ChunkCache) Add(rec ChunkRecord) { c.Remember(rec) }

// Put is an alias for Remember.
func (c *ChunkCache) Put(rec ChunkRecord) { c.Remember(rec) }

// RememberChunk records one chunk digest for a file version.
func (c *ChunkCache) RememberChunk(fileID, version string, chunk ChunkDigest, side Side) {
	c.Remember(ChunkRecord{
		FileID:     fileID,
		Version:    version,
		ChunkIndex: chunk.Index,
		Offset:     chunk.Offset,
		Size:       chunk.Size,
		Digest:     chunk.Digest,
		Status:     StatusVerified,
		Side:       side,
	})
}

// MarkManifest records every chunk in m as verified. It is used by the write
// path because the generator has already observed every byte during the
// required write stream; no re-read is performed.
func (c *ChunkCache) MarkManifest(m *Manifest) {
	if c == nil || m == nil {
		return
	}
	version := m.CacheVersion()
	now := time.Now().UTC()
	for _, ch := range m.Chunks {
		c.Remember(ChunkRecord{
			FileID:     m.FileID,
			Version:    version,
			ChunkIndex: ch.Index,
			Offset:     ch.Offset,
			Size:       ch.Size,
			Digest:     ch.Digest,
			Status:     StatusVerified,
			Side:       SideLocal,
			VerifiedAt: now,
		})
	}
}

// AllVerified reports whether every chunk in m has an exact cache hit from
// any side.
func (c *ChunkCache) AllVerified(m *Manifest) bool {
	return c.allVerified(m, "")
}

// AllVerifiedOnSide reports whether every chunk was verified by side.
func (c *ChunkCache) AllVerifiedOnSide(m *Manifest, side Side) bool {
	return c.allVerified(m, side)
}

func (c *ChunkCache) allVerified(m *Manifest, side Side) bool {
	if c == nil || m == nil || len(m.Chunks) == 0 {
		return false
	}
	version := m.CacheVersion()
	for _, ch := range m.Chunks {
		if side == "" {
			if !c.Has(m.FileID, version, ch.Index, ch.Digest) {
				return false
			}
		} else if !c.HasOnSide(m.FileID, version, ch.Index, ch.Digest, side) {
			return false
		}
	}
	return true
}

// MissingChunks returns manifest chunks without an exact cache hit.
func (c *ChunkCache) MissingChunks(m *Manifest) []ChunkDigest {
	if m == nil {
		return nil
	}
	version := m.CacheVersion()
	var out []ChunkDigest
	for _, ch := range m.Chunks {
		if !c.Has(m.FileID, version, ch.Index, ch.Digest) {
			out = append(out, ch)
		}
	}
	return out
}

// Invalidate removes records matching fileID.
func (c *ChunkCache) Invalidate(fileID string) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, rec := range c.records {
		if rec.FileID == fileID {
			delete(c.records, k)
			n++
		}
	}
	return n
}

// InvalidateFile is an alias for Invalidate.
func (c *ChunkCache) InvalidateFile(fileID string) int { return c.Invalidate(fileID) }

// InvalidateVersion removes one exact file version. Other versions remain.
func (c *ChunkCache) InvalidateVersion(fileID, version string) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, rec := range c.records {
		if rec.FileID == fileID && rec.Version == version {
			delete(c.records, k)
			n++
		}
	}
	return n
}

// Len returns the number of cached records.
func (c *ChunkCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.records)
}

// Save atomically persists the cache as JSON.
func (c *ChunkCache) Save() error {
	if c == nil || c.path == "" {
		return nil
	}
	c.mu.RLock()
	records := make([]ChunkRecord, 0, len(c.records))
	for _, rec := range c.records {
		records = append(records, rec)
	}
	c.mu.RUnlock()
	sort.Slice(records, func(i, j int) bool {
		if records[i].FileID != records[j].FileID {
			return records[i].FileID < records[j].FileID
		}
		if records[i].Version != records[j].Version {
			return records[i].Version < records[j].Version
		}
		return records[i].ChunkIndex < records[j].ChunkIndex
	})
	b, err := json.MarshalIndent(struct {
		Records []ChunkRecord `json:"records"`
	}{Records: records}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
