package integrity

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Service bundles manifest persistence, chunk caching, and quarantine for one
// replica/database. The zero value is not usable; construct with NewService.
type Service struct {
	Manifests  ManifestStore
	Cache      *ChunkCache
	Quarantine *Quarantine

	// ChunkSize is used for newly generated manifests.
	ChunkSize int

	// SaveCache controls whether cache updates are persisted after commits and
	// verifications when Cache has a path.
	SaveCache bool
}

// ServiceConfig configures a Service.
type ServiceConfig struct {
	// MetadataDir is used for the default JSON manifest store, cache file, and
	// quarantine directory.
	MetadataDir string

	// ManifestStore overrides the on-disk JSON store.
	ManifestStore ManifestStore

	// CachePath overrides MetadataDir/integrity_cache.json.
	CachePath string

	// QuarantineDir overrides MetadataDir/quarantine.
	QuarantineDir string

	// Cache is an optional preloaded cache.
	Cache *ChunkCache

	ChunkSize int
}

// NewService constructs a Service and its on-disk components.
func NewService(cfg ServiceConfig) (*Service, error) {
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	manifests := cfg.ManifestStore
	cache := cfg.Cache
	quarantineDir := cfg.QuarantineDir
	cachePath := cfg.CachePath
	if cfg.MetadataDir != "" {
		if manifests == nil {
			manifests = NewJSONManifestStore(filepath.Join(cfg.MetadataDir, "manifests"))
		}
		if cachePath == "" {
			cachePath = filepath.Join(cfg.MetadataDir, "chunk-cache.json")
		}
		if quarantineDir == "" {
			quarantineDir = DefaultQuarantineDir(cfg.MetadataDir)
		}
	}
	if cache == nil && cachePath != "" {
		loaded, err := LoadChunkCache(cachePath)
		if err != nil {
			return nil, err
		}
		cache = loaded
	} else if cache == nil {
		cache = NewChunkCache("")
	}
	var q *Quarantine
	if quarantineDir != "" {
		q = NewQuarantine(quarantineDir)
	}
	return &Service{
		Manifests:  manifests,
		Cache:      cache,
		Quarantine: q,
		ChunkSize:  chunkSize,
		SaveCache:  cachePath != "",
	}, nil
}

// Generator returns a streaming manifest generator.
func (s *Service) Generator(fileID string, version any) *Generator {
	size := 0
	if s != nil {
		size = s.ChunkSize
	}
	return NewGenerator(fileID, version, size)
}

// CommitWrite finishes a write-side manifest, marks the streamed chunks
// verified in the write cache, and persists the manifest/cache.
func (s *Service) CommitWrite(ctx context.Context, g *Generator) (*Manifest, error) {
	if g == nil {
		return nil, fmt.Errorf("integrity: nil generator")
	}
	m := g.Finish()
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if s != nil {
		if s.Manifests != nil {
			if err := s.Manifests.SaveManifest(ctx, m); err != nil {
				return m, err
			}
		}
		if s.Cache != nil {
			s.Cache.MarkManifest(m)
			if s.SaveCache {
				if err := s.Cache.Save(); err != nil {
					return m, err
				}
			}
		}
	}
	return m, nil
}

// CopyAndCommitWrite copies src to dst while streaming chunk digests. It does
// not re-read dst after the write. Source read failures are classified local;
// destination write failures are returned for the caller to classify because
// dst may be a remote client.
func (s *Service) CopyAndCommitWrite(ctx context.Context, fileID string, version any, dst io.Writer, src io.Reader) (*Manifest, int64, error) {
	g := s.Generator(fileID, version)
	versionKey, _ := NormalizeVersion(version)
	n, err := io.Copy(io.MultiWriter(dst, g), src)
	if err != nil {
		return nil, n, &Error{
			FileID:     fileID,
			ID:         fileID,
			Version:    versionKey,
			Side:       SideLocal,
			Fault:      SideLocal,
			Op:         OpWrite,
			Status:     StatusFailed,
			Kind:       "read_or_write",
			ChunkIndex: -1,
			Offset:     n,
			Err:        err,
		}
	}
	m, err := s.CommitWrite(ctx, g)
	return m, n, err
}

// LoadManifest loads verification metadata. Missing metadata returns an error
// wrapping ErrUnverified and ErrNoManifest; callers must not treat that as a
// successful verification.
func (s *Service) LoadManifest(ctx context.Context, fileID string, want any) (*Manifest, error) {
	if s == nil || s.Manifests == nil {
		return nil, fmt.Errorf("%w: no manifest store for %s", ErrUnverified, fileID)
	}
	m, err := s.Manifests.LoadManifest(ctx, fileID, want)
	if err != nil {
		if IsUnverified(err) {
			return nil, fmt.Errorf("%w: %v", ErrUnverified, err)
		}
		return nil, err
	}
	return m, nil
}

// VerifyFile performs an explicit full verification pass. If all chunks are
// already present in the version-bound cache, it returns the manifest with
// cached=true without opening or reading path.
func (s *Service) VerifyFile(ctx context.Context, path, fileID string, want any, side Side) (m *Manifest, cached bool, err error) {
	m, err = s.LoadManifest(ctx, fileID, want)
	if err != nil {
		return nil, false, err
	}
	if side == "" {
		side = SideRemote
	}
	if s != nil && s.Cache != nil && s.Cache.AllVerifiedOnSide(m, side) {
		return m, true, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return m, false, err
	}
	defer f.Close()

	var hook func(*Error)
	if s != nil && s.Quarantine != nil {
		hook = func(verr *Error) {
			verr.Path = path
			_, _ = s.Quarantine.PreserveFile(path, verr)
		}
	}
	opts := []VerifierOption{WithMismatchHook(hook)}
	if s != nil && s.Cache != nil {
		opts = append(opts, WithCache(s.Cache))
	}
	if err := VerifyReader(f, m, side, opts...); err != nil {
		return m, false, err
	}
	if s != nil && s.Cache != nil && s.SaveCache {
		_ = s.Cache.Save()
	}
	return m, false, nil
}

// PreserveFile is a convenience wrapper around the configured quarantine.
func (s *Service) PreserveFile(path string, verr *Error) (string, error) {
	if s == nil || s.Quarantine == nil {
		return "", fmt.Errorf("integrity: quarantine is not configured")
	}
	return s.Quarantine.PreserveFile(path, verr)
}
