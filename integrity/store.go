package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

// ManifestStore persists and loads version-bound manifests.
type ManifestStore interface {
	SaveManifest(ctx context.Context, m *Manifest) error
	LoadManifest(ctx context.Context, fileID string, want any) (*Manifest, error)
}

// MemoryManifestStore is useful in tests.
type MemoryManifestStore struct {
	manifests map[string]*Manifest
}

// NewMemoryManifestStore creates an empty in-memory manifest store.
func NewMemoryManifestStore() *MemoryManifestStore {
	return &MemoryManifestStore{manifests: make(map[string]*Manifest)}
}

// SaveManifest stores a copy of m.
func (s *MemoryManifestStore) SaveManifest(_ context.Context, m *Manifest) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if err := m.Validate(); err != nil {
		return err
	}
	s.manifests[m.FileID] = m
	return nil
}

// LoadManifest loads the latest manifest for fileID and optionally checks its
// version against want.
func (s *MemoryManifestStore) LoadManifest(_ context.Context, fileID string, want any) (*Manifest, error) {
	m, ok := s.manifests[fileID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoManifest, fileID)
	}
	if !manifestMatchesVersion(m, want) {
		wantVersion, _ := NormalizeVersion(want)
		return nil, fmt.Errorf("%w: file=%s want=%s got=%s", ErrVersionMismatch, fileID, wantVersion, m.CacheVersion())
	}
	return m, nil
}

func manifestMatchesVersion(m *Manifest, want any) bool {
	if want == nil {
		return true
	}
	wantVersion, info := NormalizeVersion(want)
	if !info.IsZero() && !m.VersionInfo.Matches(info) {
		return false
	}
	gotVersion, _ := NormalizeVersion(m.Version)
	if wantVersion != "" && gotVersion != "" && wantVersion != gotVersion {
		return false
	}
	return true
}

// JSONManifestStore stores one JSON manifest per file ID in a directory.
type JSONManifestStore struct {
	dir string
}

// NewJSONManifestStore creates a JSON-backed manifest store.
func NewJSONManifestStore(dir string) *JSONManifestStore {
	return &JSONManifestStore{dir: dir}
}

// Dir returns the manifest directory.
func (s *JSONManifestStore) Dir() string { return s.dir }

func (s *JSONManifestStore) manifestPath(fileID string) string {
	name := url.PathEscape(fileID) + ".json"
	return filepath.Join(s.dir, name)
}

// SaveManifest atomically writes the manifest. Replacing the file is intended:
// the new manifest has a new bound version, so callers cannot use it to verify
// old content under the old version.
func (s *JSONManifestStore) SaveManifest(_ context.Context, m *Manifest) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if err := m.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	path := s.manifestPath(m.FileID)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
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
	return os.Rename(tmp, path)
}

// LoadManifest reads and validates a manifest.
func (s *JSONManifestStore) LoadManifest(_ context.Context, fileID string, want any) (*Manifest, error) {
	b, err := os.ReadFile(s.manifestPath(fileID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNoManifest, fileID)
	} else if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%w: parse manifest for %s: %v", ErrInvalidManifest, fileID, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if !manifestMatchesVersion(&m, want) {
		wantVersion, _ := NormalizeVersion(want)
		return nil, fmt.Errorf("%w: file=%s want=%s got=%s", ErrVersionMismatch, fileID, wantVersion, m.CacheVersion())
	}
	return &m, nil
}
