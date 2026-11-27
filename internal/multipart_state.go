package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// MultipartPart records one confirmed part of an in-progress multipart upload.
// Number is the provider's one-based part number, which is what a resumed
// upload uses to skip already-confirmed bytes.
type MultipartPart struct {
	Number int   `json:"number"`
	Size   int64 `json:"size"`
	// MD5 is the hex MD5 of the part's plaintext bytes. It lets a retry
	// verify that resumed source bytes still match the stored part without
	// depending on the provider's ETag semantics, which differ under SSE-C.
	MD5 string `json:"md5,omitempty"`
	// ETag is the provider's ETag for the part. It is echoed back on complete.
	ETag string `json:"etag,omitempty"`
}

// MultipartState records the remote state of one in-progress multipart upload
// so a retry can skip parts the provider already confirmed. It is persisted
// locally before any part is sent, which makes the upload id and part numbers
// survive a process restart.
//
// A record never doubles as a completion marker: it is removed only after the
// provider has acknowledged completion of every confirmed part, so a leftover
// record always means "restartable", never "already uploaded".
type MultipartState struct {
	// Key identifies the remote object.
	Key string `json:"key"`
	// UploadID is the provider's multipart upload identifier.
	UploadID string `json:"upload_id"`
	// PartSize is the size every part was cut at. A retry must reuse the
	// same size or previously stored part boundaries no longer line up.
	PartSize int64 `json:"part_size"`
	// TotalBytes is the source size, or zero when the source size is unknown.
	TotalBytes int64 `json:"total_bytes,omitempty"`
	// Parts lists confirmed parts in ascending part-number order.
	Parts []MultipartPart `json:"parts,omitempty"`
	// Attempts counts upload attempts that used this record, so progress can
	// report which retry is running.
	Attempts int `json:"attempts,omitempty"`
	// CreatedAt and UpdatedAt bound the lifetime of the record.
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TotalParts returns the number of parts the source splits into, or zero when
// the total size is unknown.
func (s *MultipartState) TotalParts() int {
	if s == nil || s.TotalBytes <= 0 || s.PartSize <= 0 {
		return 0
	}
	return int((s.TotalBytes + s.PartSize - 1) / s.PartSize)
}

// CompletedBytes returns the number of bytes confirmed by the provider.
func (s *MultipartState) CompletedBytes() int64 {
	if s == nil {
		return 0
	}
	var n int64
	for _, p := range s.Parts {
		n += p.Size
	}
	return n
}

// FindPart returns the confirmed part with the given part number.
func (s *MultipartState) FindPart(number int) (MultipartPart, bool) {
	if s == nil {
		return MultipartPart{}, false
	}
	for _, p := range s.Parts {
		if p.Number == number {
			return p, true
		}
	}
	return MultipartPart{}, false
}

// SetPart records or replaces a confirmed part and keeps the list sorted.
func (s *MultipartState) SetPart(part MultipartPart) {
	if part.Number <= 0 {
		return
	}
	for i := range s.Parts {
		if s.Parts[i].Number == part.Number {
			s.Parts[i] = part
			return
		}
	}
	s.Parts = append(s.Parts, part)
	slices.SortFunc(s.Parts, func(a, b MultipartPart) int { return a.Number - b.Number })
}

// DeletePart removes a part that the provider no longer stores.
func (s *MultipartState) DeletePart(number int) {
	idx := slices.IndexFunc(s.Parts, func(p MultipartPart) bool { return p.Number == number })
	if idx >= 0 {
		s.Parts = slices.Delete(s.Parts, idx, idx+1)
	}
}

// Clone returns a deep copy so callers can persist a consistent snapshot
// without holding the upload lock during local I/O.
func (s *MultipartState) Clone() *MultipartState {
	if s == nil {
		return nil
	}
	clone := *s
	clone.Parts = slices.Clone(s.Parts)
	return &clone
}

// MultipartStateStore persists multipart upload state between attempts so an
// upload can resume after a transient failure or a process restart.
type MultipartStateStore struct {
	dir string
	// now is overridable in tests.
	now func() time.Time

	// mu guards mem. The in-memory cache keeps uploads resumable within one
	// process even when no persistence directory is configured.
	mu  sync.Mutex
	mem map[string]*MultipartState
}

// MultipartStateDirName is the directory, under the database meta path, that
// holds per-upload multipart state files.
const MultipartStateDirName = "uploads"

// MaxMultipartStateAge bounds how long an unused record is kept. A provider
// expires multipart uploads long before this, and stale records only waste
// space.
const MaxMultipartStateAge = 7 * 24 * time.Hour

// NewMultipartStateStore returns a store rooted at dir. An empty dir disables
// persistence; every method then degrades to a no-op.
func NewMultipartStateStore(dir string) *MultipartStateStore {
	return &MultipartStateStore{dir: dir, now: time.Now, mem: make(map[string]*MultipartState)}
}

// Enabled reports whether the store has a directory to write to.
func (s *MultipartStateStore) Enabled() bool { return s != nil && s.dir != "" }

func (s *MultipartStateStore) path(key string) string {
	return filepath.Join(s.dir, fmt.Sprintf("%016x.json", fnv64(key)))
}

// Load reads persisted state for key. A missing record returns (nil, nil) so
// callers can treat it as "start a new upload".
func (s *MultipartStateStore) Load(key string) (*MultipartState, error) {
	if s == nil {
		return nil, nil
	}
	if !s.Enabled() {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.mem[key].Clone(), nil
	}
	path := s.path(key)
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read multipart state: %w", err)
	}
	var state MultipartState
	if err := json.Unmarshal(b, &state); err != nil {
		// A corrupt record cannot be trusted to name parts that exist, so
		// drop it and start over rather than resuming from bad boundaries.
		_ = os.Remove(path)
		return nil, nil
	}
	if state.Key != key || state.UploadID == "" || state.PartSize <= 0 {
		_ = os.Remove(path)
		return nil, nil
	}
	if s.now().Sub(state.UpdatedAt) > MaxMultipartStateAge {
		_ = os.Remove(path)
		return nil, nil
	}
	s.mu.Lock()
	s.mem[key] = state.Clone()
	s.mu.Unlock()
	return &state, nil
}

// Save atomically writes state.
func (s *MultipartStateStore) Save(state *MultipartState) error {
	if s == nil || state == nil {
		return nil
	}
	// Always keep an in-memory copy: the resume optimization should work even
	// when no persistence directory is configured.
	s.mu.Lock()
	if s.mem == nil {
		s.mem = make(map[string]*MultipartState)
	}
	s.mem[state.Key] = state.Clone()
	s.mu.Unlock()

	if !s.Enabled() {
		return nil
	}
	path := s.path(state.Key)
	now := s.now()
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now

	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal multipart state: %w", err)
	}
	b = append(b, '\n')

	if err := MkdirAll(s.dir, nil); err != nil {
		return fmt.Errorf("create multipart state dir: %w", err)
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create multipart state: %w", err)
	}
	_, writeErr := f.Write(b)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write multipart state: %w", writeErr)
	} else if syncErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("sync multipart state: %w", syncErr)
	} else if closeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close multipart state: %w", closeErr)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("publish multipart state: %w", err)
	}
	return nil
}

// Remove deletes the persisted state for key. It is called only after the
// multipart upload has been completed successfully.
func (s *MultipartStateStore) Remove(key string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	delete(s.mem, key)
	s.mu.Unlock()
	if !s.Enabled() {
		return nil
	}
	if err := os.Remove(s.path(key)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove multipart state: %w", err)
	}
	return nil
}

// fnv64 is the FNV-1a hash used to derive stable, filesystem-safe state file
// names from object keys.
func fnv64(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var h uint64 = offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
