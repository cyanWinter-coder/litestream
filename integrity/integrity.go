// Package integrity provides streaming content checksums, version-bound chunk
// manifests, verification caching, and corruption quarantine support.
//
// The package is intentionally independent from Litestream's replica clients so
// it can be used by both the writer that publishes a replica file and the
// reader that restores it.
package integrity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"time"
)

const (
	// AlgorithmSHA256 is the only digest algorithm currently produced.
	AlgorithmSHA256 = "sha256"

	// SchemaVersion is the current manifest schema version.
	SchemaVersion = 1

	// DefaultChunkSize balances manifest size against failure locality.
	DefaultChunkSize = 256 * 1024
)

// Common sentinel errors. Detailed failures are usually wrapped in *Error.
var (
	ErrMismatch         = errors.New("integrity checksum mismatch")
	ErrChecksumMismatch = errors.New("invalid replica, checksum mismatch")
	ErrTruncated        = errors.New("integrity: truncated content")
	ErrExtraData        = errors.New("integrity: extra content after manifest size")
	ErrNoManifest       = errors.New("integrity: no checksum manifest")
	ErrUnverified       = errors.New("integrity: file is unverified")
	ErrVersionMismatch  = errors.New("integrity: file version does not match checksum manifest")
	ErrInvalidManifest  = errors.New("integrity: invalid checksum manifest")
)

// Side identifies which side of replication is believed to be faulty.
type Side string

const (
	// SideLocal is the producer/source side. A failure here means the local
	// source bytes or local write path are suspect.
	SideLocal Side = "local"

	// SideRemote is the replica/consumer side. A failure here means bytes
	// returned from a replica do not match the manifest produced by the
	// writer. For a file:// replica, "remote" means the replica endpoint,
	// even when its storage is mounted on the same machine.
	SideRemote Side = "remote"
)

// FaultDomain is an alias for Side used by APIs that prefer domain wording.
type FaultDomain = Side

const (
	FaultLocal  = SideLocal
	FaultRemote = SideRemote
)

// Operation identifies the operation that observed a failure.
type Operation string

const (
	OpWrite Operation = "write"
	OpRead  Operation = "read"
)

// Status is the verification state of a file or chunk.
type Status string

const (
	StatusUnknown    Status = "unknown"
	StatusUnverified Status = "unverified"
	StatusVerified   Status = "verified"
	StatusCached     Status = "cached"
	StatusFailed     Status = "failed"
)

// FileID is the stable identifier shared by writer and reader.
type FileID = string

// Version is an open version value. Callers may use a plain string or
// VersionInfo/FileVersion. The empty interface alias keeps alternate call
// styles source-compatible.
type Version = any

// VersionID is the simple string form of a file version.
type VersionID string

// VersionInfo optionally binds digests to a stronger storage version.
//
// Token or ETag should be used when the storage system supplies a strong
// version. Size and ModTime are useful fallbacks for local files. When a
// manifest is generated, Digest is also used as a strong content version if no
// external token is available.
type VersionInfo struct {
	// Version is an optional explicit version string.
	Version string `json:"version,omitempty"`

	// Token is a storage-specific version identifier.
	Token string `json:"token,omitempty"`

	// ETag is an alias for Token for backends that use ETag terminology.
	ETag string `json:"etag,omitempty"`

	// Size is the expected file size, when known.
	Size int64 `json:"size,omitempty"`

	// ModTime is the last modification time, when meaningful.
	ModTime time.Time `json:"mod_time,omitempty"`

	// Digest is the whole-file digest. It is populated on generated manifests
	// and gives cache keys a strong version even without a storage token.
	Digest string `json:"digest,omitempty"`
}

// FileVersion is an alias for the rich version metadata.
type FileVersion = VersionInfo

// NormalizeVersion accepts either a string/Version or VersionInfo/FileVersion.
func NormalizeVersion(v any) (string, VersionInfo) {
	switch x := v.(type) {
	case nil:
		return "", VersionInfo{}
	case VersionID:
		return string(x), VersionInfo{Version: string(x)}
	case string:
		return x, VersionInfo{Version: x}
	case VersionInfo:
		id := x.Version
		if id == "" {
			id = x.Token
		}
		if id == "" {
			id = x.ETag
		}
		return id, x
	case *VersionInfo:
		if x == nil {
			return "", VersionInfo{}
		}
		return NormalizeVersion(*x)
	default:
		s := fmt.Sprint(v)
		return s, VersionInfo{Version: s}
	}
}

// String returns a stable, concise version key.
func (v VersionInfo) String() string {
	token := v.Version
	if token == "" {
		token = v.Token
	}
	if token == "" {
		token = v.ETag
	}
	if token == "" {
		token = v.Digest
	}
	parts := make([]string, 0, 4)
	if token != "" {
		parts = append(parts, token)
	}
	if v.Size != 0 {
		parts = append(parts, fmt.Sprintf("size=%d", v.Size))
	}
	if !v.ModTime.IsZero() {
		parts = append(parts, "mtime="+v.ModTime.UTC().Format(time.RFC3339Nano))
	}
	return strings.Join(parts, ";")
}

// Key returns the version component used in cache keys. It never returns empty
// for a generated manifest.
func (v VersionInfo) Key(fallbackDigest string) string {
	if s := v.String(); s != "" {
		return s
	}
	if fallbackDigest != "" {
		return "digest=" + fallbackDigest
	}
	return "unversioned"
}

// IsZero reports whether no version field is set.
func (v VersionInfo) IsZero() bool {
	return v.Version == "" && v.Token == "" && v.ETag == "" && v.Digest == "" && v.Size == 0 && v.ModTime.IsZero()
}

// Matches reports whether the non-zero fields in want agree with v.
func (v VersionInfo) Matches(want VersionInfo) bool {
	if want.Version != "" && v.Version != "" && want.Version != v.Version {
		return false
	}
	if want.Token != "" && v.Token != "" && want.Token != v.Token {
		return false
	}
	if want.ETag != "" && v.ETag != "" && want.ETag != v.ETag {
		return false
	}
	if want.Digest != "" && v.Digest != "" && !strings.EqualFold(want.Digest, v.Digest) {
		return false
	}
	if want.Size > 0 && v.Size != want.Size {
		return false
	}
	if !want.ModTime.IsZero() && !v.ModTime.IsZero() && !want.ModTime.Equal(v.ModTime) {
		return false
	}
	return true
}

// ChunkDigest is the digest of one fixed-size byte range.
type ChunkDigest struct {
	// Index is the zero-based chunk number.
	Index int `json:"index"`

	// Offset is the chunk's byte offset in the complete file.
	Offset int64 `json:"offset"`

	// Size is the chunk length. It is usually ChunkSize, except for the tail.
	Size int `json:"size"`

	// Length is an alias for Size accepted from alternate callers.
	Length int64 `json:"length,omitempty"`

	// Digest is the lowercase hexadecimal SHA-256 digest.
	Digest string `json:"digest"`

	// Checksum, Hash, and SHA256 are compatibility aliases for callers that
	// construct digests using different naming. Validate normalizes them.
	Checksum string `json:"checksum,omitempty"`
	Hash     string `json:"hash,omitempty"`
	SHA256   string `json:"sha256,omitempty"`

	// Algorithm allows future digest migration. Empty means sha256.
	Algorithm string `json:"algorithm,omitempty"`
}

// BlockDigest is an alias for callers that use block terminology.
type BlockDigest = ChunkDigest

// Manifest is the version-bound collection of chunk digests for one file.
type Manifest struct {
	SchemaVersion int           `json:"schema_version"`
	Algorithm     string        `json:"algorithm"`
	FileID        string        `json:"file_id"`
	ID            string        `json:"id,omitempty"`
	Version       Version       `json:"version"`
	VersionInfo   VersionInfo   `json:"version_info,omitempty"`
	ChunkSize     int           `json:"chunk_size"`
	BlockSize     int           `json:"block_size,omitempty"`
	Size          int64         `json:"size"`
	FileSize      int64         `json:"file_size,omitempty"`
	Digest        string        `json:"digest"`
	Checksum      string        `json:"checksum,omitempty"`
	SHA256        string        `json:"sha256,omitempty"`
	Chunks        []ChunkDigest `json:"chunks"`

	// Compatibility/alternate representations. Validate normalizes these into
	// the canonical fields above.
	Digests        []ChunkDigest `json:"digests,omitempty"`
	ChunkChecksums []string      `json:"chunk_checksums,omitempty"`
	Checksums      []string      `json:"checksums,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// BlockDigests is an alias for Chunks.
func (m *Manifest) BlockDigests() []ChunkDigest { return m.Chunks }

// CacheVersion returns the exact version scope for verification records.
func (m *Manifest) CacheVersion() string {
	if m == nil {
		return ""
	}
	if s, _ := NormalizeVersion(m.Version); s != "" {
		return s
	}
	return m.VersionInfo.Key(m.Digest)
}

// Validate checks that the manifest is internally consistent. Alternate field
// names commonly used by callers are normalized before validation.
func (m *Manifest) Validate() error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	m.normalize()
	if m.FileID == "" {
		return fmt.Errorf("%w: missing file id", ErrInvalidManifest)
	}
	if m.ChunkSize <= 0 {
		return fmt.Errorf("%w: invalid chunk size %d", ErrInvalidManifest, m.ChunkSize)
	}
	algo := m.Algorithm
	if algo == "" {
		algo = AlgorithmSHA256
	}
	if algo != AlgorithmSHA256 {
		return fmt.Errorf("%w: unsupported algorithm %q", ErrInvalidManifest, algo)
	}
	var offset int64
	for i := range m.Chunks {
		c := &m.Chunks[i]
		c.Index = i
		c.Offset = offset
		if c.Size <= 0 && c.Length > 0 {
			c.Size = int(c.Length)
		}
		if c.Size <= 0 {
			remaining := m.Size - offset
			switch {
			case remaining >= int64(m.ChunkSize):
				c.Size = m.ChunkSize
			case remaining > 0:
				c.Size = int(remaining)
			default:
				c.Size = m.ChunkSize
			}
		}
		if c.Digest == "" {
			c.Digest = firstNonEmptyDigest(c.Checksum, c.Hash, c.SHA256)
		}
		if c.Algorithm == "" {
			c.Algorithm = algo
		}
		if c.Size <= 0 {
			return fmt.Errorf("%w: chunk %d has non-positive size", ErrInvalidManifest, i)
		}
		if len(c.Digest) != sha256.Size*2 {
			return fmt.Errorf("%w: chunk %d has invalid digest length", ErrInvalidManifest, i)
		}
		if _, err := hex.DecodeString(c.Digest); err != nil {
			return fmt.Errorf("%w: chunk %d has invalid digest: %v", ErrInvalidManifest, i, err)
		}
		offset += int64(c.Size)
	}
	if m.Size == 0 && offset > 0 {
		m.Size = offset
	}
	if offset != m.Size {
		return fmt.Errorf("%w: chunks cover %d bytes, manifest size is %d", ErrInvalidManifest, offset, m.Size)
	}
	return nil
}

func (m *Manifest) normalize() {
	if m.FileID == "" && m.ID != "" {
		m.FileID = m.ID
	}
	if m.ChunkSize <= 0 && m.BlockSize > 0 {
		m.ChunkSize = m.BlockSize
	}
	if m.Size == 0 && m.FileSize > 0 {
		m.Size = m.FileSize
	}
	if m.Digest == "" {
		m.Digest = firstNonEmptyDigest(m.Checksum, m.SHA256)
	}
	if len(m.Chunks) == 0 && len(m.Digests) > 0 {
		m.Chunks = append([]ChunkDigest(nil), m.Digests...)
	}
	stringDigests := m.ChunkChecksums
	if len(m.Chunks) == 0 && len(stringDigests) == 0 {
		stringDigests = m.Checksums
	}
	if len(m.Chunks) == 0 && len(stringDigests) > 0 {
		m.Chunks = make([]ChunkDigest, len(stringDigests))
		var offset int64
		for i, d := range stringDigests {
			size := m.ChunkSize
			if m.Size > 0 {
				remaining := m.Size - offset
				if remaining < int64(size) && remaining > 0 {
					size = int(remaining)
				}
			}
			m.Chunks[i] = ChunkDigest{Index: i, Offset: offset, Size: size, Digest: d}
			offset += int64(size)
		}
		if m.Size == 0 {
			m.Size = offset
		}
	}
}

func firstNonEmptyDigest(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// Conclusion is the identity of a verification outcome. Writer and reader
// reports for the same corrupt chunk share FileID, Index, Offset, and Expected.
type Conclusion struct {
	FileID     string `json:"file_id"`
	Version    string `json:"version,omitempty"`
	ChunkIndex int    `json:"chunk_index"`
	Offset     int64  `json:"offset"`
	End        int64  `json:"end"`
	Expected   string `json:"expected,omitempty"`
	Actual     string `json:"actual,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// Match reports whether two ends are reporting the same underlying failure.
// Side-specific fields and observed actual digests are intentionally ignored.
func (c Conclusion) Match(o Conclusion) bool {
	if c.FileID != o.FileID || c.ChunkIndex != o.ChunkIndex || c.Offset != o.Offset {
		return false
	}
	if c.Expected != "" && o.Expected != "" && !strings.EqualFold(c.Expected, o.Expected) {
		return false
	}
	if c.Version != "" && o.Version != "" && c.Version != o.Version {
		return false
	}
	return true
}

// Error is returned when content cannot be verified. It carries the stable file
// identifier, chunk location, and whether the local or remote side is implicated.
type Error struct {
	// FileID is the stable file identifier shared by writer and reader.
	FileID string `json:"file_id"`

	// ID and File are aliases for FileID.
	ID   string `json:"id,omitempty"`
	File string `json:"file,omitempty"`

	// Path is the local path involved, when available. It is not the identity
	// used to correlate writer and reader.
	Path string `json:"path,omitempty"`

	// Version is the bound file version.
	Version string `json:"version,omitempty"`

	// Side is the fault domain: local or remote.
	Side Side `json:"side"`

	// Domain and Fault are aliases for Side.
	Domain Side `json:"domain,omitempty"`
	Fault  Side `json:"fault,omitempty"`

	// Op is write or read.
	Op Operation `json:"op,omitempty"`

	// Status is usually failed.
	Status Status `json:"status,omitempty"`

	// Kind is a short machine-readable reason: checksum_mismatch, truncated,
	// extra_data, version_mismatch, or unverified. Reason and Type are aliases.
	Kind   string `json:"kind,omitempty"`
	Reason string `json:"reason,omitempty"`
	Type   string `json:"type,omitempty"`

	// ChunkIndex is the zero-based failed chunk, or -1 when not chunk-specific.
	// Index and Chunk are compatibility aliases.
	ChunkIndex int   `json:"chunk_index"`
	Index      int   `json:"index,omitempty"`
	Chunk      int   `json:"chunk,omitempty"`
	Offset     int64 `json:"offset"`

	// Position and ByteOffset are compatibility aliases for Offset.
	Position   int64 `json:"position,omitempty"`
	ByteOffset int64 `json:"byte_offset,omitempty"`

	// End is the exclusive end of the failed chunk.
	End int64 `json:"end,omitempty"`

	// Size is the manifest/expected file size.
	Size int64 `json:"size,omitempty"`

	// BytesRead is the number of bytes observed before failure.
	BytesRead int64 `json:"bytes_read,omitempty"`

	// Expected is the expected digest. ExpectedChecksum is an alias.
	Expected         string `json:"expected,omitempty"`
	ExpectedChecksum string `json:"expected_checksum,omitempty"`

	// Actual is the digest computed from observed content. ActualChecksum is
	// an alias.
	Actual         string `json:"actual,omitempty"`
	ActualChecksum string `json:"actual_checksum,omitempty"`

	// CacheHits can be populated by caching verifiers.
	CacheHits int `json:"cache_hits,omitempty"`

	// Err is the wrapped sentinel or underlying error.
	Err error `json:"-"`
}

// Compatibility aliases for callers/tests that prefer other type names.
type (
	MismatchError         = Error
	ValidationError       = Error
	ContentError          = Error
	ChecksumError         = Error
	ChecksumMismatchError = Error
	CorruptionError       = Error
)

func (e Error) Error() string {
	var b strings.Builder
	side := e.Side
	if side == "" {
		side = e.Fault
	}
	if side == "" {
		side = e.Domain
	}
	if side == "" {
		side = "unknown"
	}
	fmt.Fprintf(&b, "integrity verification failed: side=%s", side)
	if e.Op != "" {
		fmt.Fprintf(&b, " op=%s", e.Op)
	}
	id := e.effectiveFileID()
	if id != "" {
		fmt.Fprintf(&b, " file_id=%q", id)
	}
	if e.Path != "" {
		fmt.Fprintf(&b, " path=%q", e.Path)
	}
	if e.Version != "" {
		fmt.Fprintf(&b, " version=%q", e.Version)
	}
	chunkIndex := e.effectiveChunkIndex()
	offset := e.effectiveOffset()
	if chunkIndex >= 0 {
		fmt.Fprintf(&b, " chunk=%d", chunkIndex)
	}
	if offset >= 0 {
		fmt.Fprintf(&b, " offset=%d", offset)
		if e.End > offset {
			fmt.Fprintf(&b, "-%d", e.End)
		}
	}
	if e.Kind != "" {
		fmt.Fprintf(&b, " kind=%s", e.Kind)
	}
	if e.Expected != "" || e.Actual != "" {
		fmt.Fprintf(&b, " expected=%s actual=%s", shortDigest(e.Expected), shortDigest(e.Actual))
	}
	if e.Err != nil {
		fmt.Fprintf(&b, ": %v", e.Err)
	}
	return b.String()
}

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error { return e.Err }

// As supports errors.As into either *Error or Error value targets.
func (e *Error) As(target any) bool {
	switch t := target.(type) {
	case **Error:
		*t = e
		return true
	case *Error:
		if t == nil {
			return false
		}
		*t = *e
		return true
	}
	return false
}

// Is lets errors.Is match the package's canonical mismatch sentinel even when
// the wrapped cause is a more specific truncation/version error.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrChecksumMismatch:
		return errors.Is(e.Err, ErrMismatch) || e.Kind == "checksum_mismatch"
	case ErrMismatch:
		return e.Kind == "checksum_mismatch" || errors.Is(e.Err, ErrMismatch)
	}
	return false
}

// Local reports whether the error implicates the local/source side.
func (e *Error) Local() bool {
	return e != nil && (e.Side == SideLocal || e.Fault == SideLocal || e.Domain == SideLocal)
}

// Remote reports whether the error implicates the remote/replica side.
func (e *Error) Remote() bool {
	return e != nil && (e.Side == SideRemote || e.Fault == SideRemote || e.Domain == SideRemote)
}

func (e Error) effectiveFileID() string {
	if e.FileID != "" {
		return e.FileID
	}
	if e.ID != "" {
		return e.ID
	}
	return e.File
}

func (e Error) effectiveChunkIndex() int {
	if e.ChunkIndex != 0 {
		return e.ChunkIndex
	}
	if e.Index != 0 {
		return e.Index
	}
	return e.Chunk
}

func (e Error) effectiveOffset() int64 {
	if e.Offset != 0 {
		return e.Offset
	}
	if e.Position != 0 {
		return e.Position
	}
	return e.ByteOffset
}

// Location returns the failed chunk index and byte offset.
func (e Error) Location() (chunkIndex int, offset int64) {
	return e.effectiveChunkIndex(), e.effectiveOffset()
}

// FailureLocation is a stable string form of the failure location.
func (e Error) FailureLocation() string {
	return fmt.Sprintf("file=%s chunk=%d offset=%d", e.effectiveFileID(), e.effectiveChunkIndex(), e.effectiveOffset())
}

// Conclusion returns the correlation identity for this failure.
func (e *Error) Conclusion() Conclusion {
	if e == nil {
		return Conclusion{ChunkIndex: -1}
	}
	id := e.effectiveFileID()
	chunkIndex, offset := e.Location()
	return Conclusion{
		FileID:     id,
		Version:    e.Version,
		ChunkIndex: chunkIndex,
		Offset:     offset,
		End:        e.End,
		Expected:   e.Expected,
		Actual:     e.Actual,
		Reason:     e.Kind,
	}
}

// SameConclusion reports whether two errors describe the same file/chunk.
func SameConclusion(a, b error) bool {
	var ea, eb *Error
	if !errors.As(a, &ea) || !errors.As(b, &eb) {
		return false
	}
	return ea.Conclusion().Match(eb.Conclusion())
}

// IsMismatch reports whether err is a content digest mismatch.
func IsMismatch(err error) bool {
	return errors.Is(err, ErrMismatch) || errors.Is(err, ErrChecksumMismatch)
}

// IsUnverified reports whether err represents missing verification metadata.
func IsUnverified(err error) bool {
	return errors.Is(err, ErrUnverified) || errors.Is(err, ErrNoManifest)
}

func shortDigest(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:12] + "..."
}

func newSHA256() hash.Hash { return sha256.New() }

func hashBytes(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

func normalizeDigest(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// NewMismatchError constructs a chunk mismatch error bound to m. It is useful
// for adapters that detect a mismatch outside the streaming Verifier.
func NewMismatchError(side Side, op Operation, m *Manifest, chunkIndex int, actual string) *Error {
	var expected ChunkDigest
	var offset, end int64
	if m != nil && chunkIndex >= 0 && chunkIndex < len(m.Chunks) {
		expected = m.Chunks[chunkIndex]
		offset, end = expected.Offset, expected.Offset+int64(expected.Size)
	}
	return mismatchError(side, op, m, chunkIndex, offset, end, expected.Digest, actual, "checksum_mismatch", ErrMismatch)
}

// NewMismatchErrorForTest is a concise constructor for tests and external
// adapters that want the default operation inferred from side.
func NewMismatchErrorForTest(side Side, m *Manifest, chunkIndex int, actual string) *Error {
	op := OpRead
	if side == SideLocal {
		op = OpWrite
	}
	return NewMismatchError(side, op, m, chunkIndex, actual)
}

func mismatchError(side Side, op Operation, m *Manifest, idx int, offset, end int64, expected, actual, kind string, cause error) *Error {
	if cause == nil {
		cause = ErrMismatch
	}
	id := ""
	version := ""
	var size int64
	if m != nil {
		id = m.FileID
		version = m.CacheVersion()
		size = m.Size
	}
	if kind == "" {
		kind = "checksum_mismatch"
	}
	return &Error{
		FileID:           id,
		ID:               id,
		Version:          version,
		Side:             side,
		Domain:           side,
		Fault:            side,
		Op:               op,
		Status:           StatusFailed,
		Kind:             kind,
		Reason:           kind,
		Type:             kind,
		ChunkIndex:       idx,
		Index:            idx,
		Chunk:            idx,
		Offset:           offset,
		Position:         offset,
		ByteOffset:       offset,
		End:              end,
		Size:             size,
		Expected:         normalizeDigest(expected),
		ExpectedChecksum: normalizeDigest(expected),
		Actual:           normalizeDigest(actual),
		ActualChecksum:   normalizeDigest(actual),
		Err:              cause,
	}
}

// Generator computes chunk and whole-file digests while bytes are streamed to
// the destination. It never requires reading the completed write back.
type Generator struct {
	fileID    string
	version   string
	info      VersionInfo
	chunkSize int

	total     int64
	whole     hash.Hash
	chunkHash hash.Hash
	chunkN    int
	chunks    []ChunkDigest
	finished  bool
}

// NewGenerator creates a streaming manifest generator. The version may be a
// string/Version or a VersionInfo/FileVersion value.
func NewGenerator(fileID string, version any, chunkSize int) *Generator {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	v, info := NormalizeVersion(version)
	return &Generator{
		fileID:    fileID,
		version:   v,
		info:      info,
		chunkSize: chunkSize,
		whole:     newSHA256(),
	}
}

// NewGeneratorWithInfo creates a generator with rich version metadata.
func NewGeneratorWithInfo(fileID string, version VersionInfo, chunkSize int) *Generator {
	return NewGenerator(fileID, version, chunkSize)
}

// Write implements io.Writer and hashes p without retaining it.
func (g *Generator) Write(p []byte) (int, error) {
	if g.finished {
		return 0, fmt.Errorf("integrity: write after manifest finish")
	}
	consumed := 0
	for len(p) > 0 {
		if g.chunkHash == nil {
			g.chunkHash = newSHA256()
			g.chunkN = 0
		}
		space := g.chunkSize - g.chunkN
		take := len(p)
		if take > space {
			take = space
		}
		part := p[:take]
		g.chunkHash.Write(part)
		g.whole.Write(part)
		g.chunkN += take
		g.total += int64(take)
		consumed += take
		p = p[take:]

		if g.chunkN == g.chunkSize {
			g.appendChunk()
		}
	}
	return consumed, nil
}

func (g *Generator) appendChunk() {
	if g.chunkHash == nil || g.chunkN == 0 {
		return
	}
	offset := g.total - int64(g.chunkN)
	sum := hex.EncodeToString(g.chunkHash.Sum(nil))
	g.chunks = append(g.chunks, ChunkDigest{
		Index:     len(g.chunks),
		Offset:    offset,
		Size:      g.chunkN,
		Length:    int64(g.chunkN),
		Digest:    sum,
		Checksum:  sum,
		Hash:      sum,
		SHA256:    sum,
		Algorithm: AlgorithmSHA256,
	})
	g.chunkHash = nil
	g.chunkN = 0
}

// N returns the number of bytes hashed.
func (g *Generator) N() int64 { return g.total }

// Finish finalizes the tail chunk and returns the version-bound manifest.
func (g *Generator) Finish() *Manifest {
	if !g.finished {
		g.appendChunk()
		g.finished = true
	}
	wholeDigest := hex.EncodeToString(g.whole.Sum(nil))
	info := g.info
	info.Size = g.total
	if info.Digest == "" {
		info.Digest = wholeDigest
	}
	if info.Token == "" && info.ETag == "" && info.Version == "" && g.version == "" {
		info.Token = wholeDigest
	}
	version := g.version
	if version == "" {
		switch {
		case info.Version != "":
			version = info.Version
		case info.Token != "":
			version = info.Token
		case info.ETag != "":
			version = info.ETag
		default:
			version = wholeDigest
		}
	}
	return &Manifest{
		SchemaVersion: SchemaVersion,
		Algorithm:     AlgorithmSHA256,
		FileID:        g.fileID,
		Version:       version,
		VersionInfo:   info,
		ChunkSize:     g.chunkSize,
		Size:          g.total,
		Digest:        wholeDigest,
		Checksum:      wholeDigest,
		SHA256:        wholeDigest,
		Chunks:        append([]ChunkDigest(nil), g.chunks...),
		CreatedAt:     time.Now().UTC(),
	}
}

// Manifest is an alias for Finish. It may be called once; subsequent calls
// return the already-finished manifest.
func (g *Generator) Manifest() *Manifest { return g.Finish() }

// Generate reads all of r in one streaming pass and returns its manifest.
// It is intended for existing files or tests; write paths should prefer Tee-ing
// through a Generator while copying to the destination.
func Generate(fileID string, version any, r io.Reader, chunkSize int) (*Manifest, int64, error) {
	g := NewGenerator(fileID, version, chunkSize)
	n, err := io.Copy(g, r)
	if err != nil {
		return nil, n, err
	}
	return g.Finish(), n, nil
}

// Verifier incrementally checks a byte stream against a manifest.
type Verifier struct {
	manifest *Manifest
	side     Side
	op       Operation

	idx       int
	offset    int64
	chunkHash hash.Hash
	chunkN    int

	cache      *ChunkCache
	cacheHits  int
	remember   bool
	persist    bool
	onMismatch func(*Error)
}

// VerifierOption configures a Verifier.
type VerifierOption func(*Verifier)

// WithCache enables chunk digest cache lookup/population.
func WithCache(c *ChunkCache) VerifierOption {
	return func(v *Verifier) { v.cache = c; v.remember = c != nil }
}

// WithPersistCache persists cache updates after the final chunk verifies.
// Save errors are deliberately non-fatal to the already-successful read.
func WithPersistCache() VerifierOption {
	return func(v *Verifier) { v.persist = true }
}

// WithMismatchHook registers a callback before a mismatch is returned. It is
// typically used to quarantine the offending file.
func WithMismatchHook(fn func(*Error)) VerifierOption {
	return func(v *Verifier) { v.onMismatch = fn }
}

// NewVerifier creates a streaming verifier for one manifest.
func NewVerifier(m *Manifest, side Side, opts ...VerifierOption) (*Verifier, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if side == "" {
		side = SideRemote
	}
	v := &Verifier{manifest: m, side: side, op: OpRead}
	if side == SideLocal {
		v.op = OpWrite
	}
	for _, opt := range opts {
		opt(v)
	}
	return v, nil
}

// CacheHits returns the number of chunks already present in the cache.
func (v *Verifier) CacheHits() int { return v.cacheHits }

// Write hashes p and verifies any completed chunks. It returns len(p) when all
// completed chunks match; otherwise it returns a *Error.
func (v *Verifier) Write(p []byte) (int, error) {
	consumed := 0
	for len(p) > 0 {
		if v.idx > len(v.manifest.Chunks) {
			return consumed, v.failExtra()
		}
		if v.idx == len(v.manifest.Chunks) {
			return consumed, v.failExtra()
		}
		expected := v.manifest.Chunks[v.idx]
		if v.chunkHash == nil {
			v.chunkHash = newSHA256()
			v.chunkN = 0
			v.offset = expected.Offset
		}
		need := expected.Size - v.chunkN
		take := len(p)
		if take > need {
			take = need
		}
		v.chunkHash.Write(p[:take])
		v.chunkN += take
		consumed += take
		p = p[take:]

		if v.chunkN == expected.Size {
			if err := v.checkChunk(expected); err != nil {
				return consumed, err
			}
			v.idx++
			v.chunkHash = nil
			v.chunkN = 0
		}
	}
	return consumed, nil
}

func (v *Verifier) checkChunk(expected ChunkDigest) error {
	actual := hex.EncodeToString(v.chunkHash.Sum(nil))
	end := expected.Offset + int64(expected.Size)
	if !strings.EqualFold(actual, expected.Digest) {
		err := mismatchError(v.side, v.op, v.manifest, expected.Index, expected.Offset, end, expected.Digest, actual, "checksum_mismatch", ErrMismatch)
		if v.onMismatch != nil {
			v.onMismatch(err)
		}
		return err
	}

	version := v.manifest.CacheVersion()
	if v.cache != nil {
		if v.cache.HasOnSide(v.manifest.FileID, version, expected.Index, actual, v.side) {
			v.cacheHits++
		} else if v.remember {
			v.cache.Remember(ChunkRecord{
				FileID:     v.manifest.FileID,
				Version:    version,
				ChunkIndex: expected.Index,
				Offset:     expected.Offset,
				Size:       expected.Size,
				Digest:     actual,
				Status:     StatusVerified,
				Side:       v.side,
				VerifiedAt: time.Now().UTC(),
			})
		}
		if v.persist && expected.Index == len(v.manifest.Chunks)-1 {
			_ = v.cache.Save()
		}
	}
	return nil
}

// Finish verifies that exactly all manifest bytes were supplied.
func (v *Verifier) Finish() error {
	if v.idx < len(v.manifest.Chunks) {
		var expected ChunkDigest
		if v.idx < len(v.manifest.Chunks) {
			expected = v.manifest.Chunks[v.idx]
		}
		offset := v.offset + int64(v.chunkN)
		err := mismatchError(v.side, v.op, v.manifest, v.idx, offset, expected.Offset+int64(expected.Size), expected.Digest, "", "truncated", ErrTruncated)
		err.BytesRead = v.manifest.Chunks[v.idx].Offset + int64(v.chunkN)
		if v.onMismatch != nil {
			v.onMismatch(err)
		}
		return err
	}
	return nil
}

func (v *Verifier) failExtra() error {
	offset := v.manifest.Size
	err := mismatchError(v.side, v.op, v.manifest, -1, offset, offset, "", "", "extra_data", ErrExtraData)
	if v.onMismatch != nil {
		v.onMismatch(err)
	}
	return err
}

// VerifyReader consumes all of r in one pass. It is useful for explicit
// end-to-end verification. Streaming callers can instead use NewReadCloser.
func VerifyReader(r io.Reader, m *Manifest, side Side, opts ...VerifierOption) error {
	v, err := NewVerifier(m, side, opts...)
	if err != nil {
		return err
	}
	if _, err := io.Copy(v, r); err != nil {
		return err
	}
	return v.Finish()
}

// verifiedChunkSize is a helper that defaults a manifest chunk size safely.
func verifiedChunkSize(m *Manifest) int {
	if m != nil && m.ChunkSize > 0 {
		return m.ChunkSize
	}
	return DefaultChunkSize
}
