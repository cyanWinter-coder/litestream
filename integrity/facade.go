package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
)

// Manager is a descriptive alias for Service.
type Manager = Service

// NewManager constructs a Service/Manager. It accepts a ServiceConfig, a
// metadata directory string, a chunk size int, or component values.
func NewManager(args ...any) (*Manager, error) {
	cfg := ServiceConfig{}
	for _, arg := range args {
		switch v := arg.(type) {
		case ServiceConfig:
			cfg = v
		case string:
			cfg.MetadataDir = v
		case int:
			cfg.ChunkSize = v
		case ManifestStore:
			cfg.ManifestStore = v
		case *ChunkCache:
			cfg.Cache = v
		case *Quarantine:
			if v != nil {
				cfg.QuarantineDir = v.Dir
			}
		}
	}
	return NewService(cfg)
}

// MustNewManager constructs a Manager or panics.
func MustNewManager(args ...any) *Manager {
	m, err := NewManager(args...)
	if err != nil {
		panic(err)
	}
	return m
}

// Checker is the minimal verification interface.
type Checker interface {
	Check(r io.Reader, m *Manifest) error
}

// GenerateManifest is the two-return convenience form of Generate.
func GenerateManifest(fileID string, r io.Reader, chunkSize int) (*Manifest, error) {
	m, _, err := Generate(fileID, nil, r, chunkSize)
	return m, err
}

// GenerateManifestWithVersion is the two-return version-bound form.
func GenerateManifestWithVersion(fileID string, version any, r io.Reader, chunkSize int) (*Manifest, error) {
	m, _, err := Generate(fileID, version, r, chunkSize)
	return m, err
}

// Verify is a short alias for VerifyReader.
func Verify(r io.Reader, m *Manifest, side Side, opts ...VerifierOption) error {
	return VerifyReader(r, m, side, opts...)
}

// ValidateContent is a descriptive alias for VerifyReader.
func ValidateContent(r io.Reader, m *Manifest, side Side, opts ...VerifierOption) error {
	return VerifyReader(r, m, side, opts...)
}

// VerifyManifest is an alias for Verify.
func VerifyManifest(r io.Reader, m *Manifest, side Side, opts ...VerifierOption) error {
	return VerifyReader(r, m, side, opts...)
}

// ComputeChunkDigest computes a ChunkDigest for p. Index and offset are copied
// from the supplied template when provided.
func ComputeChunkDigest(p []byte, template ...ChunkDigest) ChunkDigest {
	d := ChunkDigest{Size: len(p), Length: int64(len(p)), Digest: hashBytes(p), Algorithm: AlgorithmSHA256}
	if len(template) > 0 {
		d.Index = template[0].Index
		d.Offset = template[0].Offset
		if template[0].Size > 0 {
			d.Size = template[0].Size
		}
	}
	return d
}

// VerifyChunk reports whether p matches the expected chunk digest.
func VerifyChunk(expected ChunkDigest, p []byte) error {
	expectedDigest := firstNonEmptyDigest(expected.Digest, expected.Checksum, expected.Hash, expected.SHA256)
	actual := hashBytes(p)
	if strings.EqualFold(actual, normalizeDigest(expectedDigest)) {
		return nil
	}
	return &Error{
		Kind:             "checksum_mismatch",
		Reason:           "checksum_mismatch",
		Type:             "checksum_mismatch",
		ChunkIndex:       expected.Index,
		Index:            expected.Index,
		Chunk:            expected.Index,
		Offset:           expected.Offset,
		Position:         expected.Offset,
		ByteOffset:       expected.Offset,
		End:              expected.Offset + int64(expected.Size),
		Expected:         expectedDigest,
		ExpectedChecksum: firstNonEmptyDigest(expected.Checksum, expectedDigest),
		Actual:           actual,
		ActualChecksum:   actual,
		Err:              ErrMismatch,
	}
}

// ComputeChecksum returns the lowercase SHA-256 checksum of all bytes in r.
func ComputeChecksum(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ChecksumFile returns the whole-file SHA-256 checksum.
func ChecksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return ComputeChecksum(f)
}

// Validate is a context-aware convenience for service verification.
func (s *Service) Validate(ctx context.Context, path, fileID string, want any, side Side) (*Manifest, bool, error) {
	return s.VerifyFile(ctx, path, fileID, want, side)
}
