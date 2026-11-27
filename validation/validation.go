// Package validation re-exports content validation primitives from ../integrity.
package validation

import (
	"io"

	"github.com/benbjohnson/litestream/integrity"
)

const (
	DefaultChunkSize = integrity.DefaultChunkSize
	SideLocal        = integrity.SideLocal
	SideRemote       = integrity.SideRemote
	StatusUnverified = integrity.StatusUnverified
	StatusVerified   = integrity.StatusVerified
	StatusFailed     = integrity.StatusFailed
)

var (
	ErrMismatch        = integrity.ErrMismatch
	ErrChecksum        = integrity.ErrChecksumMismatch
	ErrUnverified      = integrity.ErrUnverified
	ErrNoManifest      = integrity.ErrNoManifest
	ErrVersionMismatch = integrity.ErrVersionMismatch
)

type (
	Side        = integrity.Side
	Status      = integrity.Status
	Version     = integrity.Version
	FileVersion = integrity.FileVersion
	VersionInfo = integrity.VersionInfo

	Manifest         = integrity.Manifest
	ValidationReport = integrity.Conclusion
	Report           = integrity.Conclusion
	Result           = integrity.Conclusion
	Conclusion       = integrity.Conclusion

	Chunk           = integrity.ChunkDigest
	ChunkDigest     = integrity.ChunkDigest
	Error           = integrity.Error
	ValidationError = integrity.Error

	Generator  = integrity.Generator
	Verifier   = integrity.Verifier
	Validator  = integrity.Verifier
	ReadCloser = integrity.ReadCloser
	Reader     = integrity.Reader
	Cache      = integrity.ChunkCache
	Quarantine = integrity.Quarantine
	Service    = integrity.Service
	Config     = integrity.ServiceConfig
)

func NewGenerator(fileID string, version Version, chunkSize int) *Generator {
	return integrity.NewGenerator(fileID, version, chunkSize)
}

func NewValidator(m *Manifest, side Side, opts ...integrity.VerifierOption) (*Validator, error) {
	return integrity.NewVerifier(m, side, opts...)
}

func Verify(r io.Reader, m *Manifest, side Side, opts ...integrity.VerifierOption) error {
	return integrity.VerifyReader(r, m, side, opts...)
}

func ValidateContent(r io.Reader, m *Manifest, side Side, opts ...integrity.VerifierOption) error {
	return integrity.VerifyReader(r, m, side, opts...)
}

func GenerateManifest(fileID string, r io.Reader, chunkSize int) (*Manifest, error) {
	return integrity.GenerateManifest(fileID, r, chunkSize)
}

func NewQuarantine(dir string) *Quarantine { return integrity.NewQuarantine(dir) }
