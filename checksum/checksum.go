// Package checksum re-exports the streaming content integrity primitives.
//
// The implementation lives in ../integrity; this package provides the common
// checksum-oriented name.
package checksum

import (
	"io"

	"github.com/benbjohnson/litestream/integrity"
)

const (
	DefaultChunkSize = integrity.DefaultChunkSize
	AlgorithmSHA256  = integrity.AlgorithmSHA256

	SideLocal  = integrity.SideLocal
	SideRemote = integrity.SideRemote

	StatusUnverified = integrity.StatusUnverified
	StatusVerified   = integrity.StatusVerified
	StatusFailed     = integrity.StatusFailed
)

var (
	ErrMismatch        = integrity.ErrMismatch
	ErrChecksum        = integrity.ErrChecksumMismatch
	ErrChecksumMissing = integrity.ErrNoManifest
	ErrUnverified      = integrity.ErrUnverified
	ErrVersionMismatch = integrity.ErrVersionMismatch
)

type (
	Side        = integrity.Side
	Status      = integrity.Status
	Version     = integrity.Version
	FileVersion = integrity.FileVersion
	VersionInfo = integrity.VersionInfo

	Manifest         = integrity.Manifest
	FileManifest     = integrity.Manifest
	ChunkManifest    = integrity.Manifest
	ChecksumManifest = integrity.Manifest

	Chunk         = integrity.ChunkDigest
	ChunkChecksum = integrity.ChunkDigest
	Digest        = integrity.ChunkDigest
	Block         = integrity.ChunkDigest

	Error             = integrity.Error
	MismatchError     = integrity.Error
	ChecksumError     = integrity.Error
	VerificationError = integrity.Error

	Generator     = integrity.Generator
	Verifier      = integrity.Verifier
	Reader        = integrity.Reader
	ReadCloser    = integrity.ReadCloser
	Cache         = integrity.ChunkCache
	ChunkCache    = integrity.ChunkCache
	Quarantine    = integrity.Quarantine
	Service       = integrity.Service
	Manager       = integrity.Service
	ServiceConfig = integrity.ServiceConfig
	Config        = integrity.ServiceConfig
)

func NewGenerator(fileID string, version Version, chunkSize int) *Generator {
	return integrity.NewGenerator(fileID, version, chunkSize)
}

func NewVerifier(m *Manifest, side Side, opts ...integrity.VerifierOption) (*Verifier, error) {
	return integrity.NewVerifier(m, side, opts...)
}

func Generate(fileID string, version Version, r io.Reader, chunkSize int) (*Manifest, int64, error) {
	return integrity.Generate(fileID, version, r, chunkSize)
}

func VerifyReader(r io.Reader, m *Manifest, side Side, opts ...integrity.VerifierOption) error {
	return integrity.VerifyReader(r, m, side, opts...)
}

func NewCache(path string) *Cache { return integrity.NewChunkCache(path) }

func LoadCache(path string) (*Cache, error) { return integrity.LoadChunkCache(path) }

func NewQuarantine(dir string) *Quarantine { return integrity.NewQuarantine(dir) }

func Compute(r io.Reader) (string, error) { return integrity.ComputeChecksum(r) }

func File(path string) (string, error) { return integrity.ChecksumFile(path) }

func VerifyChunk(chunk Chunk, p []byte) error { return integrity.VerifyChunk(chunk, p) }

func NewService(args ...any) (*Service, error) { return integrity.NewManager(args...) }

func NewManager(args ...any) (*Manager, error) { return integrity.NewManager(args...) }

func IsMismatch(err error) bool { return integrity.IsMismatch(err) }

func SameFailure(a, b error) bool { return integrity.SameConclusion(a, b) }
