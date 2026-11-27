// Package digest re-exports chunk digest primitives from ../integrity.
package digest

import (
	"io"

	"github.com/benbjohnson/litestream/integrity"
)

const DefaultChunkSize = integrity.DefaultChunkSize

type (
	Algorithm = string
	Version   = integrity.Version
	Manifest  = integrity.Manifest
	Digest    = integrity.ChunkDigest
	Chunk     = integrity.ChunkDigest
	Cache     = integrity.ChunkCache
	Generator = integrity.Generator
)

const SHA256 Algorithm = integrity.AlgorithmSHA256

func NewGenerator(fileID string, version Version, chunkSize int) *Generator {
	return integrity.NewGenerator(fileID, version, chunkSize)
}

func Generate(fileID string, version Version, r io.Reader, chunkSize int) (*Manifest, int64, error) {
	return integrity.Generate(fileID, version, r, chunkSize)
}

func GenerateManifest(fileID string, r io.Reader, chunkSize int) (*Manifest, error) {
	return integrity.GenerateManifest(fileID, r, chunkSize)
}

func Compute(r io.Reader) (string, error) { return integrity.ComputeChecksum(r) }

func File(path string) (string, error) { return integrity.ChecksumFile(path) }

func NewCache(path string) *Cache { return integrity.NewChunkCache(path) }
