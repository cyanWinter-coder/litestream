// Package content re-exports content validation helpers from ../integrity.
package content

import "github.com/benbjohnson/litestream/integrity"

type (
	Error             = integrity.Error
	Manifest          = integrity.Manifest
	ChunkDigest       = integrity.ChunkDigest
	FileVersion       = integrity.FileVersion
	VersionInfo       = integrity.VersionInfo
	Quarantine        = integrity.Quarantine
	VerificationCache = integrity.ChunkCache
)

var (
	ErrMismatch   = integrity.ErrMismatch
	ErrUnverified = integrity.ErrUnverified
	ErrNoManifest = integrity.ErrNoManifest
)

func NewQuarantine(dir string) *Quarantine { return integrity.NewQuarantine(dir) }
