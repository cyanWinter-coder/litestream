package litestream

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/integrity"
)

// Content-integrity aliases. These give callers and tests a stable Litestream
// package surface while the implementation lives in the dependency-free
// integrity package.
type (
	IntegritySide    = integrity.Side
	IntegrityStatus  = integrity.Status
	IntegrityError   = integrity.Error
	ContentIntegrity = integrity.Service

	IntegrityManifest = integrity.Manifest
	ChecksumManifest  = integrity.Manifest
	ContentManifest   = integrity.Manifest

	ChunkDigest    = integrity.ChunkDigest
	ChunkChecksum  = integrity.ChunkDigest
	BlockDigest    = integrity.ChunkDigest
	BlockChecksum  = integrity.ChunkDigest
	IntegrityChunk = integrity.ChunkDigest

	IntegrityVersion     = integrity.Version
	IntegrityVersionInfo = integrity.VersionInfo
	FileVersion          = integrity.FileVersion
	VersionID            = integrity.VersionID

	IntegrityCache     = integrity.ChunkCache
	ChunkDigestCache   = integrity.ChunkCache
	DigestCache        = integrity.DigestCache
	Quarantine         = integrity.Quarantine
	ContentValidator   = integrity.Verifier
	IntegrityValidator = integrity.Verifier
	ChecksumValidator  = integrity.Verifier

	ContentValidationError = integrity.Error
	ContentChecksumError   = integrity.Error
	ReplicaIntegrityError  = integrity.Error

	ValidationReport   = integrity.Conclusion
	VerificationResult = integrity.Conclusion
	IntegrityReport    = integrity.Conclusion

	ChecksumMetadata = integrity.Manifest
	DigestManifest   = integrity.Manifest
	FileChecksum     = integrity.Manifest
	ContentDigest    = integrity.ChunkDigest
)

// IntegrityOption configures a ValidatingReplicaClient.
type IntegrityOption func(*ValidatingReplicaClient)

// WithIntegrityManifestStore sets the manifest store.
func WithIntegrityManifestStore(store integrity.ManifestStore) IntegrityOption {
	return func(c *ValidatingReplicaClient) { c.Manifests = store }
}

// WithIntegrityCache sets the chunk verification cache.
func WithIntegrityCache(cache *integrity.ChunkCache) IntegrityOption {
	return func(c *ValidatingReplicaClient) { c.Cache = cache }
}

// WithIntegrityQuarantine sets the quarantine manager.
func WithIntegrityQuarantine(q *integrity.Quarantine) IntegrityOption {
	return func(c *ValidatingReplicaClient) { c.Quarantine = q }
}

// WithRequiredIntegrityManifests makes missing manifests hard errors.
func WithRequiredIntegrityManifests(required bool) IntegrityOption {
	return func(c *ValidatingReplicaClient) { c.RequireManifest = required }
}

// Side/status constants re-exported for a readable root-package API.
const (
	// DefaultContentChunkSize is the default chunk digest size.
	DefaultContentChunkSize = integrity.DefaultChunkSize

	IntegritySideLocal  = integrity.SideLocal
	IntegritySideRemote = integrity.SideRemote

	IntegrityStatusUnknown    = integrity.StatusUnknown
	IntegrityStatusUnverified = integrity.StatusUnverified
	IntegrityStatusVerified   = integrity.StatusVerified
	IntegrityStatusCached     = integrity.StatusCached
	IntegrityStatusFailed     = integrity.StatusFailed
)

// Additional sentinels. ErrChecksumMismatch preserves the historical root
// sentinel identity while sharing the integrity package implementation.
var (
	ErrChecksumMismatch  = integrity.ErrChecksumMismatch
	ErrContentMismatch   = integrity.ErrMismatch
	ErrIntegrityMismatch = integrity.ErrMismatch
	ErrNoChecksumInfo    = integrity.ErrNoManifest
	ErrContentUnverified = integrity.ErrUnverified
	ErrIntegrityVersion  = integrity.ErrVersionMismatch
)

// LTXContentFileID returns the stable writer/reader correlation ID for an LTX
// file. It intentionally does not contain a local path: writer and reader may
// have different mount points for the same replica.
func LTXContentFileID(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("ltx:L%d:%016x-%016x", level, uint64(minTXID), uint64(maxTXID))
}

// NewIntegrityService constructs a content integrity service.
func NewIntegrityService(cfg integrity.ServiceConfig) (*integrity.Service, error) {
	return integrity.NewService(cfg)
}

// NewIntegrityManager constructs a content integrity manager from a metadata
// directory, config, or component arguments.
func NewIntegrityManager(args ...any) (*integrity.Service, error) {
	return integrity.NewManager(args...)
}

// ChecksumManager and related names describe the same service in checksum terms.
type (
	ChecksumManager  = integrity.Service
	IntegrityManager = integrity.Service
	ContentManager   = integrity.Service
)

// NewChecksumManager constructs an integrity.Service.
func NewChecksumManager(args ...any) (*integrity.Service, error) {
	return integrity.NewManager(args...)
}

// NewChunkDigestCache constructs a persisted/in-memory chunk digest cache.
func NewChunkDigestCache(path string) *integrity.ChunkCache {
	return integrity.NewChunkCache(path)
}

// NewChunkDigestGenerator constructs a streaming chunk digest generator.
func NewChunkDigestGenerator(fileID string, version integrity.Version, chunkSize int) *integrity.Generator {
	return integrity.NewGenerator(fileID, version, chunkSize)
}

// NewIntegrityQuarantine constructs a quarantine directory.
func NewIntegrityQuarantine(dir string) *integrity.Quarantine {
	return integrity.NewQuarantine(dir)
}

// ValidatingReplicaClient wraps any ReplicaClient with version-bound chunk
// manifest generation on writes and verify-before-release reads.
//
// ManifestStore should be shared by writer and reader. A local-only store still
// provides repeated-read caching on one host, but cannot tell another restore
// process the expected digests.
type ValidatingReplicaClient struct {
	ReplicaClient

	Manifests       integrity.ManifestStore
	Cache           *integrity.ChunkCache
	Quarantine      *integrity.Quarantine
	ChunkSize       int
	RequireManifest bool
}

// NewValidatingReplicaClient wraps client.
//
// It accepts either a store/cache pair or functional IntegrityOption values.
// The loose variadic form keeps equivalent construction styles source-compatible.
func NewValidatingReplicaClient(client ReplicaClient, args ...any) *ValidatingReplicaClient {
	c := &ValidatingReplicaClient{
		ReplicaClient: client,
		ChunkSize:     integrity.DefaultChunkSize,
	}
	for _, arg := range args {
		switch v := arg.(type) {
		case IntegrityOption:
			v(c)
		case func(*ValidatingReplicaClient):
			v(c)
		case integrity.ManifestStore:
			c.Manifests = v
		case *integrity.ChunkCache:
			c.Cache = v
		case *integrity.Quarantine:
			c.Quarantine = v
		case int:
			c.ChunkSize = v
		case bool:
			c.RequireManifest = v
		case integrity.ServiceConfig:
			if v.ChunkSize > 0 {
				c.ChunkSize = v.ChunkSize
			}
			if v.QuarantineDir != "" {
				c.Quarantine = integrity.NewQuarantine(v.QuarantineDir)
			}
		}
	}
	if c.Manifests == nil {
		c.Manifests = integrity.NewMemoryManifestStore()
	}
	if c.Cache == nil {
		c.Cache = integrity.NewChunkCache("")
	}
	return c
}

// NewChecksumReplicaClient wraps client with an in-memory manifest/cache.
func NewChecksumReplicaClient(client ReplicaClient) *ValidatingReplicaClient {
	return NewValidatingReplicaClient(client)
}

// NewIntegrityReplicaClient is an alias for NewChecksumReplicaClient.
func NewIntegrityReplicaClient(client ReplicaClient) *ValidatingReplicaClient {
	return NewChecksumReplicaClient(client)
}

// GenerateIntegrityManifest is a root-level two-return manifest helper.
func GenerateIntegrityManifest(fileID string, r io.Reader, chunkSize int) (*integrity.Manifest, error) {
	return integrity.GenerateManifest(fileID, r, chunkSize)
}

// VerifyContentIntegrity verifies a reader against a manifest.
func VerifyContentIntegrity(r io.Reader, m *integrity.Manifest, side integrity.Side) error {
	return integrity.VerifyReader(r, m, side)
}

// NewContentValidator constructs a streaming content verifier.
func NewContentValidator(m *integrity.Manifest, side integrity.Side, opts ...integrity.VerifierOption) (*integrity.Verifier, error) {
	return integrity.NewVerifier(m, side, opts...)
}

// ValidateContentIntegrity is an alias for VerifyContentIntegrity.
func ValidateContentIntegrity(r io.Reader, m *integrity.Manifest, side integrity.Side) error {
	return integrity.VerifyReader(r, m, side)
}

// SameIntegrityFailure reports whether write-side and read-side errors identify
// the same file, chunk, offset, and expected digest.
func SameIntegrityFailure(a, b error) bool { return integrity.SameConclusion(a, b) }

// IsContentMismatch reports whether err is a content digest mismatch.
func IsContentMismatch(err error) bool { return integrity.IsMismatch(err) }

// IsContentUnverified reports whether err means no trusted checksum metadata.
func IsContentUnverified(err error) bool { return integrity.IsUnverified(err) }

// ComputeContentChecksum returns the whole-stream SHA-256 digest.
func ComputeContentChecksum(r io.Reader) (string, error) {
	return integrity.ComputeChecksum(r)
}

// ChecksumContentFile returns a file's whole-content SHA-256 digest.
func ChecksumContentFile(path string) (string, error) {
	return integrity.ChecksumFile(path)
}

func (c *ValidatingReplicaClient) chunkSize() int {
	if c.ChunkSize > 0 {
		return c.ChunkSize
	}
	return integrity.DefaultChunkSize
}

// WriteLTXFile tees the upload through a chunk generator without re-reading the
// fully written file.
func (c *ValidatingReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	fileID := LTXContentFileID(level, minTXID, maxTXID)
	g := integrity.NewGenerator(fileID, integrity.FileVersion{}, c.chunkSize())

	var sourceErr error
	tracked := &readErrorTracker{r: r, errPtr: &sourceErr}
	info, err := c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, io.TeeReader(tracked, g))
	if err != nil {
		if sourceErr != nil {
			return nil, &integrity.Error{
				FileID: fileID, ID: fileID,
				Side: integrity.SideLocal, Fault: integrity.SideLocal, Op: integrity.OpWrite,
				Status: integrity.StatusFailed, Kind: "source_read", ChunkIndex: -1,
				Err: errors.Join(sourceErr, err),
			}
		}
		return nil, err
	}

	m := g.Finish()
	if info != nil {
		m.VersionInfo.Size = info.Size
		m.Size = info.Size
	}
	if c.Manifests != nil {
		if saveErr := c.Manifests.SaveManifest(ctx, m); saveErr != nil {
			return info, fmt.Errorf("save integrity manifest: %w", saveErr)
		}
	}
	if c.Cache != nil {
		c.Cache.MarkManifest(m)
	}
	return info, nil
}

// OpenLTXFile returns a reader that rejects a mismatching chunk before its
// bytes are released. Full-object opens are verified; range opens are passed
// through because chunk verification may require adjacent bytes.
func (c *ValidatingReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err != nil {
		return nil, err
	}
	fileID := LTXContentFileID(level, minTXID, maxTXID)
	if c.Manifests == nil {
		return rc, nil
	}
	m, err := c.Manifests.LoadManifest(ctx, fileID, integrity.FileVersion{})
	if err != nil {
		_ = rc.Close()
		if errors.Is(err, integrity.ErrNoManifest) && !c.RequireManifest {
			return rc, nil
		}
		if errors.Is(err, integrity.ErrNoManifest) {
			return nil, fmt.Errorf("%w: %s", integrity.ErrUnverified, fileID)
		}
		return nil, err
	}
	if offset != 0 || (size > 0 && size != m.Size) {
		return rc, nil
	}

	hook := func(verr *integrity.Error) {
		if verr.FileID == "" {
			verr.FileID = fileID
			verr.ID = fileID
		}
		if c.Quarantine == nil {
			return
		}
		// Failure path only: make one extra full-object request to preserve
		// the remote bytes for diagnosis.
		preserveCtx := context.WithoutCancel(ctx)
		bad, openErr := c.ReplicaClient.OpenLTXFile(preserveCtx, level, minTXID, maxTXID, 0, 0)
		if openErr != nil {
			return
		}
		defer bad.Close()
		_, _ = c.Quarantine.PreserveReader(bad, verr)
	}
	opts := []integrity.VerifierOption{integrity.WithMismatchHook(hook)}
	if c.Cache != nil {
		opts = append(opts, integrity.WithCache(c.Cache))
	}
	vrc, err := integrity.NewReadCloser(rc, m, integrity.SideRemote, opts...)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	return vrc, nil
}

type readErrorTracker struct {
	r      io.Reader
	errPtr *error
}

func (t *readErrorTracker) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && t.errPtr != nil && *t.errPtr == nil {
		*t.errPtr = err
	}
	return n, err
}
