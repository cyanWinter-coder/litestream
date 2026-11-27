package litestream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

// Compactor handles compaction and retention for LTX files.
// It operates solely through the ReplicaClient interface, making it
// suitable for both DB (with local file caching) and VFS (remote-only).
type Compactor struct {
	client ReplicaClient
	logger *slog.Logger

	// retentionMu serializes retention enforcement runs so that a duplicate
	// scheduler tick cannot delete the same batch of files concurrently.
	// Compaction itself does not take this lock; it guards only deletions.
	retentionMu sync.Mutex

	// VerifyCompaction enables post-compaction TXID consistency verification.
	// When enabled, verifies that files at the destination level have
	// contiguous TXID ranges after each compaction. Disabled by default.
	VerifyCompaction bool

	// RetentionEnabled controls whether Litestream actively deletes old files
	// during retention enforcement. When false, cloud provider lifecycle
	// policies handle retention instead. Local file cleanup still occurs.
	RetentionEnabled bool

	// CompactionVerifyErrorCounter is incremented when post-compaction
	// verification fails. Optional; if nil, no metric is recorded.
	CompactionVerifyErrorCounter prometheus.Counter

	// LocalFileOpener optionally opens a local LTX file for compaction.
	// If nil or returns os.ErrNotExist, falls back to remote.
	// This is used by DB to prefer local files over remote for consistency.
	LocalFileOpener func(level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error)

	// LocalFileDeleter optionally deletes local LTX files after retention.
	// If nil, only remote files are deleted.
	LocalFileDeleter func(level int, minTXID, maxTXID ltx.TXID) error

	// CacheGetter optionally retrieves cached MaxLTXFileInfo for a level.
	// If nil, max file info is always fetched from remote.
	CacheGetter func(level int) (*ltx.FileInfo, bool)

	// CacheSetter optionally stores MaxLTXFileInfo for a level.
	// If nil, max file info is not cached.
	CacheSetter func(level int, info *ltx.FileInfo)

	// RetentionExistenceChecker optionally verifies that a file is still
	// present and unchanged immediately before retention deletes it. A false
	// result skips the file until the next run so files a concurrent backup
	// is writing are never removed prematurely.
	RetentionExistenceChecker func(info *ltx.FileInfo) bool
}

// NewCompactor creates a new Compactor with the given client and logger.
func NewCompactor(client ReplicaClient, logger *slog.Logger) *Compactor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Compactor{
		client:           client,
		logger:           logger,
		RetentionEnabled: true,
	}
}

func (c *Compactor) setLogger(logger *slog.Logger) {
	c.logger = logger
}

// MaxLTXFileInfo returns metadata for the last LTX file in a level.
// Uses cache if available, otherwise fetches from remote.
func (c *Compactor) MaxLTXFileInfo(ctx context.Context, level int) (ltx.FileInfo, error) {
	if c.CacheGetter != nil {
		if info, ok := c.CacheGetter(level); ok {
			return *info, nil
		}
	}

	itr, err := c.client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return ltx.FileInfo{}, err
	}
	defer itr.Close()

	var info ltx.FileInfo
	for itr.Next() {
		item := itr.Item()
		if item.MaxTXID > info.MaxTXID {
			info = *item
		}
	}

	if c.CacheSetter != nil && info.MaxTXID > 0 {
		c.CacheSetter(level, &info)
	}

	return info, itr.Close()
}

// Compact compacts source level files into the destination level.
// Returns ErrNoCompaction if there are no files to compact.
func (c *Compactor) Compact(ctx context.Context, dstLevel int) (*ltx.FileInfo, error) {
	srcLevel := dstLevel - 1

	prevMaxInfo, err := c.MaxLTXFileInfo(ctx, dstLevel)
	if err != nil {
		return nil, fmt.Errorf("cannot determine max ltx file for destination level: %w", err)
	}
	seekTXID := prevMaxInfo.MaxTXID + 1

	itr, err := c.client.LTXFiles(ctx, srcLevel, seekTXID, false)
	if err != nil {
		return nil, fmt.Errorf("source ltx files after %s: %w", seekTXID, err)
	}
	defer itr.Close()

	var rdrs []io.Reader
	defer func() {
		for _, rd := range rdrs {
			if closer, ok := rd.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	}()

	var minTXID, maxTXID ltx.TXID
	for itr.Next() {
		info := itr.Item()

		if minTXID == 0 || info.MinTXID < minTXID {
			minTXID = info.MinTXID
		}
		if maxTXID == 0 || info.MaxTXID > maxTXID {
			maxTXID = info.MaxTXID
		}

		if c.LocalFileOpener != nil {
			if f, err := c.LocalFileOpener(srcLevel, info.MinTXID, info.MaxTXID); err == nil {
				rdrs = append(rdrs, f)
				continue
			} else if !os.IsNotExist(err) {
				return nil, fmt.Errorf("open local ltx file: %w", err)
			}
		}

		f, err := c.client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("open ltx file: %w", err)
		}
		rdrs = append(rdrs, internal.NewResumableReader(ctx, c.client, info.Level, info.MinTXID, info.MaxTXID, info.Size, f, c.logger))
	}
	if len(rdrs) == 0 {
		return nil, ErrNoCompaction
	}

	pr, pw := io.Pipe()
	go func() {
		comp, err := ltx.NewCompactor(pw, rdrs)
		if err != nil {
			_ = pw.CloseWithError(fmt.Errorf("new ltx compactor: %w", err))
			return
		}
		comp.HeaderFlags = ltx.HeaderFlagNoChecksum
		_ = pw.CloseWithError(comp.Compact(ctx))
	}()

	info, err := c.client.WriteLTXFile(ctx, dstLevel, minTXID, maxTXID, pr)
	_ = pr.CloseWithError(err)
	if err != nil {
		return nil, fmt.Errorf("write ltx file: %w", err)
	}

	if c.CacheSetter != nil {
		c.CacheSetter(dstLevel, info)
	}

	// Verify level consistency if enabled
	if c.VerifyCompaction {
		if err := c.VerifyLevelConsistency(ctx, dstLevel); err != nil {
			c.logger.Warn("post-compaction verification failed",
				"level", dstLevel,
				"error", err)
			if c.CompactionVerifyErrorCounter != nil {
				c.CompactionVerifyErrorCounter.Inc()
			}
		}
	}

	return info, nil
}

// VerifyLevelConsistency checks that LTX files at the given level have
// contiguous TXID ranges (prevMaxTXID + 1 == currMinTXID for consecutive files).
// Returns an error describing any gaps or overlaps found.
func (c *Compactor) VerifyLevelConsistency(ctx context.Context, level int) error {
	itr, err := c.client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var prevInfo *ltx.FileInfo
	for itr.Next() {
		info := itr.Item()

		// Skip first file - nothing to compare against
		if prevInfo == nil {
			prevInfo = info
			continue
		}

		// Check for TXID contiguity: prev.MaxTXID + 1 should equal curr.MinTXID
		expectedMinTXID := prevInfo.MaxTXID + 1
		if info.MinTXID != expectedMinTXID {
			if info.MinTXID > expectedMinTXID {
				return fmt.Errorf("TXID gap detected: prev.MaxTXID=%s, next.MinTXID=%s (expected %s)",
					prevInfo.MaxTXID, info.MinTXID, expectedMinTXID)
			}
			return fmt.Errorf("TXID overlap detected: prev.MaxTXID=%s, next.MinTXID=%s",
				prevInfo.MaxTXID, info.MinTXID)
		}

		prevInfo = info
	}

	if err := itr.Close(); err != nil {
		return fmt.Errorf("close iterator: %w", err)
	}

	return nil
}

// EnforceSnapshotRetention enforces retention of snapshot level files while
// preserving a recoverable chain for every time point inside the retention
// window.
//
// A snapshot is only deleted when the next-younger snapshot was itself created
// before the cutoff: that younger snapshot remains available as the recovery
// anchor for every time point the older one could serve. Deleting any snapshot
// whose successor is still inside the window would strand point-in-time
// recovery at an older position, so it is protected regardless of its age.
// Lower-level files are evaluated separately through EnforceRetentionByTXID.
//
// Returns the MaxTXID of the oldest retained snapshot (the recovery anchor),
// useful for cascading retention to lower levels.
func (c *Compactor) EnforceSnapshotRetention(ctx context.Context, retention time.Duration) (ltx.TXID, error) {
	if !c.retentionMu.TryLock() {
		c.logger.Debug("retention enforcement already in progress, skipping")
		return 0, ErrRetentionInProgress
	}
	defer c.retentionMu.Unlock()

	cutoff := time.Now().Add(-retention)
	enforcedAt := time.Now()
	c.logger.Debug("enforcing snapshot retention", "cutoff", cutoff)

	itr, err := c.client.LTXFiles(ctx, SnapshotLevel, 0, false)
	if err != nil {
		return 0, fmt.Errorf("fetch ltx files: %w", err)
	}
	var snapshots []*ltx.FileInfo
	for itr.Next() {
		snapshots = append(snapshots, itr.Item())
	}
	if err := itr.Close(); err != nil {
		return 0, fmt.Errorf("close snapshot iterator: %w", err)
	}
	slices.SortFunc(snapshots, func(a, b *ltx.FileInfo) int {
		if a.MinTXID != b.MinTXID {
			if a.MinTXID < b.MinTXID {
				return -1
			}
			return 1
		}
		if a.MaxTXID < b.MaxTXID {
			return -1
		}
		return 1
	})

	if len(snapshots) == 0 {
		return 0, nil
	}

	// The anchor is the oldest snapshot that remains after deletion. A
	// snapshot is a deletion candidate only when its successor is itself older
	// than the cutoff, because the successor then covers every time point the
	// candidate served. The newest snapshot is always retained.
	candidates, anchor := classifySnapshots(snapshots, cutoff)
	if anchor == nil {
		return 0, nil
	}
	anchorTXID := anchor.MaxTXID

	headTXID := anchor.MaxTXID
	plan, err := BuildRetentionPlan(snapshots, candidates, enforcedAt, cutoff, anchorTXID, headTXID)
	if err != nil {
		return 0, err
	}

	executor := c.newRetentionExecutor(enforcedAt)
	report, err := executor.Execute(ctx, plan)
	if err != nil {
		return 0, fmt.Errorf("remove snapshot ltx files: %w", err)
	}
	LogProtected(c.logger, report)
	return anchorTXID, nil
}

// newRetentionExecutor wires the compactor's local-delete and existence-check
// hooks into a retention executor.
func (c *Compactor) newRetentionExecutor(enforcedAt time.Time) *RetentionExecutor {
	e := NewRetentionExecutor(c.client, c.logger, c.RetentionEnabled)
	e.EnforcedAt = enforcedAt
	e.LocalFileDeleter = c.LocalFileDeleter
	e.FileExistenceChecker = c.RetentionExistenceChecker
	return e
}

// EnforceRetentionByTXID deletes files at the given level with MaxTXID below
// the target. Before deleting, it builds a cross-level plan so that any file
// bridging a gap in the restore chain is protected even when its TXID is below
// the target — the TXID floor alone cannot see dependencies that live in other
// level directories. Always keeps at least one file per level and never breaks
// the chain up to the observed head.
func (c *Compactor) EnforceRetentionByTXID(ctx context.Context, level int, txID ltx.TXID) error {
	if !c.retentionMu.TryLock() {
		c.logger.Debug("retention enforcement already in progress, skipping")
		return ErrRetentionInProgress
	}
	defer c.retentionMu.Unlock()

	c.logger.Debug("enforcing retention", "level", level, "txid", txID)
	enforcedAt := time.Now()

	allFiles, err := collectAllLTXFiles(ctx, c.client, SnapshotLevel-1)
	if err != nil {
		return err
	}

	// Head is the highest MaxTXID anywhere; the retained set must keep
	// contiguous coverage up to it.
	var headTXID ltx.TXID
	for _, info := range allFiles {
		if info.MaxTXID > headTXID {
			headTXID = info.MaxTXID
		}
	}

	// Anchor on the oldest retained snapshot when one exists; coverage then
	// starts at its MaxTXID rather than TXID 1.
	var anchorTXID ltx.TXID
	for _, info := range allFiles {
		if info.Level == SnapshotLevel {
			if anchorTXID == 0 || info.MaxTXID < anchorTXID {
				anchorTXID = info.MaxTXID
			}
		}
	}

	// Candidates are this level's files below the floor. The recovery planner
	// decides which of them are actually safe to remove; it protects any file
	// still required to keep the chain contiguous up to the head, including the
	// newest file when no higher-level coverage substitutes for it.
	var candidates []*ltx.FileInfo
	for _, info := range allFiles {
		if info.Level == level && info.MaxTXID < txID {
			candidates = append(candidates, info)
		}
	}

	plan, err := BuildRetentionPlan(allFiles, candidates, enforcedAt, time.Time{}, anchorTXID, headTXID)
	if err != nil {
		return err
	}

	// TXID-floor cascade cleanup runs against already-replicated files; do
	// not gate it on the local-cache existence checker, whose absence is
	// expected for pure remote clients and for cached files already removed.
	executor := c.newRetentionExecutor(enforcedAt)
	executor.FileExistenceChecker = nil
	report, err := executor.Execute(ctx, plan)
	if err != nil {
		return fmt.Errorf("remove ltx files: %w", err)
	}
	LogProtected(c.logger, report)
	return nil
}

// EnforceL0Retention retains L0 files based on L1 compaction progress and time.
// Files are only eligible for deletion when they have been compacted into L1
// AND are older than retention. Eligibility is then passed through the
// cross-level recovery planner, which protects any L0 file still needed to
// bridge a restore-chain gap (e.g. when the covering L1 file is itself missing
// or incomplete). This guarantees contiguous coverage rather than assuming the
// L1 floor alone is sufficient.
func (c *Compactor) EnforceL0Retention(ctx context.Context, retention time.Duration) error {
	if retention <= 0 {
		return nil
	}
	if !c.retentionMu.TryLock() {
		c.logger.Debug("retention enforcement already in progress, skipping")
		return ErrRetentionInProgress
	}
	defer c.retentionMu.Unlock()

	enforcedAt := time.Now()
	c.logger.Debug("enforcing l0 retention", "retention", retention)

	itr, err := c.client.LTXFiles(ctx, 1, 0, false)
	if err != nil {
		return fmt.Errorf("fetch l1 files: %w", err)
	}
	var maxL1TXID ltx.TXID
	for itr.Next() {
		info := itr.Item()
		if info.MaxTXID > maxL1TXID {
			maxL1TXID = info.MaxTXID
		}
	}
	if err := itr.Close(); err != nil {
		return fmt.Errorf("close l1 iterator: %w", err)
	}
	if maxL1TXID == 0 {
		return nil
	}

	allFiles, err := collectAllLTXFiles(ctx, c.client, SnapshotLevel-1)
	if err != nil {
		return err
	}

	var headTXID ltx.TXID
	var anchorTXID ltx.TXID
	for _, info := range allFiles {
		if info.MaxTXID > headTXID {
			headTXID = info.MaxTXID
		}
		if info.Level == SnapshotLevel && (anchorTXID == 0 || info.MaxTXID < anchorTXID) {
			anchorTXID = info.MaxTXID
		}
	}

	threshold := enforcedAt.Add(-retention)
	var candidates []*ltx.FileInfo
	var newestL0 *ltx.FileInfo
	for _, info := range allFiles {
		if info.Level != 0 {
			continue
		}
		if newestL0 == nil || info.MaxTXID > newestL0.MaxTXID {
			newestL0 = info
		}
		createdAt := info.CreatedAt
		if createdAt.IsZero() {
			createdAt = threshold
		}
		// Files newer than the threshold or not yet compacted are not
		// candidates; the planner additionally enforces the in-flight guard.
		if createdAt.After(threshold) {
			continue
		}
		if info.MaxTXID <= maxL1TXID {
			candidates = append(candidates, info)
		}
	}
	// Never delete the newest L0 file; VFS readers rely on contiguous coverage.
	if newestL0 != nil {
		filtered := candidates[:0]
		for _, info := range candidates {
			if info != newestL0 {
				filtered = append(filtered, info)
			}
		}
		candidates = filtered
	}

	plan, err := BuildRetentionPlan(allFiles, candidates, enforcedAt, threshold, anchorTXID, headTXID)
	if err != nil {
		return err
	}

	executor := c.newRetentionExecutor(enforcedAt)
	report, err := executor.Execute(ctx, plan)
	if err != nil {
		return fmt.Errorf("remove expired l0 files: %w", err)
	}
	LogProtected(c.logger, report)
	if len(report.Deleted) > 0 {
		c.logger.Info("l0 retention enforced",
			"deleted_count", len(report.Deleted),
			"protected_count", len(report.Protected),
			"max_l1_txid", maxL1TXID)
	}

	return nil
}
