package litestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/superfly/ltx"
)

// ErrRetentionInProgress is returned when a retention enforcement run cannot
// acquire the per-target lock because another run is still deleting files.
// Callers should treat this as a benign skip and retry on the next interval
// rather than as a failure.
var ErrRetentionInProgress = errors.New("retention enforcement already in progress")

// RetentionReport records the outcome of a single retention enforcement run.
// Every field is populated before any deletion happens, so the report also
// serves as the audit trail explaining why each file was kept or removed.
type RetentionReport struct {
	// Cutoff is the retention boundary: files created at or after it are
	// considered inside the retention window.
	Cutoff time.Time

	// EnforcedAt is the instant the run started. Files created at or after
	// it are treated as in-flight (produced by a concurrent backup) and are
	// never deleted, even if the listing observed them.
	EnforcedAt time.Time

	// HeadTXID is the highest MaxTXID observed across every level.
	HeadTXID ltx.TXID

	// AnchorTXID is the MaxTXID of the oldest snapshot that must remain to
	// anchor point-in-time recovery. Zero when no snapshot exists.
	AnchorTXID ltx.TXID

	// Deleted is the final, deletion-ordered list of files that are safe to
	// remove because every retained time point still has a complete,
	// gap-free restore chain without them.
	Deleted []*ltx.FileInfo

	// Protected lists files that were deletion candidates but must be kept
	// because they bridge a gap in the cross-level restore chain.
	Protected []*ltx.FileInfo

	// InFlight lists files skipped because they appeared at or after
	// EnforcedAt, i.e. a concurrent backup may still be writing them.
	InFlight []*ltx.FileInfo

	// Skipped lists files that were candidates but could not be verified
	// immediately before deletion (e.g. they already disappeared). They are
	// left for the next run.
	Skipped []*ltx.FileInfo
}

// DeletedCount returns the number of files actually removed.
func (r *RetentionReport) DeletedCount() int { return len(r.Deleted) }

// collectAllLTXFiles lists every LTX file across snapshot and compaction
// levels. Restore chains span level directories (snapshot -> Ln -> ... -> L0),
// so retention must reason about all levels together rather than judging each
// directory in isolation.
func collectAllLTXFiles(ctx context.Context, client ReplicaClient, maxLevel int) ([]*ltx.FileInfo, error) {
	var all []*ltx.FileInfo
	for level := SnapshotLevel; level >= 0; level-- {
		if level != SnapshotLevel && level > maxLevel {
			continue
		}
		itr, err := client.LTXFiles(ctx, level, 0, false)
		if err != nil {
			return nil, fmt.Errorf("fetch level %d ltx files: %w", level, err)
		}
		for itr.Next() {
			all = append(all, itr.Item())
		}
		if err := itr.Close(); err != nil {
			return nil, fmt.Errorf("close level %d iterator: %w", level, err)
		}
	}
	return all, nil
}

// classifySnapshots applies the recoverability rule to an age-ordered
// (oldest-first) snapshot listing.
//
// A snapshot is a deletion candidate only when its younger successor was
// itself created before the cutoff: the successor then remains available as
// the recovery anchor for every time point the candidate used to serve. The
// newest snapshot is always retained. The returned anchor is the oldest
// snapshot that survives and therefore the lower bound of recoverable time.
func classifySnapshots(snapshots []*ltx.FileInfo, cutoff time.Time) (candidates []*ltx.FileInfo, anchor *ltx.FileInfo) {
	if len(snapshots) == 0 {
		return nil, nil
	}

	// Find the first snapshot that is NOT a candidate. A snapshot is a
	// candidate only when its successor is already older than the cutoff.
	// Everything before that index is a candidate; the first non-candidate is
	// the oldest surviving snapshot and therefore the recovery anchor.
	firstKept := len(snapshots) - 1 // default: only the newest survives
	for i := 0; i < len(snapshots)-1; i++ {
		successor := snapshots[i+1]
		if successor.CreatedAt.Before(cutoff) {
			candidates = append(candidates, snapshots[i])
			continue
		}
		// snapshots[i]'s successor remains in the window, so snapshots[i] is
		// retained and is the oldest surviving snapshot.
		firstKept = i
		break
	}
	// When every non-newest snapshot is a candidate, the oldest surviving
	// snapshot is snapshots[1] (the first candidate's successor). With a
	// single snapshot there are no candidates and the sole snapshot is kept.
	if len(candidates) == len(snapshots)-1 && len(snapshots) > 1 {
		firstKept = 1
	}
	return candidates, snapshots[firstKept]
}

// BuildRetentionPlan decides which candidate files may be deleted without
// breaking the restore chain of any retained time point.
//
// allFiles is the complete cross-level listing (snapshot plus every
// compaction level). candidates is the subset the caller considers expired by
// its own policy (age, TXID floor, ...). The function simulates deletion and,
// whenever removing a candidate would open a TXID gap, protects the
// gap-bridging files instead. Dependencies are derived from the actual
// MinTXID/MaxTXID ranges across level directories, never from timestamps
// embedded in filenames.
//
// enforcedAt protects files produced while this run is in progress, and
// cutoff/anchor are recorded for audit purposes.
func BuildRetentionPlan(allFiles, candidates []*ltx.FileInfo, enforcedAt time.Time, cutoff time.Time, anchorTXID, headTXID ltx.TXID) (*RetentionReport, error) {
	report := &RetentionReport{
		Cutoff:     cutoff,
		EnforcedAt: enforcedAt,
		HeadTXID:   headTXID,
		AnchorTXID: anchorTXID,
	}

	candidateSet := make(map[*ltx.FileInfo]struct{}, len(candidates))

	// Files created while this enforcement run is in progress belong to a
	// concurrent backup. They are never candidates, regardless of what the
	// initial listing observed.
	var planned []*ltx.FileInfo
	for _, info := range candidates {
		if !info.CreatedAt.IsZero() && !info.CreatedAt.Before(enforcedAt) {
			report.InFlight = append(report.InFlight, info)
			continue
		}
		candidateSet[info] = struct{}{}
		planned = append(planned, info)
	}

	// Everything not up for deletion forms the retained set.
	protected := make(map[*ltx.FileInfo]struct{}, len(allFiles))
	for _, info := range allFiles {
		if _, ok := candidateSet[info]; ok {
			continue
		}
		protected[info] = struct{}{}
	}

	// Sort the entire cross-level set by MinTXID, then by broader coverage
	// (larger MaxTXID) first so overlapping files at the same start position
	// are evaluated from the most useful one.
	ordered := slices.Clone(allFiles)
	slices.SortFunc(ordered, func(a, b *ltx.FileInfo) int {
		if a.MinTXID != b.MinTXID {
			if a.MinTXID < b.MinTXID {
				return -1
			}
			return 1
		}
		if a.MaxTXID != b.MaxTXID {
			if a.MaxTXID > b.MaxTXID {
				return -1
			}
			return 1
		}
		if a.Level != b.Level {
			if a.Level > b.Level {
				return -1
			}
			return 1
		}
		return 0
	})

	// Greedy chain walk. coverage is the highest contiguous TXID reachable so
	// far. Each pass first extends coverage through every retained file that
	// attaches, then, if a gap still blocks the head, protects the single
	// deletable candidate that bridges it furthest. The loop converges because
	// every iteration protects at least one file and the set is finite. This
	// mirrors how CalcRestorePlan stitches snapshots and LTX files across level
	// directories.
	coverage := anchorTXID
	for {
		// Pass 1: extend coverage through all currently retained files.
		progressed := true
		for progressed {
			progressed = false
			for _, info := range ordered {
				if _, ok := protected[info]; !ok {
					continue
				}
				if info.MinTXID <= coverage+1 && info.MaxTXID > coverage {
					coverage = info.MaxTXID
					progressed = true
				}
			}
		}

		if coverage >= headTXID {
			break
		}

		// Pass 2: find the deletable candidate that attaches to the covered
		// prefix and extends it the furthest.
		var bridge *ltx.FileInfo
		for _, info := range ordered {
			if _, ok := protected[info]; ok {
				continue
			}
			if info.MinTXID > coverage+1 {
				break // ordered by MinTXID; nothing later can attach
			}
			if info.MaxTXID > coverage && (bridge == nil || info.MaxTXID > bridge.MaxTXID) {
				bridge = info
			}
		}

		if bridge == nil {
			break // no candidate can close the remaining gap
		}
		protected[bridge] = struct{}{}
		delete(candidateSet, bridge)
		report.Protected = append(report.Protected, bridge)
	}

	// Final invariant: the retained set must cover the full chain up to the
	// head. Refuse to delete anything if it does not.
	if coverage < headTXID {
		return nil, fmt.Errorf("retention unsafe: retained files cover only up to %s but head is %s; no files deleted",
			coverage, headTXID)
	}

	// Preserve the caller's candidate ordering for predictable, readable
	// audit logs (typically oldest-first).
	for _, info := range planned {
		if _, ok := candidateSet[info]; ok {
			report.Deleted = append(report.Deleted, info)
		}
	}
	slices.SortFunc(report.Protected, func(a, b *ltx.FileInfo) int {
		if a.MinTXID != b.MinTXID {
			if a.MinTXID < b.MinTXID {
				return -1
			}
			return 1
		}
		return 0
	})
	return report, nil
}

// RetentionExecutor applies a retention plan against a replica client while
// keeping the remaining files recoverable on every failure path.
type RetentionExecutor struct {
	client  ReplicaClient
	logger  *slog.Logger
	enabled bool

	// EnforcedAt skips files created while the run is in progress even if
	// they slipped into the plan.
	EnforcedAt time.Time

	// LocalFileDeleter optionally removes the local cached copy after the
	// remote file has been deleted. A failure here is logged, not fatal: the
	// local cache is disposable and re-downloaded on demand.
	LocalFileDeleter func(level int, minTXID, maxTXID ltx.TXID) error

	// FileExistenceChecker optionally verifies that a file is still present
	// and unchanged immediately before deletion. Returning false skips the
	// file until the next run, covering backends where a concurrent writer
	// could be touching the object.
	FileExistenceChecker func(info *ltx.FileInfo) bool
}

// NewRetentionExecutor creates an executor. When enabled is false, remote
// deletion is skipped (cloud lifecycle policy owns cleanup) but local cache
// cleanup and audit logging still run.
func NewRetentionExecutor(client ReplicaClient, logger *slog.Logger, enabled bool) *RetentionExecutor {
	if logger == nil {
		logger = slog.Default()
	}
	return &RetentionExecutor{client: client, logger: logger, enabled: enabled, EnforcedAt: time.Now()}
}

// Execute deletes the planned files one at a time. On the first remote
// deletion failure it stops and returns the error, leaving every not-yet-
// deleted file in place; because the plan was pre-validated, the surviving
// set always remains fully recoverable. Nothing is partially "committed":
// deletion is purely subtractive and each step remains safe on its own.
func (e *RetentionExecutor) Execute(ctx context.Context, plan *RetentionReport) (*RetentionReport, error) {
	final := &RetentionReport{
		Cutoff:     plan.Cutoff,
		EnforcedAt: plan.EnforcedAt,
		HeadTXID:   plan.HeadTXID,
		AnchorTXID: plan.AnchorTXID,
		Protected:  slices.Clone(plan.Protected),
		InFlight:   slices.Clone(plan.InFlight),
	}

	for _, info := range plan.Deleted {
		// Re-check the in-flight guard at deletion time. A backup that
		// finished uploading between planning and deletion must not lose a
		// file merely because this process had already observed it.
		if !info.CreatedAt.IsZero() && !info.CreatedAt.Before(e.EnforcedAt) {
			e.logger.Info("retention: skipping file created during enforcement",
				"level", info.Level,
				"minTXID", info.MinTXID.String(),
				"maxTXID", info.MaxTXID.String(),
				"createdAt", info.CreatedAt.Format(time.RFC3339Nano))
			final.InFlight = append(final.InFlight, info)
			continue
		}

		if e.FileExistenceChecker != nil && !e.FileExistenceChecker(info) {
			e.logger.Info("retention: skipping file no longer present or changed",
				"level", info.Level,
				"minTXID", info.MinTXID.String(),
				"maxTXID", info.MaxTXID.String())
			final.Skipped = append(final.Skipped, info)
			continue
		}

		if e.enabled {
			// Delete one object per call so an error identifies the exact
			// file and never strands a batch in an unknown state.
			if err := e.client.DeleteLTXFiles(ctx, []*ltx.FileInfo{info}); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return final, err
				}
				return final, fmt.Errorf("delete ltx file level=%d %s-%s: %w",
					info.Level, info.MinTXID, info.MaxTXID, err)
			}
		} else {
			e.logger.Debug("retention: remote deletion disabled, keeping remote file",
				"level", info.Level,
				"minTXID", info.MinTXID.String(),
				"maxTXID", info.MaxTXID.String())
		}

		e.logger.Info("retention: deleted file",
			"level", info.Level,
			"minTXID", info.MinTXID.String(),
			"maxTXID", info.MaxTXID.String(),
			"createdAt", info.CreatedAt.Format(time.RFC3339Nano),
			"anchorTXID", plan.AnchorTXID.String(),
			"headTXID", plan.HeadTXID.String(),
			"retainedFrom", plan.Cutoff.Format(time.RFC3339Nano))
		final.Deleted = append(final.Deleted, info)

		if e.LocalFileDeleter != nil {
			if err := e.LocalFileDeleter(info.Level, info.MinTXID, info.MaxTXID); err != nil {
				// Local cache loss cannot break remote recoverability.
				e.logger.Error("retention: failed to remove local ltx file",
					"level", info.Level,
					"minTXID", info.MinTXID.String(),
					"maxTXID", info.MaxTXID.String(),
					"error", err)
			}
		}
	}

	e.logger.Info("retention run complete",
		"deleted", len(final.Deleted),
		"protected", len(final.Protected),
		"in_flight", len(final.InFlight),
		"skipped", len(final.Skipped),
		"anchorTXID", plan.AnchorTXID.String(),
		"headTXID", plan.HeadTXID.String())
	return final, nil
}

// LogProtected emits one audit entry per retained candidate so operators can
// explain afterwards why a time point (and the files anchoring it) survived.
func LogProtected(logger *slog.Logger, report *RetentionReport) {
	for _, info := range report.Protected {
		logger.Info("retention: retaining file to preserve restore chain",
			"level", info.Level,
			"minTXID", info.MinTXID.String(),
			"maxTXID", info.MaxTXID.String(),
			"reason", "bridges restore chain gap",
			"anchorTXID", report.AnchorTXID.String(),
			"headTXID", report.HeadTXID.String())
	}
	for _, info := range report.InFlight {
		logger.Info("retention: retaining file produced during enforcement",
			"level", info.Level,
			"minTXID", info.MinTXID.String(),
			"maxTXID", info.MaxTXID.String(),
			"reason", "in-flight backup")
	}
}
