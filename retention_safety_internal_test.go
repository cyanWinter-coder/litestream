package litestream

import (
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func fi(level int, lo, hi uint64) *ltx.FileInfo {
	return &ltx.FileInfo{Level: level, MinTXID: ltx.TXID(lo), MaxTXID: ltx.TXID(hi), CreatedAt: time.Now()}
}

func snap(hi uint64, age time.Duration) *ltx.FileInfo {
	return &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: ltx.TXID(hi), CreatedAt: time.Now().Add(-age)}
}

func TestClassifySnapshots(t *testing.T) {
	now := time.Now()
	cutoff := now.Add(-time.Hour)

	t.Run("all candidates except newest", func(t *testing.T) {
		// Successors all older than cutoff: every non-newest snapshot is a
		// candidate; the second snapshot becomes the anchor.
		s := []*ltx.FileInfo{snap(5, 4*time.Hour), snap(10, 2*time.Hour), snap(15, 30*time.Minute)}
		// Make -30m actually outside cutoff for this case by shifting: use a
		// tighter window instead.
		c := now.Add(-10 * time.Minute)
		cands, anchor := classifySnapshots(s, c)
		if len(cands) != 2 {
			t.Fatalf("candidates=%d, want 2", len(cands))
		}
		if anchor.MaxTXID != 10 {
			t.Errorf("anchor=%d, want 10", anchor.MaxTXID)
		}
	})

	t.Run("first in-window snapshot is anchor", func(t *testing.T) {
		s := []*ltx.FileInfo{
			snap(5, 4*time.Hour),
			snap(10, 2*time.Hour),
			snap(15, 30*time.Minute),
			snap(20, 5*time.Minute),
		}
		cands, anchor := classifySnapshots(s, cutoff)
		// Only max5's successor (max10, -2h) is outside cutoff -> max5 candidate.
		// max10's successor (max15, -30m) is inside -> anchor max10.
		if len(cands) != 1 || cands[0].MaxTXID != 5 {
			t.Fatalf("candidates=%v, want [5]", cands)
		}
		if anchor.MaxTXID != 10 {
			t.Errorf("anchor=%d, want 10", anchor.MaxTXID)
		}
	})

	t.Run("no candidates when successor in window", func(t *testing.T) {
		s := []*ltx.FileInfo{
			snap(5, 2*time.Hour),
			snap(10, 30*time.Minute),
			snap(15, 5*time.Minute),
		}
		cands, anchor := classifySnapshots(s, cutoff)
		if len(cands) != 0 {
			t.Fatalf("candidates=%d, want 0", len(cands))
		}
		// No snapshot is deleted, so the oldest retained snapshot (max5) is the
		// conservative floor for lower-level cascade cleanup.
		if anchor.MaxTXID != 5 {
			t.Errorf("anchor=%d, want 5", anchor.MaxTXID)
		}
	})
}

func TestBuildRetentionPlan_NoSnapshotStartsAtTXIDOne(t *testing.T) {
	// Without a snapshot, coverage must begin at TXID 1; the first L1 file is
	// therefore protected even though it sits below the TXID floor.
	l1a := fi(1, 1, 2)
	l1b := fi(1, 3, 5)
	l1c := fi(1, 6, 10)
	all := []*ltx.FileInfo{l1a, l1b, l1c}
	plan, err := BuildRetentionPlan(all, []*ltx.FileInfo{l1a}, time.Now().Add(time.Hour), time.Time{}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Deleted) != 0 || len(plan.Protected) != 1 {
		t.Errorf("deleted=%d protected=%d, want 0/1", len(plan.Deleted), len(plan.Protected))
	}
}

func TestBuildRetentionPlan_CrossLevelGapProtection(t *testing.T) {
	// L1 covers 1-3 but the next L1 starts at 6; the L0 files covering 4-5 are
	// the only bridge and must be protected although their MaxTXID is low.
	l1a := fi(1, 1, 3)
	l0a := fi(0, 4, 4)
	l0b := fi(0, 5, 5)
	l1b := fi(1, 6, 10)
	all := []*ltx.FileInfo{l1a, l0a, l0b, l1b}
	plan, err := BuildRetentionPlan(all, []*ltx.FileInfo{l0a, l0b}, time.Now().Add(time.Hour), time.Time{}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Deleted) != 0 {
		t.Errorf("deleted=%d, want 0 (L0 bridges the L1 gap)", len(plan.Deleted))
	}
	if len(plan.Protected) != 2 {
		t.Errorf("protected=%d, want 2", len(plan.Protected))
	}
}

func TestBuildRetentionPlan_RedundantL0UnderContiguousL1Deleted(t *testing.T) {
	// L1 fully covers 1-5 contiguously, so the L0 files in that range are
	// redundant and deletable even across level directories.
	l1a := fi(1, 1, 3)
	l1b := fi(1, 4, 5)
	l0a := fi(0, 1, 1)
	l0b := fi(0, 2, 3)
	l0c := fi(0, 6, 6) // tail, must stay
	all := []*ltx.FileInfo{l1a, l1b, l0a, l0b, l0c}
	plan, err := BuildRetentionPlan(all, []*ltx.FileInfo{l0a, l0b, l0c}, time.Now().Add(time.Hour), time.Time{}, 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Deleted) != 2 {
		t.Errorf("deleted=%d, want 2 redundant L0; protected=%d", len(plan.Deleted), len(plan.Protected))
	}
}

func TestBuildRetentionPlan_UnsafeReturnsErrorAndDeletesNothing(t *testing.T) {
	// A real hole that no candidate can bridge must abort, not delete.
	a := fi(1, 1, 2)
	b := fi(1, 6, 10)
	all := []*ltx.FileInfo{a, b}
	_, err := BuildRetentionPlan(all, []*ltx.FileInfo{a}, time.Now().Add(time.Hour), time.Time{}, 0, 10)
	if err == nil {
		t.Fatal("expected unsafe retention error")
	}
}

func TestBuildRetentionPlan_SnapshotCoversRedundantLowerLevel(t *testing.T) {
	snap := fi(SnapshotLevel, 1, 5)
	l1a := fi(1, 1, 2)
	l1b := fi(1, 3, 5)
	l1c := fi(1, 6, 10)
	all := []*ltx.FileInfo{snap, l1a, l1b, l1c}
	candidates := []*ltx.FileInfo{l1a, l1b}

	plan, err := BuildRetentionPlan(all, candidates, time.Now().Add(time.Hour), time.Time{}, 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Deleted) != 2 {
		t.Errorf("deleted=%d, want 2; protected=%d", len(plan.Deleted), len(plan.Protected))
	}
}

func TestBuildRetentionPlan_ProtectsGapBridger(t *testing.T) {
	// No snapshot. Deleting [1,2] opens a hole because the next file starts at 3.
	l1a := fi(1, 1, 2)
	l1b := fi(1, 3, 5)
	l1c := fi(1, 6, 10)
	all := []*ltx.FileInfo{l1a, l1b, l1c}
	candidates := []*ltx.FileInfo{l1a}

	plan, err := BuildRetentionPlan(all, candidates, time.Now().Add(time.Hour), time.Time{}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Deleted) != 0 {
		t.Errorf("deleted=%d, want 0", len(plan.Deleted))
	}
	if len(plan.Protected) != 1 || plan.Protected[0] != l1a {
		t.Errorf("protected=%v, want [1-2]", plan.Protected)
	}
}

func TestBuildRetentionPlan_InFlightNotDeleted(t *testing.T) {
	enforcedAt := time.Now()
	old := fi(1, 1, 2)
	old.CreatedAt = enforcedAt.Add(-time.Hour)
	fresh := fi(1, 3, 5)
	fresh.CreatedAt = enforcedAt.Add(time.Minute)
	head := fi(1, 6, 10)
	all := []*ltx.FileInfo{old, fresh, head}
	candidates := []*ltx.FileInfo{old, fresh}

	plan, err := BuildRetentionPlan(all, candidates, enforcedAt, time.Time{}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.InFlight) != 1 || plan.InFlight[0] != fresh {
		t.Errorf("inflight=%v, want [3-5]", plan.InFlight)
	}
	// old cannot be deleted because fresh is in-flight and must stay, but old
	// itself bridges coverage from TXID 1, so it is protected, not deleted.
	if len(plan.Deleted) != 0 {
		t.Errorf("deleted=%d, want 0", len(plan.Deleted))
	}
}
