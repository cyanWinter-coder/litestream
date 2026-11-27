package file_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/integrity"
)

func integrityTestLTX(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   7,
		MaxTXID:   7,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, bytes.Repeat([]byte{0x5a}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestFileReplicaClient_ContentIntegrityRejectsAndQuarantines(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	client := file.NewReplicaClient(root)
	data := integrityTestLTX(t)

	if _, err := client.WriteLTXFile(ctx, 0, 7, 7, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	path := client.LTXFilePath(0, 7, 7)

	// Corrupt one byte without changing file size, modeling the reported
	// size-correct but damaged replica.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	rc, err := client.OpenLTXFile(ctx, 0, 7, 7, 0, 0)
	if err != nil {
		t.Fatalf("open should defer verification to read: %v", err)
	}
	_, readErr := io.ReadAll(rc)
	_ = rc.Close()

	var ie *integrity.Error
	if !errors.As(readErr, &ie) {
		t.Fatalf("expected integrity.Error, got %T: %v", readErr, readErr)
	}
	if !ie.Remote() {
		t.Fatalf("expected remote/replica attribution: %v", ie)
	}
	if ie.FileID == "" || ie.ChunkIndex < 0 || ie.Offset < 0 {
		t.Fatalf("error missing identity/location: %+v", ie)
	}
	if !errors.Is(readErr, litestream.ErrChecksumMismatch) {
		t.Fatalf("expected historical checksum sentinel, got %v", readErr)
	}

	quarantineDir := filepath.Join(root, ".integrity", "quarantine")
	blobs, err := filepath.Glob(filepath.Join(quarantineDir, "*.corrupt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 1 {
		t.Fatalf("quarantined blobs=%d want 1", len(blobs))
	}
	preserved, err := os.ReadFile(blobs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(preserved, raw) {
		t.Fatal("quarantined bytes do not match corrupt replica")
	}

	// Ordinary replica cleanup must not remove the marked live evidence or
	// its unique quarantine copy.
	info := &ltx.FileInfo{Level: 0, MinTXID: 7, MaxTXID: 7}
	if err := client.DeleteLTXFiles(ctx, []*ltx.FileInfo{info}); err != nil {
		t.Fatal(err)
	}
	// A second cleanup pass must keep the protection but not make another
	// full quarantine copy.
	if err := client.DeleteLTXFiles(ctx, []*ltx.FileInfo{info}); err != nil {
		t.Fatal(err)
	}
	if blobsAgain, err := filepath.Glob(filepath.Join(quarantineDir, "*.corrupt")); err != nil {
		t.Fatal(err)
	} else if len(blobsAgain) != 1 {
		t.Fatalf("quarantine copies after repeated cleanup=%d want 1", len(blobsAgain))
	}
	if preservedLive, err := os.ReadFile(path); err != nil {
		t.Fatalf("marked corrupt live file should remain after cleanup: %v", err)
	} else if !bytes.Equal(preservedLive, raw) {
		t.Fatal("live corrupt file changed during protected cleanup")
	}
	if _, err := os.Stat(blobs[0]); err != nil {
		t.Fatalf("quarantine must survive cleanup: %v", err)
	}
}

func TestFileReplicaClient_LegacyFileWithoutIntegrityIsUnverified(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	client := file.NewReplicaClient(root)
	data := integrityTestLTX(t)

	path := client.LTXFilePath(0, 8, 8)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	rc, err := client.OpenLTXFile(ctx, 0, 8, 8, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	statusRC, ok := rc.(interface {
		IntegrityStatus() litestream.IntegrityStatus
	})
	if !ok {
		t.Fatalf("reader does not expose integrity status: %T", rc)
	}
	if got := statusRC.IntegrityStatus(); got != litestream.IntegrityStatusUnverified {
		t.Fatalf("status=%q want unverified", got)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("legacy replica should remain readable but unverified: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("legacy bytes mismatch")
	}
}

func TestFileReplicaClient_SecondFullReadUsesRemoteCacheHit(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	client := file.NewReplicaClient(root)
	data := integrityTestLTX(t)
	if _, err := client.WriteLTXFile(ctx, 0, 9, 9, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	readAll := func() (int, error) {
		rc, err := client.OpenLTXFile(ctx, 0, 9, 9, 0, 0)
		if err != nil {
			return 0, err
		}
		defer rc.Close()
		n, err := io.Copy(io.Discard, rc)
		if err != nil {
			return int(n), err
		}
		hits := 0
		if h, ok := rc.(interface{ CacheHits() int }); ok {
			hits = h.CacheHits()
		}
		return hits, nil
	}

	if hits, err := readAll(); err != nil || hits != 0 {
		t.Fatalf("first read hits=%d err=%v", hits, err)
	}
	if hits, err := readAll(); err != nil || hits == 0 {
		t.Fatalf("second read hits=%d err=%v; want chunk cache hit", hits, err)
	}
}
