package integrity_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream/integrity"
)

func sampleData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i/7)
	}
	return b
}

func TestGenerateAndVerifyRoundTrip(t *testing.T) {
	data := sampleData(5000)
	m, n, err := integrity.Generate("ltx:0:1-1", integrity.FileVersion{}, bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(data)) || m.Size != int64(len(data)) {
		t.Fatalf("sizes n=%d manifest=%d", n, m.Size)
	}
	if len(m.Chunks) != 5 {
		t.Fatalf("chunks=%d want 5", len(m.Chunks))
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := integrity.VerifyReader(bytes.NewReader(data), m, integrity.SideRemote); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyCorruptionReportsIdentityLocationAndSides(t *testing.T) {
	data := sampleData(5000)
	m, _, err := integrity.Generate("db/ltx/0/0000000000000001-0000000000000001.ltx", integrity.FileVersion{Token: "v1"}, bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatal(err)
	}

	corrupt := append([]byte(nil), data...)
	corrupt[2500] ^= 0xff // chunk 2, offset 2048-3071

	err = integrity.VerifyReader(bytes.NewReader(corrupt), m, integrity.SideRemote)
	var re *integrity.Error
	if !errors.As(err, &re) {
		t.Fatalf("expected *integrity.Error, got %T: %v", err, err)
	}
	if !re.Remote() || re.Local() {
		t.Fatalf("expected remote attribution: %+v", re)
	}
	if re.FileID != m.FileID || re.ChunkIndex != 2 || re.Offset != 2048 {
		t.Fatalf("bad location: id=%q chunk=%d offset=%d", re.FileID, re.ChunkIndex, re.Offset)
	}
	if re.Expected == re.Actual || re.Expected != m.Chunks[2].Digest {
		t.Fatalf("digests not reported: expected=%s actual=%s", re.Expected, re.Actual)
	}
	if !errors.Is(err, integrity.ErrMismatch) || !errors.Is(err, integrity.ErrChecksumMismatch) {
		t.Fatalf("sentinel mismatch not reachable: %v", err)
	}
	if locChunk, locOff := re.Location(); locChunk != 2 || locOff != 2048 {
		t.Fatalf("Location()=(%d,%d)", locChunk, locOff)
	}

	// The writer's manifest-derived conclusion must correlate exactly with the
	// reader's observed failure location and expected digest.
	writerConclusion := integrity.Conclusion{
		FileID:     m.FileID,
		Version:    m.CacheVersion(),
		ChunkIndex: 2,
		Offset:     m.Chunks[2].Offset,
		End:        m.Chunks[2].Offset + int64(m.Chunks[2].Size),
		Expected:   m.Chunks[2].Digest,
	}
	if !writerConclusion.Match(re.Conclusion()) {
		t.Fatalf("conclusions do not match:\nwriter=%+v\nreader=%+v", writerConclusion, re.Conclusion())
	}

	var valueTarget integrity.Error
	if !errors.As(err, &valueTarget) {
		t.Fatal("errors.As should support value Error target")
	}
	if valueTarget.ChunkIndex != 2 {
		t.Fatalf("value target chunk=%d", valueTarget.ChunkIndex)
	}

	localErr := integrity.NewMismatchErrorForTest(integrity.SideLocal, m, 2, "actual")
	if !localErr.Local() || localErr.Remote() {
		t.Fatalf("expected local attribution")
	}
	if !integrity.SameConclusion(localErr, re) {
		t.Fatalf("local and remote conclusions should correlate despite side difference")
	}
}

func TestVerifyTruncation(t *testing.T) {
	data := sampleData(4096)
	m, _, err := integrity.Generate("file", integrity.FileVersion{}, bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatal(err)
	}
	err = integrity.VerifyReader(bytes.NewReader(data[:3000]), m, integrity.SideRemote)
	var re *integrity.Error
	if !errors.As(err, &re) {
		t.Fatal(err)
	}
	if !errors.Is(err, integrity.ErrTruncated) {
		t.Fatalf("expected truncation, got %v", err)
	}
	if re.ChunkIndex != 2 || re.Offset != 3000 {
		t.Fatalf("bad truncation location chunk=%d offset=%d", re.ChunkIndex, re.Offset)
	}
}

func TestMissingManifestIsUnverifiedNotVerified(t *testing.T) {
	dir := t.TempDir()
	svc, err := integrity.NewService(integrity.ServiceConfig{MetadataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.LoadManifest(context.Background(), "old-replica-file", integrity.FileVersion{})
	if !errors.Is(err, integrity.ErrUnverified) && !errors.Is(err, integrity.ErrNoManifest) {
		t.Fatalf("expected unverified/no manifest, got %v", err)
	}
	if svc.Cache.AllVerified(&integrity.Manifest{FileID: "x"}) {
		t.Fatal("nil/empty manifest unexpectedly fully cached")
	}
}

func TestCacheHitSkipsExplicitFullVerification(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "replica.ltx")
	data := sampleData(4096)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	svc, err := integrity.NewService(integrity.ServiceConfig{MetadataDir: filepath.Join(dir, "meta"), ChunkSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	g := svc.Generator("file-id", integrity.FileVersion{Token: "v1"})
	if _, err := io.Copy(g, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	m, err := svc.CommitWrite(ctx, g)
	if err != nil {
		t.Fatal(err)
	}

	// First explicit verification has to read the file.
	_, cached, err := svc.VerifyFile(ctx, path, "file-id", integrity.FileVersion{}, integrity.SideRemote)
	if err != nil || cached {
		t.Fatalf("first verify cached=%v err=%v", cached, err)
	}

	// Corrupt the path after first verification to prove the cache-hit path
	// does not re-read it. This is the requested skip for repeated full checks;
	// normal restore reads still use streaming verification.
	if err := os.WriteFile(path, append(append([]byte(nil), data...), 'x'), 0o600); err != nil {
		t.Fatal(err)
	}
	gotM, cached, err := svc.VerifyFile(ctx, path, "file-id", integrity.FileVersion{}, integrity.SideRemote)
	if err != nil || !cached {
		t.Fatalf("second verify cached=%v err=%v", cached, err)
	}
	if gotM.FileID != m.FileID {
		t.Fatal("wrong manifest")
	}
}

func TestCacheIsBoundToVersionAndUpdateInvalidates(t *testing.T) {
	c := integrity.NewChunkCache("")
	m1, _, err := integrity.Generate("f", integrity.FileVersion{Token: "v1"}, bytes.NewReader(sampleData(2048)), 1024)
	if err != nil {
		t.Fatal(err)
	}
	c.MarkManifest(m1)
	if !c.AllVerified(m1) {
		t.Fatal("v1 should be fully cached")
	}

	m2, _, err := integrity.Generate("f", integrity.FileVersion{Token: "v2"}, bytes.NewReader(sampleData(2049)), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if c.AllVerified(m2) {
		t.Fatal("new file version must not be verified by old chunk records")
	}
	if len(c.MissingChunks(m2)) != len(m2.Chunks) {
		t.Fatal("all new-version chunks should miss")
	}
	if n := c.InvalidateVersion("f", m1.CacheVersion()); n == 0 {
		t.Fatal("expected old version records to be removable")
	}
}

func TestPersistedCacheReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	c := integrity.NewChunkCache(path)
	m, _, err := integrity.Generate("f", integrity.FileVersion{Token: "v1"}, bytes.NewReader(sampleData(1200)), 1024)
	if err != nil {
		t.Fatal(err)
	}
	c.MarkManifest(m)
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	c2, err := integrity.LoadChunkCache(path)
	if err != nil {
		t.Fatal(err)
	}
	if !c2.AllVerified(m) {
		t.Fatal("persisted cache did not reload exact records")
	}
}

func TestQuarantinePreservesFailedFileAndNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "bad.ltx")
	bad := []byte("corrupt contents")
	if err := os.WriteFile(source, bad, 0o640); err != nil {
		t.Fatal(err)
	}
	q := integrity.NewQuarantine(filepath.Join(dir, "quarantine"))
	verr := &integrity.Error{FileID: "f", ChunkIndex: 3, Offset: 3072, Side: integrity.SideRemote, Err: integrity.ErrMismatch}
	p1, err := q.PreserveFile(source, verr)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := q.PreserveFile(source, verr)
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Fatal("quarantine preservation overwrote/reused a path")
	}
	for _, p := range []string{p1, p2} {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, bad) {
			t.Fatalf("preserved bytes mismatch: %q", got)
		}
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode()&0o222 != 0 {
			t.Fatalf("quarantined file should be read-only: %v", info.Mode())
		}
	}
	if matches, _ := filepath.Glob(filepath.Join(q.Dir, "*.json")); len(matches) != 2 {
		t.Fatalf("metadata files=%d want 2", len(matches))
	}
}

type countingReader struct {
	r     io.Reader
	reads int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.reads += int64(n)
	return n, err
}

func TestWriteGenerationDoesNotRereadDestination(t *testing.T) {
	data := sampleData(12345)
	src := &countingReader{r: bytes.NewReader(data)}
	var dst bytes.Buffer
	g := integrity.NewGenerator("f", integrity.FileVersion{Token: "v1"}, 4096)
	w := io.MultiWriter(&dst, g)
	n, err := io.Copy(w, src)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(data)) || src.reads != int64(len(data)) {
		t.Fatalf("n=%d source reads=%d", n, src.reads)
	}
	if dst.Len() != len(data) {
		t.Fatalf("destination length %d", dst.Len())
	}
	// The assertion is structural: no file handle or destination reader was
	// passed to the generator, so completing the manifest cannot re-read it.
	m := g.Finish()
	if m.Size != int64(len(data)) || len(m.Chunks) != 4 {
		t.Fatalf("bad manifest size=%d chunks=%d", m.Size, len(m.Chunks))
	}
}

func TestValidatingReaderDoesNotReleaseCorruptChunk(t *testing.T) {
	data := sampleData(3000)
	m, _, err := integrity.Generate("f", integrity.FileVersion{}, bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), data...)
	corrupt[10] ^= 1
	r, err := integrity.NewValidatingReader(bytes.NewReader(corrupt), m, integrity.SideRemote)
	if err != nil {
		t.Fatal(err)
	}
	out, readErr := io.ReadAll(r)
	var ve *integrity.Error
	if !errors.As(readErr, &ve) {
		t.Fatalf("expected integrity error, got %v", readErr)
	}
	if ve.ChunkIndex != 0 {
		t.Fatalf("chunk=%d", ve.ChunkIndex)
	}
	// The first chunk must not be released once its digest fails.
	if len(out) != 0 {
		t.Fatalf("corrupt chunk released %d bytes", len(out))
	}
}

func TestJSONManifestStoreMissingAndVersionMismatch(t *testing.T) {
	ctx := context.Background()
	store := integrity.NewJSONManifestStore(t.TempDir())
	data := sampleData(1200)
	m, _, err := integrity.Generate("id/with/slashes", integrity.FileVersion{Token: "v1"}, bytes.NewReader(data), 512)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadManifest(ctx, "id/with/slashes", integrity.FileVersion{Token: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != m.Digest {
		t.Fatal("digest mismatch after JSON round trip")
	}
	_, err = store.LoadManifest(ctx, "id/with/slashes", integrity.FileVersion{Token: "v2"})
	if !errors.Is(err, integrity.ErrVersionMismatch) {
		t.Fatalf("expected version mismatch, got %v", err)
	}
	_, err = store.LoadManifest(ctx, "missing", integrity.FileVersion{})
	if !errors.Is(err, integrity.ErrNoManifest) {
		t.Fatalf("expected no manifest, got %v", err)
	}
}

func TestManifestAcceptsCompatibilityFieldNames(t *testing.T) {
	data := []byte("abcdefgh")
	h0 := sha256.Sum256(data[:4])
	h1 := sha256.Sum256(data[4:])
	m := &integrity.Manifest{
		ID:        "compat",
		BlockSize: 4,
		FileSize:  int64(len(data)),
		SHA256:    "root-alias",
		Digests: []integrity.ChunkDigest{
			{SHA256: hex.EncodeToString(h0[:])},
			{Length: 4, Hash: hex.EncodeToString(h1[:])},
		},
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if m.FileID != "compat" || m.ChunkSize != 4 || m.Digest != "root-alias" {
		t.Fatalf("manifest aliases not normalized: %+v", m)
	}
	if err := integrity.VerifyReader(bytes.NewReader(data), m, integrity.SideRemote); err != nil {
		t.Fatal(err)
	}
}

func TestErrorMessageContainsIdentityAndOffset(t *testing.T) {
	e := &integrity.Error{FileID: "abc", ChunkIndex: 9, Offset: 900, Side: integrity.SideRemote, Err: integrity.ErrMismatch}
	msg := e.Error()
	for _, want := range []string{"side=remote", `file_id="abc"`, "chunk=9", "offset=900"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q does not contain %q", msg, want)
		}
	}
	if !strings.Contains(fmt.Sprint(e.Location()), "9") {
		t.Fatal("location missing")
	}
}
