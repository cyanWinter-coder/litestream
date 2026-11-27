package litestream_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/integrity"
)

type memoryLTXClient struct {
	litestream.ReplicaClient
	data map[string][]byte
}

func (c *memoryLTXClient) key(level int, min, max ltx.TXID) string {
	return litestream.LTXContentFileID(level, min, max)
}

func (c *memoryLTXClient) WriteLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if c.data == nil {
		c.data = make(map[string][]byte)
	}
	c.data[c.key(level, minTXID, maxTXID)] = b
	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(b))}, nil
}

func (c *memoryLTXClient) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	b := c.data[c.key(level, minTXID, maxTXID)]
	r := bytes.NewReader(b)
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	if size > 0 {
		return io.NopCloser(io.LimitReader(r, size)), nil
	}
	return io.NopCloser(r), nil
}

func TestValidatingReplicaClientWriteReadConclusionMatches(t *testing.T) {
	ctx := context.Background()
	inner := &memoryLTXClient{}
	client := litestream.NewValidatingReplicaClient(inner, integrity.NewMemoryManifestStore(), integrity.NewChunkCache(""), 128)

	data := bytes.Repeat([]byte("litestream-content-integrity-"), 32)
	if _, err := client.WriteLTXFile(ctx, 0, 10, 10, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// Writer-side manifest gives the expected conclusion.
	fileID := litestream.LTXContentFileID(0, 10, 10)
	manifest, err := client.Manifests.LoadManifest(ctx, fileID, integrity.FileVersion{})
	if err != nil {
		t.Fatal(err)
	}
	writerFailure := integrity.NewMismatchError(integrity.SideLocal, integrity.OpWrite, manifest, 1, "actual-writer")

	corrupt := append([]byte(nil), data...)
	corrupt[150] ^= 0xff
	inner.data[fileID] = corrupt

	rc, err := client.OpenLTXFile(ctx, 0, 10, 10, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(rc)
	_ = rc.Close()

	var readFailure *integrity.Error
	if !errors.As(err, &readFailure) {
		t.Fatalf("expected integrity error, got %v", err)
	}
	if !readFailure.Remote() {
		t.Fatalf("read failure should be remote: %v", readFailure)
	}
	if readFailure.ChunkIndex != 1 || readFailure.Offset != 128 {
		t.Fatalf("location=(%d,%d), want chunk 1 offset 128", readFailure.ChunkIndex, readFailure.Offset)
	}
	if !integrity.SameConclusion(writerFailure, readFailure) {
		t.Fatalf("conclusions differ:\nwriter=%+v\nreader=%+v", writerFailure.Conclusion(), readFailure.Conclusion())
	}
	if !errors.Is(err, litestream.ErrChecksumMismatch) {
		t.Fatalf("historical sentinel should wrap: %v", err)
	}
}

func TestValidatingReplicaClientMissingManifestUnverifiedWhenStrict(t *testing.T) {
	ctx := context.Background()
	inner := &memoryLTXClient{data: map[string][]byte{
		litestream.LTXContentFileID(0, 1, 1): []byte("legacy"),
	}}
	client := litestream.NewValidatingReplicaClient(inner, litestream.WithRequiredIntegrityManifests(true))
	_, err := client.OpenLTXFile(ctx, 0, 1, 1, 0, 0)
	if !errors.Is(err, integrity.ErrUnverified) {
		t.Fatalf("expected unverified error, got %v", err)
	}
}
