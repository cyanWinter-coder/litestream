package file_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/file"
)

func TestReplicaClient_ResumeLTXFileAppendsVerifiedPrefix(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	full := createLTXData(1, 8, bytes.Repeat([]byte("partial-upload-"), 32))

	if _, err := client.WriteLTXFile(ctx, 9, 1, 8, bytes.NewReader(full)); err != nil {
		t.Fatal(err)
	}
	path := client.LTXFilePath(9, 1, 8)

	prefixLen := int64(len(full) / 2)
	if err := os.Truncate(path, prefixLen); err != nil {
		t.Fatal(err)
	}

	info, err := client.ResumeLTXFile(ctx, 9, 1, 8, "upload-id", prefixLen, bytes.NewReader(full[prefixLen:]))
	if err != nil {
		t.Fatalf("ResumeLTXFile: %v", err)
	}
	if info.Size != int64(len(full)) {
		t.Fatalf("info size=%d, want %d", info.Size, len(full))
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, full) {
		t.Fatalf("resumed object length=%d, want %d", len(got), len(full))
	}
}

func TestReplicaClient_ResumeLTXFileRejectsOffsetMismatch(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	full := createLTXData(1, 9, []byte("data"))
	if _, err := client.WriteLTXFile(ctx, 0, 1, 9, bytes.NewReader(full)); err != nil {
		t.Fatal(err)
	}

	if _, err := client.ResumeLTXFile(ctx, 0, 1, ltx.TXID(9), "id", int64(len(full))-1, bytes.NewReader(nil)); err == nil {
		t.Fatal("expected offset mismatch error")
	}
}
