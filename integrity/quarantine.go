package integrity

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Quarantine preserves content that failed verification. Files are copied (not
// hard-linked) so later in-place truncation or cleanup cannot alter preserved
// bytes. Every preservation uses a new O_EXCL name; existing entries are never
// overwritten.
type Quarantine struct {
	Dir string

	// Now is exposed for tests that need deterministic names.
	Now func() time.Time
}

// QuarantineMetadata is written next to each preserved blob as JSON.
type QuarantineMetadata struct {
	PreservedAt time.Time `json:"preserved_at"`
	SourcePath  string    `json:"source_path,omitempty"`
	BlobPath    string    `json:"blob_path"`
	Error       *Error    `json:"error,omitempty"`
}

// NewQuarantine creates a quarantine rooted at dir.
func NewQuarantine(dir string) *Quarantine {
	return &Quarantine{Dir: dir, Now: time.Now}
}

// DefaultQuarantineDir returns the standard quarantine directory under a
// replica metadata root.
func DefaultQuarantineDir(metadataDir string) string {
	return filepath.Join(metadataDir, "quarantine")
}

func (q *Quarantine) now() time.Time {
	if q == nil || q.Now == nil {
		return time.Now().UTC()
	}
	return q.Now().UTC()
}

// PreserveFile copies path into quarantine and returns the preserved blob path.
// The source file is never removed or modified.
func (q *Quarantine) PreserveFile(path string, verificationErr *Error) (string, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open corrupt file for quarantine: %w", err)
	}
	defer in.Close()

	blobPath, metaPath, err := q.newPaths(verificationErr)
	if err != nil {
		return "", err
	}
	out, err := openExclusiveReadOnly(blobPath)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return "", fmt.Errorf("copy corrupt file to quarantine: %w", err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	if err := os.Chmod(blobPath, 0o444); err != nil {
		return "", err
	}
	if err := q.writeMetadata(metaPath, path, blobPath, verificationErr); err != nil {
		return blobPath, err
	}
	return blobPath, nil
}

// QuarantineFile is an alias for PreserveFile.
func (q *Quarantine) QuarantineFile(path string, verificationErr *Error) (string, error) {
	return q.PreserveFile(path, verificationErr)
}

// PreserveReader streams r into a new unique quarantine blob. This is used for
// remote objects after a failed verification.
func (q *Quarantine) PreserveReader(r io.Reader, verificationErr *Error) (string, error) {
	blobPath, metaPath, err := q.newPaths(verificationErr)
	if err != nil {
		return "", err
	}
	out, err := openExclusiveReadOnly(blobPath)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, r); err != nil {
		_ = out.Close()
		return "", fmt.Errorf("preserve corrupt stream: %w", err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	if err := os.Chmod(blobPath, 0o444); err != nil {
		return "", err
	}
	if err := q.writeMetadata(metaPath, "", blobPath, verificationErr); err != nil {
		return blobPath, err
	}
	return blobPath, nil
}

func (q *Quarantine) newPaths(e *Error) (blob, metadata string, err error) {
	if q == nil || q.Dir == "" {
		return "", "", errors.New("integrity: quarantine directory is required")
	}
	if err := os.MkdirAll(q.Dir, 0o755); err != nil {
		return "", "", err
	}
	chunk := -1
	offset := int64(-1)
	fileID := "file"
	if e != nil {
		chunk = e.ChunkIndex
		offset = e.Offset
		if e.FileID != "" {
			fileID = e.FileID
		} else if e.ID != "" {
			fileID = e.ID
		}
	}
	var randBytes [4]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		return "", "", err
	}
	base := fmt.Sprintf("%s-c%d-o%d-%s-%s",
		q.now().Format("20060102T150405.000000000Z"),
		chunk, offset, safeToken(fileID, 48), hex.EncodeToString(randBytes[:]))
	blob = filepath.Join(q.Dir, base+".corrupt")
	metadata = filepath.Join(q.Dir, base+".json")
	return blob, metadata, nil
}

func openExclusiveReadOnly(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
}

func (q *Quarantine) writeMetadata(metaPath, sourcePath, blobPath string, e *Error) error {
	meta := QuarantineMetadata{
		PreservedAt: q.now(),
		SourcePath:  sourcePath,
		BlobPath:    blobPath,
		Error:       e,
	}
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(metaPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return err
	}
	return f.Sync()
}

func safeToken(s string, max int) string {
	b := make([]byte, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b = append(b, byte(r))
		default:
			b = append(b, '_')
		}
		if len(b) >= max {
			break
		}
	}
	if len(b) == 0 {
		return "file"
	}
	return string(b)
}
