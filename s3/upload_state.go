package s3

import (
	"fmt"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

// SetMultipartStateDir configures where resumable multipart state is persisted.
// The directory should be local to the process (typically the database meta
// directory). An empty value disables persistence; retries still reuse parts
// confirmed within a single process run.
func (c *ReplicaClient) SetMultipartStateDir(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.multipartDir = dir
	c.multipartStore = internal.NewMultipartStateStore(dir)
}

// multipartStateStore returns the store backing resumable uploads. It is
// created lazily so a zero-value client still works.
func (c *ReplicaClient) multipartStateStore() *internal.MultipartStateStore {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.multipartStore == nil {
		c.multipartStore = internal.NewMultipartStateStore(c.multipartDir)
	}
	return c.multipartStore
}

// setUploadProgress records the latest progress for an upload. Progress is
// keyed by LTX file, not object key, so callers that only know the TXID range
// can query it.
func (c *ReplicaClient) setUploadProgress(p litestream.UploadProgress) {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	if c.progress == nil {
		c.progress = make(map[string]litestream.UploadProgress)
	}
	c.progress[uploadProgressKey(p.Level, p.MinTXID, p.MaxTXID)] = p
}

// UploadProgress returns the latest progress of the upload for the given LTX
// file. It reports false if no upload has been attempted in this process.
func (c *ReplicaClient) UploadProgress(level int, minTXID, maxTXID ltx.TXID) (litestream.UploadProgress, bool) {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	p, ok := c.progress[uploadProgressKey(level, minTXID, maxTXID)]
	if !ok {
		// A retried upload can be resumed after a restart, before this process
		// has attempted it. Reconstruct progress from persisted part state so
		// operators still see how much is reused.
		if state, err := c.multipartStateStore().Load(multipartStateKey(c, level, minTXID, maxTXID)); err == nil && state != nil {
			p = litestream.UploadProgress{
				Key:            state.Key,
				Level:          level,
				MinTXID:        minTXID,
				MaxTXID:        maxTXID,
				TotalBytes:     state.TotalBytes,
				UploadedBytes:  state.CompletedBytes(),
				ReusedBytes:    state.CompletedBytes(),
				TotalParts:     state.TotalParts(),
				CompletedParts: len(state.Parts),
				RemainingParts: state.TotalParts() - len(state.Parts),
				Attempt:        max(state.Attempts, 1),
				Resumed:        len(state.Parts) > 0,
			}
			if p.TotalParts == 0 {
				p.RemainingParts = -1
			}
			return p, true
		}
	}
	return p, ok
}

func uploadProgressKey(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%04x/%s", level, ltx.FormatFilename(minTXID, maxTXID))
}

// multipartStateKey returns the object key used to persist state for an LTX
// file. It mirrors the key the write path uses.
func multipartStateKey(c *ReplicaClient, level int, minTXID, maxTXID ltx.TXID) string {
	return c.Path + "/" + fmt.Sprintf("%04x/%s", level, ltx.FormatFilename(minTXID, maxTXID))
}
