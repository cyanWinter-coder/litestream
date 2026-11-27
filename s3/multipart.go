package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

// errPartTimeout reports a part request that exceeded its own deadline. It is
// distinct from caller cancellation: the object-level retry may try again, and
// parts confirmed so far are reused.
type partTimeoutError struct{ err error }

func (e *partTimeoutError) Error() string   { return "s3: part upload timed out: " + e.err.Error() }
func (e *partTimeoutError) Unwrap() error   { return e.err }
func (e *partTimeoutError) Retryable() bool { return true }

// errUploadExpired reports that the provider no longer has the multipart
// upload named by local state (it expired or was aborted out-of-band). The
// uploader restarts with a fresh upload ID instead of failing permanently.
var errUploadExpired = errors.New("s3: multipart upload no longer exists")

// ErrUploadSourceChanged reports that a resumed upload found the source bytes
// for an already-confirmed part no longer match. The partial multipart upload
// is aborted rather than completed, so a stale part can never end up in the
// published object.
var ErrUploadSourceChanged = errors.New("s3: upload source changed during retry")

// multipartPartRetryPolicy bounds retries of a single part request. Full
// jitter is applied between attempts so instances that hit the same provider
// outage do not retry in lockstep.
var multipartPartRetryPolicy = internal.RetryPolicy{
	MaxAttempts: 3,
	BaseDelay:   200 * time.Millisecond,
	MaxDelay:    5 * time.Second,
	MaxElapsed:  20 * time.Second,
}

// partSource yields the bytes of a multipart upload part-by-part.
// Implementations must return exactly the bytes for a part.
type partSource interface {
	// TotalSize returns the source size, or -1 when unknown.
	TotalSize() int64
	// ReadPart reads the one-based partNumber into p and returns its size.
	// Unknown-size sources are read sequentially, so part numbers arrive in
	// order.
	ReadPart(partNumber int, p []byte) (int, error)
}

// seekPartSource reads parts from a random-access reader. Parts can be read in
// any order and concurrently when the source is a true ReaderAt.
type seekPartSource struct {
	r             io.ReaderAt
	start         int64
	size          int64
	partSize      int64
	allowParallel bool
}

func (s *seekPartSource) TotalSize() int64 { return s.size }

func (s *seekPartSource) parallelParts() bool { return s.allowParallel }

func (s *seekPartSource) ReadPart(partNumber int, p []byte) (int, error) {
	offset := int64(partNumber-1) * s.partSize
	n := min(s.partSize, s.size-offset)
	if n <= 0 {
		return 0, fmt.Errorf("part %d outside source (offset=%d size=%d)", partNumber, offset, s.size)
	}
	if int64(len(p)) < n {
		return 0, fmt.Errorf("part buffer too small: have %d, need %d", len(p), n)
	}
	if _, err := io.ReadFull(io.NewSectionReader(s.r, s.start+offset, n), p[:n]); err != nil {
		return 0, err
	}
	return int(n), nil
}

// streamPartSource reads parts sequentially from a non-seekable stream. Parts
// cannot be skipped ahead: a resumed part must still be read and discarded to
// keep the stream aligned with its part number.
type streamPartSource struct {
	r        io.Reader
	partSize int64
	next     int // one-based next part to read
}

func (s *streamPartSource) TotalSize() int64 { return -1 }

func (s *streamPartSource) ReadPart(partNumber int, p []byte) (int, error) {
	if partNumber != s.next {
		return 0, fmt.Errorf("part %d requested but stream is at part %d", partNumber, s.next)
	}
	if int64(len(p)) < s.partSize {
		return 0, fmt.Errorf("part buffer too small: have %d, need %d", len(p), s.partSize)
	}
	n, err := io.ReadFull(s.r, p[:s.partSize])
	s.next++
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		// The final part is allowed to be short.
		return n, nil
	}
	return n, err
}

// defaultMultipartRetryPolicy bounds whole-object attempts. Part requests have
// their own smaller budget; this layer retries the object after that budget is
// spent, reusing every part the provider already confirmed.
var defaultMultipartRetryPolicy = internal.RetryPolicy{
	MaxAttempts: 4,
	BaseDelay:   500 * time.Millisecond,
	MaxDelay:    10 * time.Second,
	// Bound the whole upload attempt, not just one part, so a provider
	// outage cannot hold the replica monitor (and any SyncAndWait caller)
	// indefinitely. The database keeps accepting writes meanwhile because
	// uploads run on the replica monitor, not the WAL capture path.
	MaxElapsed: 60 * time.Second,
}

// multipartRetryPolicy returns the configured whole-object policy.
func (c *ReplicaClient) multipartRetryPolicy() internal.RetryPolicy {
	if c.MultipartRetryPolicy.MaxAttempts > 0 {
		return c.MultipartRetryPolicy
	}
	return defaultMultipartRetryPolicy
}

// partRetryPolicy returns the configured single-part policy.
func (c *ReplicaClient) partRetryPolicy() internal.RetryPolicy {
	if c.PartRetryPolicy.MaxAttempts > 0 {
		return c.PartRetryPolicy
	}
	return multipartPartRetryPolicy
}

// uploadMultipart uploads source to key using an S3 multipart upload that
// resumes after failures. Already-confirmed parts are reused; the upload is
// only completed after every part has been confirmed.
func (c *ReplicaClient) uploadMultipart(
	ctx context.Context,
	level int,
	minTXID, maxTXID ltx.TXID,
	key string,
	timestamp time.Time,
	src partSource,
	partSize int64,
) (*string, error) {
	// The caller holds the per-object lock (WriteLTXFile), so a retried upload
	// of this object cannot overlap a new sync writing the same path.
	totalSize := src.TotalSize()
	progress := litestream.UploadProgress{
		Key:            key,
		Level:          level,
		MinTXID:        minTXID,
		MaxTXID:        maxTXID,
		TotalBytes:     totalSize,
		TotalParts:     partCount(totalSize, partSize),
		RemainingParts: -1,
		Attempt:        1,
	}
	if totalSize >= 0 {
		progress.RemainingParts = progress.TotalParts
	}
	c.setUploadProgress(progress)

	state, alreadyDone, err := c.prepareMultipartState(ctx, key, timestamp, partSize, totalSize, &progress)
	if err != nil {
		return nil, c.finishMultipart(err, progress)
	} else if alreadyDone != nil {
		// A previous attempt completed the object but never saw the response.
		// The provider has no multipart upload left to resume, yet the object
		// is durable at the expected size. Report success instead of
		// re-uploading it under a new upload ID.
		if err := c.multipartStateStore().Remove(key); err != nil {
			c.logger.Debug("remove multipart state", "key", key, "error", err)
		}
		if totalSize >= 0 {
			progress.UploadedBytes = totalSize
			progress.CompletedParts = progress.TotalParts
			progress.RemainingParts = 0
		}
		c.setUploadProgress(progress)
		c.logger.Info("multipart upload already completed", "key", key)
		return alreadyDone, nil
	}

	// sourceChanged aborts the upload and returns a permanent error: the bytes
	// no longer match the parts already stored, so completing would publish a
	// corrupt object.
	sourceChanged := func() error {
		c.abortMultipartUpload(ctx, key, state.UploadID)
		_ = c.multipartStateStore().Remove(key)
		return internal.Permanent(fmt.Errorf("%w: %s", ErrUploadSourceChanged, key))
	}

	// Retry the whole object after part-level retries are exhausted. Each
	// attempt reuses every part the provider confirmed, so the budget is spent
	// on missing bytes rather than re-sending the object from the start.
	//
	// An unknown-size source is a one-shot stream: it cannot be rewound, so
	// retrying the object would read EOF and report a misleading failure.
	policy := c.multipartRetryPolicy()
	if totalSize < 0 {
		policy.MaxAttempts = 1
	}
	var etag *string
	err = internal.Retry(ctx, policy, func(attempt internal.RetryAttempt) {
		progress.Attempt = attempt.Attempt
		c.setUploadProgress(progress)
		internal.UploadRetryCounterVec.WithLabelValues(ReplicaClientType, "retry").Inc()
		c.logger.Warn("retrying ltx upload",
			"key", key,
			"attempt", attempt.Attempt,
			"delay", attempt.Delay,
			"uploaded_bytes", progress.UploadedBytes,
			"reused_bytes", progress.ReusedBytes,
			"remaining_parts", progress.RemainingParts,
			"error", attempt.Err)
	}, func(ctx context.Context) error {
		// Parts confirmed by a previous run are re-read from the local source
		// only to verify their checksum; their bytes are not re-sent.
		err := c.uploadMultipartParts(ctx, key, state, src, partSize, &progress, sourceChanged)
		if errors.Is(err, errUploadExpired) {
			// Local state named an upload the provider no longer has. If the
			// object already landed, this attempt is complete; otherwise start
			// a fresh multipart upload rather than failing permanently.
			if totalSize >= 0 {
				if doneETag, ok := c.objectMatchesSize(ctx, key, totalSize); ok {
					etag = doneETag
					return nil
				}
			}
			c.abortMultipartUpload(ctx, key, state.UploadID)
			_ = c.multipartStateStore().Remove(key)
			newState, doneETag, prepErr := c.prepareMultipartState(ctx, key, timestamp, partSize, totalSize, &progress)
			if prepErr != nil {
				return prepErr
			} else if doneETag != nil {
				// A concurrent writer completed the object while this attempt
				// was restarting. Report success; the bytes are durable.
				etag = doneETag
				return nil
			}
			state = newState
			err = c.uploadMultipartParts(ctx, key, state, src, partSize, &progress, sourceChanged)
		}
		if err != nil {
			return err
		}

		// Complete only after every part is confirmed. Writing any "uploaded"
		// marker before this point would let the next process believe an
		// object exists when it does not.
		etag, err = c.completeMultipartUpload(ctx, key, state, totalSize, &progress)
		return err
	})
	if err != nil {
		c.setUploadProgress(progress)
		return nil, c.finishMultipart(err, progress)
	}

	// Drop the resume record only after the object is durable.
	if err := c.multipartStateStore().Remove(key); err != nil {
		c.logger.Debug("remove multipart state", "key", key, "error", err)
	}

	if totalSize >= 0 {
		progress.UploadedBytes = totalSize
		progress.CompletedParts = progress.TotalParts
		progress.RemainingParts = 0
	}
	progress.Resumed = progress.ReusedBytes > 0
	c.setUploadProgress(progress)

	if progress.Resumed {
		internal.UploadRetryCounterVec.WithLabelValues(ReplicaClientType, "resumed").Inc()
		internal.UploadReusedBytesCounterVec.WithLabelValues(ReplicaClientType).Add(float64(progress.ReusedBytes))
		c.logger.Info("resumed multipart upload",
			"key", key,
			"attempt", progress.Attempt,
			"uploaded_bytes", progress.UploadedBytes,
			"reused_bytes", progress.ReusedBytes,
			"remaining_parts", progress.RemainingParts)
	}

	return etag, nil
}

// prepareMultipartState loads resumable state for key or creates a new
// multipart upload. It reconciles persisted state against the provider so a
// part that no longer exists is re-sent instead of being assumed present.
//
// A non-nil etag means the object is already complete and no upload is
// needed: the upload finished on a prior attempt but its completion response
// was lost.
func (c *ReplicaClient) prepareMultipartState(
	ctx context.Context,
	key string,
	timestamp time.Time,
	partSize, totalSize int64,
	progress *litestream.UploadProgress,
) (*internal.MultipartState, *string, error) {
	store := c.multipartStateStore()

	state, err := store.Load(key)
	if err != nil {
		// A local state read failure only loses the resume optimization; the
		// upload can still start from scratch correctly.
		c.logger.Debug("load multipart state", "key", key, "error", err)
		state = nil
	}

	if state != nil {
		geometryChanged := state.PartSize != partSize ||
			(totalSize >= 0 && state.TotalBytes > 0 && state.TotalBytes != totalSize)
		if !geometryChanged {
			resumable, completedETag := c.reconcileMultipartParts(ctx, key, state, totalSize)
			if completedETag != nil {
				// The upload no longer exists and the object is durable at the
				// expected size. This is the completion marker's job: never
				// treat it as an incomplete upload.
				return nil, completedETag, nil
			}
			if resumable {
				state.Attempts++
				progress.Attempt = state.Attempts
				progress.Resumed = true
				if err := store.Save(state.Clone()); err != nil {
					c.logger.Debug("save multipart state", "key", key, "error", err)
				}
				return state, nil, nil
			}
		}
		// Stale or unusable: abort the remote upload so its parts stop
		// accruing storage charges, then start over.
		c.abortMultipartUpload(ctx, key, state.UploadID)
		_ = store.Remove(key)
		state = nil
	}

	var uploadID string
	err = internal.Retry(ctx, c.multipartRetryPolicy(), func(attempt internal.RetryAttempt) {
		c.logger.Warn("retrying create multipart upload",
			"key", key, "attempt", attempt.Attempt, "delay", attempt.Delay, "error", attempt.Err)
	}, func(ctx context.Context) error {
		var err error
		uploadID, err = c.createMultipartUpload(ctx, key, timestamp, totalSize)
		if err != nil && !internal.IsRetryable(err) {
			return internal.Permanent(err)
		}
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("s3: create multipart upload %s: %w", key, err)
	}
	state = &internal.MultipartState{
		Key:        key,
		UploadID:   uploadID,
		PartSize:   partSize,
		TotalBytes: totalSize,
		Attempts:   1,
	}
	progress.Attempt = 1
	// Persist before the first part so a crash cannot leak an upload ID that
	// no future process knows how to resume or abort.
	if err := store.Save(state.Clone()); err != nil {
		c.logger.Debug("save multipart state", "key", key, "error", err)
	}

	progress.Resumed = false
	progress.ReusedBytes = 0
	progress.CompletedParts = 0
	return state, nil, nil
}

// reconcileMultipartParts drops locally-recorded parts that the provider no
// longer has, or whose size changed. It reports whether any part remains
// resumable.
//
// Listing failures fall back to the local record: several S3-compatible
// providers do not implement ListParts. Uploading a part number twice is safe
// because S3 replaces the previous part with the same number.
func (c *ReplicaClient) reconcileMultipartParts(ctx context.Context, key string, state *internal.MultipartState, totalSize int64) (resumable bool, completedETag *string) {
	parts, err := c.listMultipartParts(ctx, key, state.UploadID)
	if err != nil {
		if isNoSuchUpload(err) {
			// The upload expired or was already completed. Distinguish the
			// two by checking the object: only a durable object at the
			// expected size proves the upload finished.
			if totalSize >= 0 {
				if etag, ok := c.objectMatchesSize(ctx, key, totalSize); ok {
					return false, etag
				}
			}
			return false, nil
		}
		c.logger.Debug("list multipart parts", "key", key, "error", err)
		return len(state.Parts) > 0, nil
	}
	if parts == nil {
		// ListParts unsupported: trust local state.
		return len(state.Parts) > 0, nil
	}

	confirmed := make(map[int]internal.MultipartPart, len(parts))
	for _, p := range parts {
		number := int(aws.ToInt32(p.PartNumber))
		confirmed[number] = internal.MultipartPart{
			Number: number,
			Size:   aws.ToInt64(p.Size),
			ETag:   aws.ToString(p.ETag),
		}
	}

	// Keep only parts the provider still reports at the recorded size, and
	// refresh their ETag. A part the provider no longer has is re-uploaded.
	// Parts beyond the known part count are stale from a different source
	// length; completing with them would publish extra bytes.
	maxPart := 0
	if totalSize >= 0 {
		maxPart = partCount(totalSize, state.PartSize)
	}
	kept := make([]internal.MultipartPart, 0, len(state.Parts))
	for _, part := range state.Parts {
		if maxPart > 0 && part.Number > maxPart {
			continue
		}
		remote, ok := confirmed[part.Number]
		if !ok || remote.Size != part.Size {
			continue
		}
		if remote.ETag != "" {
			part.ETag = remote.ETag
		}
		kept = append(kept, part)
	}
	state.Parts = kept
	return len(state.Parts) > 0, nil
}

// uploadMultipartParts sends every unconfirmed part.
func (c *ReplicaClient) uploadMultipartParts(
	ctx context.Context,
	key string,
	state *internal.MultipartState,
	src partSource,
	partSize int64,
	progress *litestream.UploadProgress,
	sourceChanged func() error,
) error {
	store := c.multipartStateStore()

	// stateMu guards state while parts are confirmed concurrently.
	var stateMu sync.Mutex
	update := newProgressTracker(progress, &stateMu, state, c)

	// sendPart reads, verifies, and uploads one part. A part confirmed by a
	// previous run is re-read and checksummed so stale bytes can never be
	// combined into the published object.
	sendPart := func(partNumber int) error {
		stateMu.Lock()
		prev, reused := state.FindPart(partNumber)
		stateMu.Unlock()

		buf := make([]byte, partSize)
		n, err := src.ReadPart(partNumber, buf)
		if err != nil {
			return internal.Permanent(fmt.Errorf("read ltx part %d: %w", partNumber, err))
		}
		buf = buf[:n]

		sum := md5.Sum(buf)
		md5hex := hex.EncodeToString(sum[:])

		if reused {
			// Reuse is safe only if the resumed bytes are identical to the
			// ones already stored. A mismatch means the source changed, so
			// the whole upload must be rebuilt.
			if prev.MD5 == "" || prev.MD5 != md5hex {
				return &uploadSourceChangedError{key: key, part: partNumber}
			}
			return nil
		}

		etag, err := c.uploadPart(ctx, key, state.UploadID, partNumber, buf, progress)
		if err != nil {
			return err
		}

		// Record only after the provider confirms the part, so a failed
		// request can never leave a part number marked as complete.
		stateMu.Lock()
		state.SetPart(internal.MultipartPart{Number: partNumber, Size: int64(n), MD5: md5hex, ETag: etag})
		// Save while holding the lock so concurrent confirmations cannot
		// persist an older snapshot over a newer one.
		if err := store.Save(state.Clone()); err != nil {
			c.logger.Debug("save multipart state", "key", key, "error", err)
		}
		stateMu.Unlock()
		update.snapshot()
		return nil
	}

	// Known-size sources split into a fixed part list and upload concurrently.
	// Unknown-size sources are read once, sequentially, because they cannot be
	// rewound.
	if totalSize := src.TotalSize(); totalSize >= 0 {
		concurrency := c.Concurrency
		if concurrency <= 0 {
			concurrency = 5
		}
		if p, ok := src.(interface{ parallelParts() bool }); ok && !p.parallelParts() {
			concurrency = 1
		}
		totalParts := partCount(totalSize, partSize)

		// With a single worker, send parts in order. Besides matching user
		// expectations for a serial upload, it keeps the part sequence
		// deterministic for tests that inject failures at a given part.
		if concurrency == 1 {
			for n := 1; n <= totalParts; n++ {
				if err := sendPart(n); err != nil {
					return c.classifyPartError(err, sourceChanged)
				}
			}
			return nil
		}

		g, gctx := errgroup.WithContext(ctx)
		sem := semaphore.NewWeighted(int64(concurrency))
		for n := 1; n <= totalParts; n++ {
			n := n
			g.Go(func() error {
				if err := sem.Acquire(gctx, 1); err != nil {
					return err
				}
				defer sem.Release(1)
				return sendPart(n)
			})
		}
		if err := g.Wait(); err != nil {
			return c.classifyPartError(err, sourceChanged)
		}
		return nil
	}

	// Unknown-size sources are read sequentially. Bytes belonging to a part
	// confirmed by a previous run are still read from the stream so its offset
	// stays aligned, then verified against the stored checksum.
	for n := 1; ; n++ {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		if n > maxMultipartParts {
			return fmt.Errorf("s3: more than %d parts for %s", maxMultipartParts, key)
		}

		buf := make([]byte, partSize)
		nn, err := src.ReadPart(n, buf)
		if errors.Is(err, io.EOF) && nn == 0 {
			// EOF before a part a previous attempt confirmed means the source
			// shrank; completing now would publish a truncated object.
			stateMu.Lock()
			stale := false
			for _, part := range state.Parts {
				if part.Number >= n {
					stale = true
					break
				}
			}
			stateMu.Unlock()
			if stale {
				return sourceChanged()
			}
			return nil
		} else if err != nil || nn == 0 && !errors.Is(err, io.EOF) {
			return internal.Permanent(fmt.Errorf("read ltx part %d: %w", n, err))
		}
		buf = buf[:nn]

		sum := md5.Sum(buf)
		md5hex := hex.EncodeToString(sum[:])

		stateMu.Lock()
		prev, reused := state.FindPart(n)
		stateMu.Unlock()
		if reused {
			if prev.MD5 == "" || prev.MD5 != md5hex {
				return sourceChanged()
			}
		} else {
			etag, err := c.uploadPart(ctx, key, state.UploadID, n, buf, progress)
			if err != nil {
				return err
			}
			stateMu.Lock()
			state.SetPart(internal.MultipartPart{Number: n, Size: int64(nn), MD5: md5hex, ETag: etag})
			if err := store.Save(state.Clone()); err != nil {
				c.logger.Debug("save multipart state", "key", key, "error", err)
			}
			stateMu.Unlock()
			update.snapshot()
		}

		if int64(nn) < partSize {
			return nil // short part is the last
		}
	}
}

// maxMultipartParts is S3's hard limit on parts per upload.
const maxMultipartParts = 10000

// partUploadTimeout bounds one part request, including its retries. S3's
// client has no per-request deadline, so a provider that accepts a connection
// and never answers would otherwise hold the upload open until the process
// exits. It is generous enough for a 5 MiB part over a slow link.
func partUploadTimeout(c *ReplicaClient) time.Duration {
	timeout := 2 * time.Minute
	if c.PartUploadTimeout > 0 {
		timeout = c.PartUploadTimeout
	}
	return timeout
}

// uploadPart uploads one part with jittered retries.
func (c *ReplicaClient) uploadPart(ctx context.Context, key, uploadID string, partNumber int, body []byte, progress *litestream.UploadProgress) (string, error) {
	var etag string
	ctx, cancel := context.WithTimeout(ctx, partUploadTimeout(c))
	defer cancel()

	err := internal.Retry(ctx, c.partRetryPolicy(), func(attempt internal.RetryAttempt) {
		c.logger.Debug("retrying ltx part upload",
			"key", key, "part", partNumber, "attempt", attempt.Attempt,
			"delay", attempt.Delay,
			"uploaded_bytes", progress.UploadedBytes,
			"remaining_parts", progress.RemainingParts,
			"error", attempt.Err)
	}, func(ctx context.Context) error {
		input := &s3.UploadPartInput{
			Bucket:        aws.String(c.Bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(int32(partNumber)),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
		}
		c.applySSECUploadPart(input)
		out, err := c.s3.UploadPart(ctx, input)
		if err != nil {
			return err
		}
		etag = aws.ToString(out.ETag)
		if etag == "" {
			return internal.Permanent(fmt.Errorf("s3: upload part %d of %s: no ETag returned", partNumber, key))
		}
		return nil
	})
	if err != nil {
		if isNoSuchUpload(err) {
			return "", fmt.Errorf("%w: %v", errUploadExpired, err)
		}
		// A part-level deadline is ours, not the caller's. Reclassify it as
		// retryable so the object-level retry can run; caller cancellation
		// still surfaces through Retry's context check.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return "", &partTimeoutError{err: err}
		}
		return "", fmt.Errorf("s3: upload part %d of %s: %w", partNumber, key, err)
	}
	return etag, nil
}

// classifyPartError converts a source-changed sentinel into the aborting
// permanent error; everything else passes through.
func (c *ReplicaClient) classifyPartError(err error, sourceChanged func() error) error {
	var changed *uploadSourceChangedError
	if errors.As(err, &changed) {
		return sourceChanged()
	}
	// An expired upload is handled inside the attempt by recreating it, so it
	// never reaches the object retry layer from here. Keep the sentinel in the
	// chain for that check.
	return err
}

type uploadSourceChangedError struct {
	key  string
	part int
}

func (e *uploadSourceChangedError) Error() string {
	return fmt.Sprintf("s3: source changed for part %d of %s", e.part, e.key)
}

// progressTracker serializes progress updates from concurrent part uploads.
type progressTracker struct {
	mu       sync.Mutex
	progress *litestream.UploadProgress
	stateMu  *sync.Mutex
	state    *internal.MultipartState
	client   *ReplicaClient
}

func newProgressTracker(progress *litestream.UploadProgress, stateMu *sync.Mutex, state *internal.MultipartState, client *ReplicaClient) *progressTracker {
	t := &progressTracker{progress: progress, stateMu: stateMu, state: state, client: client}

	// Parts confirmed before this attempt are exactly the bytes not re-sent;
	// record them as reused so progress shows how much the retry saved.
	confirmed := state.CompletedBytes()
	t.progress.ReusedBytes = confirmed
	t.progress.UploadedBytes = confirmed
	t.progress.CompletedParts = len(state.Parts)
	if t.progress.TotalParts > 0 {
		t.progress.RemainingParts = max(t.progress.TotalParts-len(state.Parts), 0)
	} else {
		t.progress.RemainingParts = -1
	}
	t.progress.Resumed = confirmed > 0
	client.setUploadProgress(*t.progress)
	return t
}

// snapshot republishes progress from the shared part state. It is called after
// a part is confirmed or verified.
func (t *progressTracker) snapshot() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.stateMu.Lock()
	completed := len(t.state.Parts)
	uploaded := t.state.CompletedBytes()
	t.stateMu.Unlock()

	t.progress.CompletedParts = completed
	t.progress.UploadedBytes = uploaded
	if t.progress.TotalParts > 0 {
		t.progress.RemainingParts = max(t.progress.TotalParts-completed, 0)
	} else {
		t.progress.RemainingParts = -1
	}
	t.client.setUploadProgress(*t.progress)
}

// finishMultipart converts an upload failure into a diagnosable error. It
// reports retryability and the progress reached so the caller can decide
// whether the remaining bytes are worth another attempt.
func (c *ReplicaClient) finishMultipart(err error, progress litestream.UploadProgress) error {
	if err == nil {
		return nil
	}
	var uploadErr *litestream.UploadError
	if errors.As(err, &uploadErr) {
		return err
	}
	retryable := internal.IsRetryable(err) && !errors.Is(err, ErrUploadSourceChanged)
	if progress.RemainingParts < 0 && progress.TotalParts > 0 {
		progress.RemainingParts = max(progress.TotalParts-progress.CompletedParts, 0)
	}
	internal.UploadRetryCounterVec.WithLabelValues(ReplicaClientType, "failed").Inc()
	c.logger.Warn("upload failed",
		"key", progress.Key,
		"level", progress.Level,
		"attempt", progress.Attempt,
		"uploaded_bytes", progress.UploadedBytes,
		"reused_bytes", progress.ReusedBytes,
		"completed_parts", progress.CompletedParts,
		"remaining_parts", progress.RemainingParts,
		"retryable", retryable,
		"error", err)
	return &litestream.UploadError{
		Key:            progress.Key,
		Level:          progress.Level,
		MinTXID:        progress.MinTXID,
		MaxTXID:        progress.MaxTXID,
		Attempts:       max(progress.Attempt, 1),
		UploadedBytes:  progress.UploadedBytes,
		ReusedBytes:    progress.ReusedBytes,
		CompletedParts: progress.CompletedParts,
		RemainingParts: progress.RemainingParts,
		Retryable:      retryable,
		Err:            err,
	}
}

// createMultipartUpload starts a multipart upload with the same write
// parameters as a single PutObject so the two paths stay in parity.
func (c *ReplicaClient) createMultipartUpload(ctx context.Context, key string, timestamp time.Time, totalSize int64) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
		Metadata: map[string]string{
			MetadataKeyTimestamp: timestamp.Format(time.RFC3339Nano),
		},
	}
	if c.StorageClass != "" {
		input.StorageClass = types.StorageClass(c.StorageClass)
	}
	if c.SSECustomerKey != "" {
		input.SSECustomerAlgorithm = aws.String(c.SSECustomerAlgorithm)
		input.SSECustomerKey = aws.String(c.SSECustomerKey)
		input.SSECustomerKeyMD5 = aws.String(c.SSECustomerKeyMD5)
	}
	if c.SSEKMSKeyID != "" {
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		input.SSEKMSKeyId = aws.String(c.SSEKMSKeyID)
	}

	out, err := c.s3.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", err
	}
	if aws.ToString(out.UploadId) == "" {
		return "", fmt.Errorf("no UploadId returned")
	}
	return aws.ToString(out.UploadId), nil
}

// listMultipartParts returns all currently-stored parts of an upload. A nil,
// nil return means the provider does not implement ListParts.
func (c *ReplicaClient) listMultipartParts(ctx context.Context, key, uploadID string) ([]types.Part, error) {
	paginator := s3.NewListPartsPaginator(c.s3, &s3.ListPartsInput{
		Bucket:   aws.String(c.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	var parts []types.Part
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			if isListPartsUnsupported(err) {
				return nil, nil
			}
			return nil, err
		}
		parts = append(parts, page.Parts...)
	}
	return parts, nil
}

// completeMultipartUpload finalizes the object from the confirmed parts. A lost
// completion response is reconciled against HeadObject so a retry does not
// report failure for an object that is already durable.
func (c *ReplicaClient) completeMultipartUpload(
	ctx context.Context,
	key string,
	state *internal.MultipartState,
	totalSize int64,
	progress *litestream.UploadProgress,
) (*string, error) {
	if totalSize >= 0 {
		want := partCount(totalSize, state.PartSize)
		if len(state.Parts) != want {
			return nil, fmt.Errorf("s3: refusing to complete %s: %d of %d parts confirmed", key, len(state.Parts), want)
		}
	} else {
		// Without a known size the part count comes from the stream, so the
		// only safe completion is a gap-free sequence starting at 1.
		for i, part := range state.Parts {
			if part.Number != i+1 {
				return nil, fmt.Errorf("s3: refusing to complete %s: non-contiguous parts at %d", key, part.Number)
			}
		}
		if len(state.Parts) == 0 {
			return nil, fmt.Errorf("s3: refusing to complete %s: no parts confirmed", key)
		}
	}

	parts := make([]types.CompletedPart, 0, len(state.Parts))
	for _, part := range state.Parts {
		parts = append(parts, types.CompletedPart{
			PartNumber: aws.Int32(int32(part.Number)),
			ETag:       aws.String(part.ETag),
		})
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(c.Bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(state.UploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}
	c.applySSECComplete(input)

	var out *s3.CompleteMultipartUploadOutput
	err := internal.Retry(ctx, c.partRetryPolicy(), func(attempt internal.RetryAttempt) {
		c.logger.Debug("retrying complete multipart upload",
			"key", key, "attempt", attempt.Attempt, "delay", attempt.Delay, "error", attempt.Err)
	}, func(ctx context.Context) error {
		var err error
		out, err = c.s3.CompleteMultipartUpload(ctx, input)
		return err
	})
	if err == nil {
		return out.ETag, nil
	}

	// NoSuchUpload after a completion attempt means the upload already
	// finished (or expired). Accept it only when the object exists at the
	// expected size; otherwise the caller must rebuild the object.
	if isNoSuchUpload(err) || internal.IsRetryable(err) {
		if etag, ok := c.objectMatchesSize(ctx, key, totalSize); ok {
			c.logger.Debug("multipart upload already completed", "key", key)
			return etag, nil
		}
	}
	return nil, err
}

// objectMatchesSize reports whether key exists with the expected size.
func (c *ReplicaClient) objectMatchesSize(ctx context.Context, key string, size int64) (*string, bool) {
	if size < 0 {
		return nil, false
	}
	input := &s3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key)}
	c.applySSECHead(input)
	out, err := c.s3.HeadObject(ctx, input)
	if err != nil {
		return nil, false
	}
	if aws.ToInt64(out.ContentLength) != size {
		return nil, false
	}
	return out.ETag, true
}

// abortMultipartUpload releases provider storage for an upload that can no
// longer be completed. Aborting is best-effort: the failure that triggered it
// is the one the caller must see.
func (c *ReplicaClient) abortMultipartUpload(ctx context.Context, key, uploadID string) {
	if uploadID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_, err := c.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(c.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		c.logger.Debug("abort multipart upload", "key", key, "upload_id", uploadID, "error", err)
	}
}

func (c *ReplicaClient) applySSECUploadPart(v *s3.UploadPartInput) {
	if c.SSECustomerKey == "" {
		return
	}
	v.SSECustomerAlgorithm = aws.String(c.SSECustomerAlgorithm)
	v.SSECustomerKey = aws.String(c.SSECustomerKey)
	v.SSECustomerKeyMD5 = aws.String(c.SSECustomerKeyMD5)
}

func (c *ReplicaClient) applySSECHead(v *s3.HeadObjectInput) {
	if c.SSECustomerKey == "" {
		return
	}
	v.SSECustomerAlgorithm = aws.String(c.SSECustomerAlgorithm)
	v.SSECustomerKey = aws.String(c.SSECustomerKey)
	v.SSECustomerKeyMD5 = aws.String(c.SSECustomerKeyMD5)
}

func (c *ReplicaClient) applySSECComplete(v *s3.CompleteMultipartUploadInput) {
	if c.SSECustomerKey == "" {
		return
	}
	v.SSECustomerAlgorithm = aws.String(c.SSECustomerAlgorithm)
	v.SSECustomerKey = aws.String(c.SSECustomerKey)
	v.SSECustomerKeyMD5 = aws.String(c.SSECustomerKeyMD5)
}

func partCount(totalSize, partSize int64) int {
	if totalSize <= 0 || partSize <= 0 {
		return 0
	}
	return int((totalSize + partSize - 1) / partSize)
}

func isNoSuchUpload(err error) bool {
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "NoSuchUpload"
	}
	return false
}

// isListPartsUnsupported reports provider responses that mean ListParts itself
// is unavailable, as opposed to the upload being gone.
func isListPartsUnsupported(err error) bool {
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotImplemented", "MethodNotAllowed", "InvalidRequest":
			return true
		}
	}
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) {
		switch httpErr.HTTPStatusCode() {
		case 405, 501:
			return true
		}
	}
	return false
}

var _ litestream.UploadProgressReporter = (*ReplicaClient)(nil)
