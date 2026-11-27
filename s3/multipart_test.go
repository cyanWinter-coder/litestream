package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

// multipartServer is a minimal S3 multipart endpoint that tracks parts and
// lets a test inject failures on selected requests.
type multipartServer struct {
	t   *testing.T
	srv *httptest.Server

	mu             sync.Mutex
	uploadID       string
	parts          map[int][]byte
	partETags      map[int]string
	partSends      map[int]int
	expiredUploads map[string]bool
	completed      bool
	completes      int
	aborts         int
	creates        int
	requests       map[string]int
	listParts      bool
	singlePuts     int
	singlePutBody  []byte
	completeFn     func(attempt int, w http.ResponseWriter) bool
	partFn         func(part int, attempt int, w http.ResponseWriter) bool
	createFn       func(w http.ResponseWriter) bool
}

func newMultipartServer(t *testing.T) *multipartServer {
	t.Helper()
	ms := &multipartServer{
		t:              t,
		uploadID:       "upload-1",
		parts:          make(map[int][]byte),
		partETags:      make(map[int]string),
		partSends:      make(map[int]int),
		expiredUploads: make(map[string]bool),
		requests:       make(map[string]int),
		listParts:      true,
	}
	ms.srv = httptest.NewServer(http.HandlerFunc(ms.handle))
	t.Cleanup(ms.srv.Close)
	return ms
}

func (ms *multipartServer) handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	q := r.URL.Query()

	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		ms.mu.Lock()
		ms.creates++
		ms.requests["create"]++
		attempt := ms.requests["create"]
		fn := ms.createFn
		ms.mu.Unlock()
		if fn != nil && !fn(w) {
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-key</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, ms.uploadID)
		_ = attempt
		return

	case r.Method == http.MethodPut && q.Get("partNumber") != "":
		ms.mu.Lock()
		expired := ms.expiredUploads[q.Get("uploadId")]
		ms.mu.Unlock()
		if expired {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>`)
			return
		}
		part, err := strconv.Atoi(q.Get("partNumber"))
		if err != nil {
			ms.t.Errorf("invalid part number: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			ms.t.Errorf("read part body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ms.mu.Lock()
		ms.requests["part"]++
		ms.partSends[part]++
		attempt := ms.requests["part"]
		fn := ms.partFn
		ms.mu.Unlock()
		if fn != nil && !fn(part, attempt, w) {
			return
		}
		ms.mu.Lock()
		ms.parts[part] = body
		etag := fmt.Sprintf(`"part-etag-%d"`, part)
		ms.partETags[part] = etag
		ms.mu.Unlock()
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		return

	case r.Method == http.MethodGet && q.Get("uploadId") != "":
		// ListParts.
		ms.mu.Lock()
		if ms.completed {
			ms.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>`)
			return
		}
		if !ms.listParts {
			ms.mu.Unlock()
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
		var partsXML strings.Builder
		for n, body := range ms.parts {
			fmt.Fprintf(&partsXML,
				`<Part><PartNumber>%d</PartNumber><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>%s</ETag><Size>%d</Size></Part>`,
				n, ms.partETags[n], len(body))
		}
		count := len(ms.parts)
		ms.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><ListPartsResult><Bucket>test-bucket</Bucket><Key>test-key</Key><UploadId>%s</UploadId><IsTruncated>false</IsTruncated><MaxParts>1000</MaxParts><PartNumberMarker>0</PartNumberMarker><NextPartNumberMarker>0</NextPartNumberMarker><PartCount>%d</PartCount>%s</ListPartsResult>`,
			ms.uploadID, count, partsXML.String())
		return

	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		ms.mu.Lock()
		ms.requests["complete"]++
		attempt := ms.requests["complete"]
		fn := ms.completeFn
		ms.mu.Unlock()
		if fn != nil && !fn(attempt, w) {
			return
		}
		// Validate the completion request names every confirmed part in
		// ascending order with no gaps.
		body, _ := io.ReadAll(r.Body)
		ms.mu.Lock()
		ms.completed = true
		ms.completes++
		partCount := len(ms.parts)
		ms.mu.Unlock()
		var seen []int
		rest := string(body)
		for {
			idx := strings.Index(rest, "<PartNumber>")
			if idx < 0 {
				break
			}
			rest = rest[idx+len("<PartNumber>"):]
			end := strings.Index(rest, "</PartNumber>")
			if end < 0 {
				break
			}
			n, err := strconv.Atoi(rest[:end])
			if err != nil {
				ms.t.Errorf("bad part number %q: %v", rest[:end], err)
				break
			}
			seen = append(seen, n)
			rest = rest[end:]
		}
		if len(seen) != partCount {
			ms.t.Errorf("CompleteMultipartUpload named %d parts, server has %d", len(seen), partCount)
		}
		for i, n := range seen {
			if n != i+1 {
				ms.t.Errorf("parts out of order or gapped at index %d: %v", i, seen)
				break
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Location>http://test-bucket.s3.amazonaws.com/test-key</Location><Bucket>test-bucket</Bucket><Key>test-key</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`)
		return

	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		ms.mu.Lock()
		ms.aborts++
		ms.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return

	case r.Method == http.MethodPut && q.Has("x-id"):
		body, _ := io.ReadAll(r.Body)
		ms.mu.Lock()
		ms.singlePuts++
		ms.singlePutBody = body
		ms.mu.Unlock()
		w.Header().Set("ETag", `"single-etag"`)
		w.WriteHeader(http.StatusOK)
		return

	case r.Method == http.MethodHead:
		ms.mu.Lock()
		var size int
		for _, b := range ms.parts {
			size += len(b)
		}
		done := ms.completed
		ms.mu.Unlock()
		if !done {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.Header().Set("ETag", `"complete-etag"`)
		w.WriteHeader(http.StatusOK)
		return
	}

	ms.t.Logf("unhandled request: %s %s", r.Method, r.URL.String())
	w.WriteHeader(http.StatusOK)
}

// partAttemptCounts returns how many times each part number was requested.
func (ms *multipartServer) partAttemptCounts() map[int]int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	out := make(map[int]int, len(ms.partSends))
	for k, v := range ms.partSends {
		out[k] = v
	}
	return out
}

// storedParts returns the currently-stored part numbers.
func (ms *multipartServer) storedParts() []int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	parts := make([]int, 0, len(ms.parts))
	for n := range ms.parts {
		parts = append(parts, n)
	}
	return parts
}

func (ms *multipartServer) assembled() []byte {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var out []byte
	for n := 1; n <= len(ms.parts); n++ {
		body, ok := ms.parts[n]
		if !ok {
			ms.t.Fatalf("missing part %d of %d", n, len(ms.parts))
		}
		out = append(out, body...)
	}
	return out
}

// shortenSDKRetries keeps failure-path tests fast: the real SDK retryer spends
// seconds retrying every injected 5xx before the upload layer even sees it.
func shortenSDKRetries(t *testing.T) {
	t.Helper()
	old := transportRetryerFactory
	transportRetryerFactory = func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 1
			o.RateLimiter = ratelimit.None
		})
	}
	t.Cleanup(func() { transportRetryerFactory = old })
}

func newMultipartTestClient(t *testing.T, ms *multipartServer) *ReplicaClient {
	t.Helper()
	shortenSDKRetries(t)
	client := NewReplicaClient()
	client.Bucket = "test-bucket"
	client.Path = "replica"
	client.Region = "us-east-1"
	client.Endpoint = ms.srv.URL
	client.ForcePathStyle = true
	client.AccessKeyID = "test-access-key"
	client.SecretAccessKey = "test-secret-key"
	client.PartSize = 5 * 1024 * 1024
	client.Concurrency = 1
	if err := client.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return client
}

func writeTestLTX(t *testing.T, client *ReplicaClient, data []byte) (*ltx.FileInfo, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "0000000000000002.ltx")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write ltx: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open ltx: %v", err)
	}
	defer f.Close()
	return client.WriteLTXFile(context.Background(), 0, 2, 2, f)
}

// TestWriteLTXFile_MultipartRetryReusesConfirmedParts verifies a transient
// failure part-way through a multipart upload retries only the missing parts
// and that progress reports the reused bytes.
func TestWriteLTXFile_MultipartRetryReusesConfirmedParts(t *testing.T) {
	ms := newMultipartServer(t)
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)
	client := newMultipartTestClient(t, ms)

	// Fail part 2 once. With the per-part budget already spent, the failure
	// escalates to the object layer, which must re-send only part 2 and
	// reuse part 1.
	client.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	var failed atomic.Bool
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		if part == 2 && failed.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return false
		}
		return true
	}

	info, err := writeTestLTX(t, client, data)
	if err != nil {
		t.Fatalf("WriteLTXFile: %v", err)
	}
	if !bytes.Equal(ms.assembled(), data) {
		t.Fatalf("assembled object does not match source")
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size=%d, want %d", info.Size, len(data))
	}
	if ms.aborts != 0 {
		t.Fatalf("aborts=%d, want 0", ms.aborts)
	}
	if !failed.Load() {
		t.Fatal("test did not inject a retryable failure")
	}
	progress, ok := client.UploadProgress(0, 2, 2)
	if !ok {
		t.Fatal("expected upload progress")
	}
	if progress.ReusedBytes == 0 {
		t.Fatalf("expected reused bytes on retry, progress=%+v", progress)
	}
	if progress.RemainingParts != 0 {
		t.Fatalf("remaining parts=%d, want 0", progress.RemainingParts)
	}
}

// TestWriteLTXFile_MultipartResumesAfterRestart verifies persisted part state
// lets a new client resume an upload without re-sending stored parts.
func TestWriteLTXFile_MultipartResumesAfterRestart(t *testing.T) {
	ms := newMultipartServer(t)
	stateDir := t.TempDir()
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 16*mb)

	first := newMultipartTestClient(t, ms)
	first.SetMultipartStateDir(stateDir)
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		// Fail part 3 so the first two parts are confirmed and persisted
		// before this attempt gives up.
		if part == 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	}
	// Only attempt each part and object once so this attempt fails out to the
	// caller with the confirmed parts persisted.
	first.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	first.MultipartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	if _, err := writeTestLTX(t, first, data); err == nil {
		t.Fatal("expected the first attempt to fail")
	}
	partsAfterFail := len(ms.parts)
	if partsAfterFail == 0 {
		t.Fatal("expected at least one confirmed part before the failure")
	}

	// A fresh client with no in-memory state must load the persisted record.
	second := newMultipartTestClient(t, ms)
	second.SetMultipartStateDir(stateDir)

	var resentParts atomic.Int64
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		resentParts.Add(1)
		return true
	}

	info, err := writeTestLTX(t, second, data)
	if err != nil {
		t.Fatalf("resumed WriteLTXFile: %v", err)
	}
	if !bytes.Equal(ms.assembled(), data) {
		t.Fatal("assembled object does not match source after resume")
	}
	totalParts := (len(data) + 5*mb - 1) / (5 * mb)
	if got, want := int(resentParts.Load()), totalParts-partsAfterFail; got > want {
		t.Fatalf("resent %d parts after resume, want at most %d", got, want)
	}
	if got := int(resentParts.Load()); got == 0 {
		t.Fatal("expected the missing parts to be re-sent")
	}
	if ms.creates != 1 {
		t.Fatalf("CreateMultipartUpload called %d times, want 1 (resume must reuse the upload)", ms.creates)
	}
	progress, ok := second.UploadProgress(0, 2, 2)
	if !ok {
		t.Fatal("expected upload progress")
	}
	if progress.ReusedBytes == 0 {
		t.Fatalf("expected reused bytes after restart, progress=%+v", progress)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size=%d, want %d", info.Size, len(data))
	}
}

// TestWriteLTXFile_MultipartCompleteOnlyAfterAllParts verifies completion is
// attempted only after every part is confirmed, and that a failed completion
// does not publish a completion marker.
func TestWriteLTXFile_MultipartCompleteOnlyAfterAllParts(t *testing.T) {
	ms := newMultipartServer(t)
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)
	client := newMultipartTestClient(t, ms)

	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		if part == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return false
		}
		return true
	}
	client.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	client.MultipartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}

	_, err := writeTestLTX(t, client, data)
	if err == nil {
		t.Fatal("expected failure")
	}
	if ms.completes != 0 {
		t.Fatalf("CompleteMultipartUpload called %d times despite a missing confirmed part", ms.completes)
	}
	progress, ok := client.UploadProgress(0, 2, 2)
	if !ok {
		t.Fatal("expected progress")
	}
	if progress.RemainingParts <= 0 {
		t.Fatalf("remaining parts=%d, want > 0", progress.RemainingParts)
	}
}

// TestWriteLTXFile_MultipartAuthenticationIsNotRetried verifies an
// authorization failure aborts immediately and is reported as non-retryable.
func TestWriteLTXFile_MultipartAuthenticationIsNotRetried(t *testing.T) {
	ms := newMultipartServer(t)
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)
	client := newMultipartTestClient(t, ms)

	var partRequests atomic.Int64
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		partRequests.Add(1)
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
		return false
	}

	_, err := writeTestLTX(t, client, data)
	if err == nil {
		t.Fatal("expected authentication failure")
	}
	var uploadErr *litestream.UploadError
	if !errors.As(err, &uploadErr) {
		t.Fatalf("error=%T %v, want *litestream.UploadError", err, err)
	}
	if uploadErr.Retryable {
		t.Fatalf("authentication failure marked retryable: %v", err)
	}
	if !litestream.IsAuthenticationError(uploadErr.Err) {
		t.Fatalf("underlying error is not an auth failure: %v", uploadErr.Err)
	}
	// Concurrency may have several parts in flight when the first 403 lands.
	// What matters is that no part is sent more than once: an authentication
	// failure must never re-enter the retry path, at either layer.
	for part, attempts := range ms.partAttemptCounts() {
		if attempts > 1 {
			t.Fatalf("part %d sent %d times; auth failure must not retry", part, attempts)
		}
	}
	if got := partRequests.Load(); got > 1 {
		t.Logf("part requests=%d (concurrent in-flight requests share the first 403)", got)
	}
	if ms.creates != 1 {
		t.Fatalf("CreateMultipartUpload called %d times; auth failure must not restart the upload", ms.creates)
	}
}

func TestWriteLTXFile_MultipartSourceChangeAborts(t *testing.T) {
	ms := newMultipartServer(t)
	stateDir := t.TempDir()
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)

	client := newMultipartTestClient(t, ms)
	client.SetMultipartStateDir(stateDir)

	var failPart atomic.Bool
	failPart.Store(true)
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		if failPart.Load() && part == 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	}

	// One part attempt and one object attempt so the call returns once both
	// budgets are spent and the confirmed parts persist.
	client.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	client.MultipartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}

	if _, err := writeTestLTX(t, client, data); err == nil {
		t.Fatal("expected first upload to fail")
	}
	if confirmed := len(ms.storedParts()); confirmed < 2 {
		t.Fatalf("confirmed parts=%d, want >= 2", confirmed)
	}

	// Change the source so a part that is already stored has different bytes.
	// Keep the LTX header intact so the upload still starts.
	mutated := bytes.Clone(data)
	mutated[len(mutated)/2] ^= 0xff

	failPart.Store(false)
	_, err := writeTestLTX(t, client, mutated)
	if err == nil {
		t.Fatal("expected source-changed error")
	}
	if !strings.Contains(err.Error(), "source changed") {
		t.Fatalf("error=%v, want source changed", err)
	}
	// The corrupt partial upload must be aborted, not completed.
	if ms.aborts == 0 {
		t.Fatal("expected AbortMultipartUpload after a source change")
	}
	if ms.completes != 0 {
		t.Fatalf("completed=%d corrupt uploads", ms.completes)
	}
}

// TestWriteLTXFile_MultipartCompletionResponseLostThenRetried verifies a
// completion whose response is lost does not cause the object to be uploaded
// again under a new upload ID. The retry must reconcile against the durable
// object.
func TestWriteLTXFile_MultipartCompletionResponseLostThenRetried(t *testing.T) {
	ms := newMultipartServer(t)
	stateDir := t.TempDir()
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)

	client := newMultipartTestClient(t, ms)
	client.SetMultipartStateDir(stateDir)
	client.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	client.MultipartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}

	// The provider stores the completed object but the response is lost: the
	// test client sees a transport-level failure.
	ms.completeFn = func(attempt int, w http.ResponseWriter) bool {
		if attempt == 1 {
			// Abort the connection without writing a response.
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer is not hijackable")
				return false
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return false
			}
			_ = conn.Close()
			return false
		}
		return true
	}

	// The first call fails: the client cannot know whether completion landed.
	if _, err := writeTestLTX(t, client, data); err == nil {
		t.Fatal("expected the first completion to fail")
	}
	if ms.aborts != 0 {
		t.Fatalf("aborts=%d: a lost completion response must not abort a completed upload", ms.aborts)
	}

	// A second client resumes from persisted state. The provider reports
	// NoSuchUpload for the completed upload, so the object must be reconciled
	// by size rather than re-uploaded.
	second := newMultipartTestClient(t, ms)
	second.SetMultipartStateDir(stateDir)
	second.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}

	partsBefore := len(ms.storedParts())
	if _, err := writeTestLTX(t, second, data); err != nil {
		t.Fatalf("second WriteLTXFile: %v", err)
	}
	if ms.creates != 1 {
		t.Fatalf("CreateMultipartUpload called %d times, want 1", ms.creates)
	}
	if got := len(ms.storedParts()); got != partsBefore {
		t.Fatalf("parts after reconciliation=%d, want unchanged %d", got, partsBefore)
	}
	if !bytes.Equal(ms.assembled(), data) {
		t.Fatal("assembled object does not match source")
	}
}

// TestWriteLTXFile_MultipartRestartsWithFreshUploadWhenProviderExpired
// verifies local state naming an upload the provider no longer has restarts
// cleanly instead of failing permanently.
func TestWriteLTXFile_MultipartRestartsWithFreshUploadWhenProviderExpired(t *testing.T) {
	ms := newMultipartServer(t)
	stateDir := t.TempDir()
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)

	first := newMultipartTestClient(t, ms)
	first.SetMultipartStateDir(stateDir)
	first.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	first.MultipartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}

	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		if part == 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	}
	if _, err := writeTestLTX(t, first, data); err == nil {
		t.Fatal("expected first upload to fail")
	}

	// The provider drops the upload and every request against the old ID now
	// fails with NoSuchUpload, as after an expiration or lifecycle abort.
	ms.mu.Lock()
	ms.uploadID = "upload-2"
	oldID := "upload-1"
	ms.expiredUploads = map[string]bool{oldID: true}
	ms.parts = make(map[int][]byte)
	ms.partETags = make(map[int]string)
	ms.mu.Unlock()

	second := newMultipartTestClient(t, ms)
	second.SetMultipartStateDir(stateDir)

	// Requests against the expired upload must fail, but a new upload works.
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool { return true }

	info, err := writeTestLTX(t, second, data)
	if err != nil {
		t.Fatalf("resumed WriteLTXFile: %v", err)
	}
	if !bytes.Equal(ms.assembled(), data) {
		t.Fatal("assembled object does not match source")
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size=%d, want %d", info.Size, len(data))
	}
	if ms.aborts == 0 {
		t.Fatal("expected the expired upload to be aborted")
	}
	if ms.creates != 2 {
		t.Fatalf("CreateMultipartUpload calls=%d, want 2 (original + restart)", ms.creates)
	}
}

// TestWriteLTXFile_MultipartReconcilesDroppedProviderParts verifies a part the
// provider no longer lists is re-sent instead of being assumed present, so
// completion cannot assemble a short object.
func TestWriteLTXFile_MultipartReconcilesDroppedProviderParts(t *testing.T) {
	ms := newMultipartServer(t)
	stateDir := t.TempDir()
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)

	first := newMultipartTestClient(t, ms)
	first.SetMultipartStateDir(stateDir)
	first.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	first.MultipartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		if part == 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return false
		}
		return true
	}
	if _, err := writeTestLTX(t, first, data); err == nil {
		t.Fatal("expected first upload to fail")
	}

	// The provider loses part 2, e.g. after internal garbage collection. The
	// local record still claims it, so a resumed upload must trust the
	// provider's listing and re-send that part.
	ms.mu.Lock()
	delete(ms.parts, 2)
	delete(ms.partETags, 2)
	ms.mu.Unlock()

	second := newMultipartTestClient(t, ms)
	second.SetMultipartStateDir(stateDir)
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool { return true }

	var resent []int
	var resentMu sync.Mutex
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		resentMu.Lock()
		resent = append(resent, part)
		resentMu.Unlock()
		return true
	}

	if _, err := writeTestLTX(t, second, data); err != nil {
		t.Fatalf("resumed WriteLTXFile: %v", err)
	}
	resentMu.Lock()
	got := append([]int(nil), resent...)
	resentMu.Unlock()
	if !slices.Contains(got, 2) {
		t.Fatalf("re-sent parts=%v, want part 2 re-sent after the provider dropped it", got)
	}
	if slices.Contains(got, 1) {
		t.Fatalf("re-sent parts=%v, part 1 was already stored and must be reused", got)
	}
	if !bytes.Equal(ms.assembled(), data) {
		t.Fatal("assembled object does not match source")
	}
}

func TestMultipartUploadThresholdStillSinglePutsSmallObjects(t *testing.T) {
	ms := newMultipartServer(t)
	client := newMultipartTestClient(t, ms)

	// An object exactly one part in size must not start a multipart upload.
	data := mustLTXWithSize(t, 5*1024*1024)
	if _, err := writeTestLTX(t, client, data); err != nil {
		t.Fatalf("WriteLTXFile: %v", err)
	}
	if ms.creates != 0 {
		t.Fatalf("CreateMultipartUpload called %d times for a single-part object", ms.creates)
	}
	ms.mu.Lock()
	single := ms.singlePuts
	ms.mu.Unlock()
	if single != 1 {
		t.Fatalf("PutObject count=%d, want 1", single)
	}
}

func TestWriteLTXFile_MultipartSinglePutFallbackOnListPartsUnsupported(t *testing.T) {
	ms := newMultipartServer(t)
	ms.listParts = false
	stateDir := t.TempDir()
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)

	client := newMultipartTestClient(t, ms)
	client.SetMultipartStateDir(stateDir)

	var failed atomic.Bool
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		if part == 3 && failed.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return false
		}
		return true
	}

	info, err := writeTestLTX(t, client, data)
	if err != nil {
		t.Fatalf("WriteLTXFile: %v", err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size=%d", info.Size)
	}
	if !bytes.Equal(ms.assembled(), data) {
		t.Fatal("assembled object does not match source")
	}
}

func TestMultipartStateDirDisabledStillResumesInProcess(t *testing.T) {
	ms := newMultipartServer(t)
	const mb = 1024 * 1024
	data := mustLTXWithSize(t, 12*mb)
	client := newMultipartTestClient(t, ms)
	// Spend the part budget on the first failure so the object-level retry
	// (which is what reuses parts) actually runs.
	client.PartRetryPolicy = internal.RetryPolicy{MaxAttempts: 1}

	var failed atomic.Bool
	ms.partFn = func(part int, attempt int, w http.ResponseWriter) bool {
		if part == 3 && failed.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return false
		}
		return true
	}

	if _, err := writeTestLTX(t, client, data); err != nil {
		t.Fatalf("WriteLTXFile: %v", err)
	}
	if client.multipartStateStore().Enabled() {
		t.Fatal("state dir unexpectedly enabled")
	}
	progress, ok := client.UploadProgress(0, 2, 2)
	if !ok {
		t.Fatal("expected progress")
	}
	if progress.ReusedBytes == 0 {
		t.Fatalf("in-process retry should reuse confirmed parts, progress=%+v", progress)
	}
}
