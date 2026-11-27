package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type httpStatusError struct {
	code int
}

func (e *httpStatusError) Error() string       { return fmt.Sprintf("http %d", e.code) }
func (e *httpStatusError) HTTPStatusCode() int { return e.code }

type apiError struct {
	code string
}

func (e *apiError) Error() string     { return "api " + e.code }
func (e *apiError) ErrorCode() string { return e.code }

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"Nil", nil, false},
		{"ServerError500", &httpStatusError{500}, true},
		{"ServerError503", &httpStatusError{503}, true},
		{"TooManyRequests", &httpStatusError{429}, true},
		{"RequestTimeout", &httpStatusError{408}, true},
		{"Forbidden403", &httpStatusError{403}, false},
		{"Unauthorized401", &httpStatusError{401}, false},
		{"BadRequest400", &httpStatusError{400}, false},
		{"NotFound404", &httpStatusError{404}, false},
		{"AccessDenied", &apiError{"AccessDenied"}, false},
		{"NoSuchUpload", &apiError{"NoSuchUpload"}, false},
		{"ConnectionReset", fmt.Errorf("read: %w", syscall.ECONNRESET), true},
		{"UnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"ContextCanceled", context.Canceled, false},
		{"UnknownError", errors.New("mystery"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryable(tt.err); got != tt.want {
				t.Fatalf("IsRetryable(%v)=%v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsAuthenticationError(t *testing.T) {
	if !IsAuthenticationError(&httpStatusError{403}) {
		t.Fatal("expected 403 to be an authentication error")
	}
	if !IsAuthenticationError(&apiError{"SignatureDoesNotMatch"}) {
		t.Fatal("expected signature mismatch to be an authentication error")
	}
	if IsAuthenticationError(&httpStatusError{500}) {
		t.Fatal("500 must not be an authentication error")
	}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	setRetryHooks(t, func() float64 { return 0 }, func(context.Context, time.Duration) error { return nil })

	var attempts int
	err := Retry(context.Background(), RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond}, nil, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return &httpStatusError{503}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
}

func TestRetryBudgetExhausted(t *testing.T) {
	setRetryHooks(t, func() float64 { return 0 }, func(context.Context, time.Duration) error { return nil })

	var attempts int
	err := Retry(context.Background(), RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond}, nil, func(context.Context) error {
		attempts++
		return &httpStatusError{500}
	})
	if !errors.Is(err, ErrRetryBudgetExhausted) {
		t.Fatalf("expected budget error, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
	var budgetErr *RetryBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Attempts != 3 {
		t.Fatalf("budget error = %#v, want 3 attempts", budgetErr)
	}
}

func TestRetryNonRetryableDoesNotRetry(t *testing.T) {
	setRetryHooks(t, func() float64 { return 0 }, func(context.Context, time.Duration) error { return nil })

	var attempts int
	err := Retry(context.Background(), RetryPolicy{MaxAttempts: 5}, nil, func(context.Context) error {
		attempts++
		return &apiError{"AccessDenied"}
	})
	if attempts != 1 {
		t.Fatalf("authentication failure retried %d times, want 1", attempts)
	}
	if IsRetryable(err) {
		t.Fatal("authentication error must not be retryable")
	}
}

func TestRetryPermanentWrapperStopsRetry(t *testing.T) {
	setRetryHooks(t, func() float64 { return 0 }, func(context.Context, time.Duration) error { return nil })

	var attempts int
	err := Retry(context.Background(), RetryPolicy{MaxAttempts: 5}, nil, func(context.Context) error {
		attempts++
		return Permanent(&httpStatusError{500})
	})
	if attempts != 1 {
		t.Fatalf("permanent error retried %d times, want 1", attempts)
	}
	if IsRetryable(err) {
		t.Fatal("permanent error must not be retryable")
	}
}

func TestRetryContextCancellationStops(t *testing.T) {
	setRetryHooks(t, func() float64 { return 1 }, func(ctx context.Context, _ time.Duration) error {
		return context.Cause(ctx)
	})

	ctx, cancel := context.WithCancel(context.Background())
	var attempts int
	err := Retry(ctx, RetryPolicy{MaxAttempts: 10, BaseDelay: time.Second}, nil, func(context.Context) error {
		attempts++
		cancel()
		return &httpStatusError{500}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestRetryJitterSpread(t *testing.T) {
	// Full jitter must produce delays inside the exponential ceiling and
	// must not collapse every instance onto the same instant.
	delays := make([]time.Duration, 0, 64)
	var mu sync.Mutex
	setRetryHooks(t, defaultJitter, func(_ context.Context, d time.Duration) error {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		return nil
	})

	var attempts atomic.Int64
	_ = Retry(context.Background(), RetryPolicy{MaxAttempts: 8, BaseDelay: time.Second, MaxDelay: 4 * time.Second}, nil, func(context.Context) error {
		if attempts.Add(1) < 8 {
			return &httpStatusError{502}
		}
		return nil
	})

	seen := make(map[time.Duration]struct{})
	for _, d := range delays {
		if d < 0 || d > 4*time.Second {
			t.Fatalf("delay %s outside [0, 4s]", d)
		}
		seen[d] = struct{}{}
	}
	if len(delays) == 0 {
		t.Fatal("expected retry delays")
	}
	if len(seen) < 2 {
		t.Fatalf("delays=%v: full jitter should not produce identical values", delays)
	}
}

// setRetryHooks replaces the jitter and sleep hooks for one test.
func setRetryHooks(t *testing.T, jitter func() float64, sleep func(context.Context, time.Duration) error) {
	t.Helper()
	oldJitter, oldSleep := retryJitter, retrySleep
	retryJitter, retrySleep = jitter, sleep
	t.Cleanup(func() { retryJitter, retrySleep = oldJitter, oldSleep })
}

func defaultJitter() float64 {
	// Deterministic pseudo-random source for this test only.
	return float64(time.Now().UnixNano()%1000) / 1000
}

func TestMultipartStateStorePersistence(t *testing.T) {
	dir := t.TempDir()
	store := NewMultipartStateStore(dir)

	state := &MultipartState{
		Key:        "replica/0000/0000000000000001-0000000000000001.ltx",
		UploadID:   "upload-1",
		PartSize:   5 * 1024 * 1024,
		TotalBytes: 12 * 1024 * 1024,
	}
	state.SetPart(MultipartPart{Number: 2, Size: 5 * 1024 * 1024, MD5: "bb", ETag: "e2"})
	state.SetPart(MultipartPart{Number: 1, Size: 5 * 1024 * 1024, MD5: "aa", ETag: "e1"})
	if err := store.Save(state); err != nil {
		t.Fatalf("save: %v", err)
	}

	// A fresh store simulates a process restart.
	reloaded, err := NewMultipartStateStore(dir).Load(state.Key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if reloaded == nil {
		t.Fatal("expected persisted state")
	}
	if reloaded.UploadID != "upload-1" {
		t.Fatalf("upload id = %q", reloaded.UploadID)
	}
	if got, want := len(reloaded.Parts), 2; got != want {
		t.Fatalf("parts=%d, want %d", got, want)
	}
	if reloaded.Parts[0].Number != 1 {
		t.Fatalf("parts not sorted: %#v", reloaded.Parts)
	}
	if got, want := reloaded.CompletedBytes(), int64(10*1024*1024); got != want {
		t.Fatalf("completed bytes=%d, want %d", got, want)
	}

	if err := store.Remove(state.Key); err != nil {
		t.Fatalf("remove: %v", err)
	}
	reloaded, err = NewMultipartStateStore(dir).Load(state.Key)
	if err != nil {
		t.Fatalf("load after remove: %v", err)
	}
	if reloaded != nil {
		t.Fatal("expected state to be removed")
	}
}

func TestMultipartStateStoreCorruptRecordDiscarded(t *testing.T) {
	dir := t.TempDir()
	store := NewMultipartStateStore(dir)
	state := &MultipartState{Key: "k", UploadID: "u", PartSize: 5, TotalBytes: 5}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}

	// Corrupt the record directly: a record that cannot be parsed must be
	// dropped rather than resumed from possibly-wrong part boundaries.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(entries))
	}
	if err := os.WriteFile(filepath.Join(dir, entries[0].Name()), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := store.Load("k")
	if err != nil {
		t.Fatalf("load corrupt: %v", err)
	}
	if got != nil {
		t.Fatalf("expected corrupt record to be discarded, got %#v", got)
	}
}

func TestMultipartStateStoreExpiredRecordDiscarded(t *testing.T) {
	dir := t.TempDir()
	store := NewMultipartStateStore(dir)
	now := time.Now()
	store.now = func() time.Time { return now }

	state := &MultipartState{Key: "k", UploadID: "u", PartSize: 5, TotalBytes: 5}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}

	// Advance beyond the retention window: the provider has long expired the
	// multipart upload, so resuming its part numbers cannot work.
	reloaded := NewMultipartStateStore(dir)
	reloaded.now = func() time.Time { return now.Add(MaxMultipartStateAge + time.Minute) }
	got, err := reloaded.Load("k")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != nil {
		t.Fatalf("expected expired record to be discarded, got %#v", got)
	}
}

func TestMultipartStateStoreInMemoryWithoutDir(t *testing.T) {
	store := NewMultipartStateStore("")
	state := &MultipartState{Key: "k", UploadID: "u", PartSize: 5}
	state.SetPart(MultipartPart{Number: 1, Size: 5})
	if err := store.Save(state); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.Load("k")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got == nil || len(got.Parts) != 1 {
		t.Fatalf("expected in-memory state, got %#v", got)
	}
	if err := store.Remove("k"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got, _ := store.Load("k"); got != nil {
		t.Fatal("expected removal from memory")
	}
}

func TestKeyedMutexSerializesSameKey(t *testing.T) {
	var m KeyedMutex
	const key = "same"
	var active, maxActive atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.Lock(context.Background(), key); err != nil {
				t.Error(err)
				return
			}
			n := active.Add(1)
			for {
				cur := maxActive.Load()
				if n <= cur || maxActive.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			m.Unlock(key)
		}()
	}
	wg.Wait()
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("max concurrent holders=%d, want 1", got)
	}
}

func TestKeyedMutexDifferentKeysProceed(t *testing.T) {
	var m KeyedMutex
	if !m.TryLock("a") {
		t.Fatal("expected to lock a")
	}
	if m.TryLock("a") {
		t.Fatal("same key must not lock twice")
	}
	if !m.TryLock("b") {
		t.Fatal("different keys must not block")
	}
	m.Unlock("a")
	m.Unlock("b")
	if !m.TryLock("a") {
		t.Fatal("expected a to be reacquirable")
	}
	m.Unlock("a")
}

func TestKeyedMutexCanceledWait(t *testing.T) {
	var m KeyedMutex
	if err := m.Lock(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Lock(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	m.Unlock("k")
}
