package litestream_test

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
)

type uploadTestHTTPError struct{ code int }

func (e *uploadTestHTTPError) Error() string       { return fmt.Sprintf("http %d", e.code) }
func (e *uploadTestHTTPError) HTTPStatusCode() int { return e.code }

func TestUploadError_ReasonAndUnwrap(t *testing.T) {
	t.Run("Exhausted", func(t *testing.T) {
		err := &litestream.UploadError{
			Level: 0, MinTXID: 1, MaxTXID: 1, Key: "k",
			Attempts: 3, UploadedBytes: 10, ReusedBytes: 4,
			CompletedParts: 2, RemainingParts: 1,
			Retryable: true, Err: &uploadTestHTTPError{http.StatusServiceUnavailable},
		}
		if got, want := err.Reason(), "retry budget exhausted"; got != want {
			t.Fatalf("Reason()=%q, want %q", got, want)
		}
		if !errors.Is(err, litestream.ErrRetryBudgetExhausted) {
			t.Fatal("expected ErrRetryBudgetExhausted in chain")
		}
		// The underlying provider error stays inspectable.
		var httpErr interface{ HTTPStatusCode() int }
		if !errors.As(err, &httpErr) || httpErr.HTTPStatusCode() != http.StatusServiceUnavailable {
			t.Fatal("underlying error not preserved")
		}
		msg := err.Error()
		for _, want := range []string{"uploaded=10 bytes", "reused=4 bytes", "remaining=1", "retry budget exhausted"} {
			if !strings.Contains(msg, want) {
				t.Fatalf("error %q missing %q", msg, want)
			}
		}
	})

	t.Run("Permanent", func(t *testing.T) {
		err := &litestream.UploadError{
			Key: "k", Attempts: 1, Retryable: false, Err: &uploadTestHTTPError{403},
		}
		if got, want := err.Reason(), "permanent"; got != want {
			t.Fatalf("Reason()=%q, want %q", got, want)
		}
		if errors.Is(err, litestream.ErrRetryBudgetExhausted) {
			t.Fatal("permanent upload error must not report budget exhaustion")
		}
	})

	t.Run("AuthenticationIsNotRetryable", func(t *testing.T) {
		if litestream.IsRetryable(&uploadTestHTTPError{http.StatusForbidden}) {
			t.Fatal("403 must not be retryable")
		}
		if !litestream.IsAuthenticationError(&uploadTestHTTPError{http.StatusUnauthorized}) {
			t.Fatal("401 must be classified as authentication failure")
		}
	})
}

func TestUploadProgress_ZeroValue(t *testing.T) {
	var p litestream.UploadProgress
	if p.UploadedBytes != 0 || p.RemainingParts != 0 {
		t.Fatalf("unexpected zero-value progress: %+v", p)
	}
}
