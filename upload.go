package litestream

import (
	"fmt"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

// ErrRetryBudgetExhausted reports that an upload kept failing with retryable
// errors until its attempt or elapsed-time budget ran out. Errors returned
// after exhausted retries wrap this sentinel, so callers can distinguish a
// transient provider outage from a permanent failure.
var ErrRetryBudgetExhausted = internal.ErrRetryBudgetExhausted

// RetryPolicy bounds retries of a single storage operation.
type RetryPolicy = internal.RetryPolicy

// IsRetryable reports whether an upload error is worth retrying. It returns
// false for authentication failures, validation errors, and unknown errors so
// that only transient provider failures consume the retry budget.
func IsRetryable(err error) bool { return internal.IsRetryable(err) }

// IsAuthenticationError reports whether err is an authorization or
// authentication failure. Such errors must never enter a retry queue.
func IsAuthenticationError(err error) bool { return internal.IsAuthenticationError(err) }

// UploadProgress describes the state of an in-flight LTX upload. A backend
// reports progress after each confirmed part so operators can see how much of
// a retried upload was reused instead of re-sent.
type UploadProgress struct {
	// Key identifies the remote object. For S3 this is the object key.
	Key string
	// Level and TXID range identify the LTX file being written.
	Level   int
	MinTXID ltx.TXID
	MaxTXID ltx.TXID

	// TotalBytes is the size of the object, or zero when unknown.
	TotalBytes int64
	// UploadedBytes counts every byte confirmed by the remote store,
	// including bytes reused from a previous attempt.
	UploadedBytes int64
	// ReusedBytes counts the subset of UploadedBytes skipped because a
	// previous attempt already stored that part.
	ReusedBytes int64

	// TotalParts is the number of parts for the object, or zero when unknown.
	TotalParts int
	// CompletedParts counts parts confirmed by the remote store.
	CompletedParts int
	// RemainingParts counts parts still to confirm, or -1 when unknown.
	RemainingParts int

	// Attempt is the one-based upload attempt currently running.
	Attempt int
	// Resumed reports whether this upload reused parts from a prior attempt.
	Resumed bool
}

// UploadProgressReporter is implemented by replica clients that expose upload
// progress. Callers use it to display or log how much of a retried upload was
// reused.
type UploadProgressReporter interface {
	UploadProgress(level int, minTXID, maxTXID ltx.TXID) (UploadProgress, bool)
}

// UploadStateDirSetter is implemented by replica clients that persist
// resumable upload state locally. The DB layer points it at the database's
// meta directory so part numbers survive a process restart.
type UploadStateDirSetter interface {
	SetMultipartStateDir(dir string)
}

// UploadError describes an upload that failed after its retry budget was
// exhausted, or a permanently failed upload. It carries enough context to
// diagnose the failure: which object failed, how many attempts ran, how much
// data was already confirmed by the remote store, and how many parts remain.
type UploadError struct {
	Key      string
	Level    int
	MinTXID  ltx.TXID
	MaxTXID  ltx.TXID
	Attempts int

	UploadedBytes  int64
	ReusedBytes    int64
	CompletedParts int
	RemainingParts int

	// Retryable reports whether the underlying failure would have been
	// retryable. When true and the budget was exhausted, Err wraps
	// ErrRetryBudgetExhausted.
	Retryable bool

	Err error
}

// Reason returns a stable, operator-facing classification of the failure:
//
//	"retry budget exhausted"    - transient provider failures outlasted the budget
//	"permanent"                 - retrying cannot help (authentication,
//	                              validation, corrupt local source)
//	"retryable failure, not retried" - transient in nature but not retried here
func (e *UploadError) Reason() string {
	if e == nil {
		return ""
	}
	if e.Retryable {
		return "retry budget exhausted"
	}
	if IsRetryable(e.Err) {
		return "retryable failure, not retried"
	}
	return "permanent"
}

// Error formats the failure with the fields operators need to act on it.
func (e *UploadError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("upload ltx level=%d min=%s max=%s key=%s: %s after %d attempt(s), "+
		"uploaded=%d bytes reused=%d bytes parts=%d remaining=%d: %v",
		e.Level, e.MinTXID, e.MaxTXID, e.Key, e.Reason(), e.Attempts,
		e.UploadedBytes, e.ReusedBytes, e.CompletedParts, e.RemainingParts, e.Err)
}

// Unwrap exposes both the retry-budget sentinel (when applicable) and the
// underlying provider error.
func (e *UploadError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errs := make([]error, 0, 2)
	if e.Retryable {
		errs = append(errs, ErrRetryBudgetExhausted)
	}
	if e.Err != nil {
		errs = append(errs, e.Err)
	}
	return errs
}

// newUploadError builds an UploadError from a progress snapshot.
func newUploadError(progress UploadProgress, attempts int, retryable bool, err error) *UploadError {
	return &UploadError{
		Key:            progress.Key,
		Level:          progress.Level,
		MinTXID:        progress.MinTXID,
		MaxTXID:        progress.MaxTXID,
		Attempts:       attempts,
		UploadedBytes:  progress.UploadedBytes,
		ReusedBytes:    progress.ReusedBytes,
		CompletedParts: progress.CompletedParts,
		RemainingParts: progress.RemainingParts,
		Retryable:      retryable,
		Err:            err,
	}
}
