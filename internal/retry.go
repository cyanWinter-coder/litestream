package internal

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"syscall"
	"time"
)

// ErrRetryBudgetExhausted reports that an operation kept failing with
// retryable errors until its attempt or elapsed-time budget ran out. Callers
// use errors.Is to distinguish "gave up" from "failed permanently".
var ErrRetryBudgetExhausted = errors.New("retry budget exhausted")

// RetryPolicy bounds retries for an operation. The zero value is not useful;
// callers should normalize defaults before use.
type RetryPolicy struct {
	// MaxAttempts includes the initial attempt. Values below 1 are treated
	// as a single attempt.
	MaxAttempts int

	// BaseDelay is the ceiling for the first retry delay. Delays grow
	// exponentially from here and are capped by MaxDelay.
	BaseDelay time.Duration

	// MaxDelay caps the exponential delay ceiling.
	MaxDelay time.Duration

	// MaxElapsed bounds the total wall-clock time spent retrying, including
	// the initial attempt. Zero disables the elapsed-time budget.
	MaxElapsed time.Duration
}

// RetryAttempt describes a retry that is about to happen. It is passed to the
// optional progress callback so operators can see backoff and the last error.
type RetryAttempt struct {
	// Attempt is the one-based attempt number that will run next.
	Attempt int
	// Delay is how long Retry sleeps before that attempt.
	Delay time.Duration
	// Err is the error returned by the previous attempt.
	Err error
}

// retryJitter returns a value in [0, 1). It is a variable so tests can make
// backoff deterministic.
var retryJitter = rand.Float64

// retrySleep waits for d or until ctx is done. It is a variable so tests can
// avoid real time.
var retrySleep = func(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

// Retry runs fn until it succeeds, returns a non-retryable error, the context
// is canceled, or the policy budget is exhausted.
//
// Delays use full jitter: a uniform sample from [0, min(MaxDelay,
// BaseDelay*2^attempt)). Full jitter spreads retries of many instances across
// the whole backoff window instead of letting them converge on the same
// exponential points, which is what causes retry storms after a provider-wide
// 5xx episode.
func Retry(ctx context.Context, policy RetryPolicy, onRetry func(RetryAttempt), fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	if policy.BaseDelay < 0 {
		policy.BaseDelay = 0
	}
	if policy.MaxDelay < policy.BaseDelay {
		policy.MaxDelay = policy.BaseDelay
	}

	start := time.Now()

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		// Caller cancellation is terminal even if the underlying transport
		// error was retryable. Check the cause before the error so a canceled
		// request does not spend another budget window.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return joinCause(ctxErr, err)
		}
		if !IsRetryable(err) {
			return err
		}
		if attempt >= policy.MaxAttempts {
			return &RetryBudgetError{Attempts: attempt, Err: err}
		}

		delay := retryDelay(policy, attempt)
		if policy.MaxElapsed > 0 && time.Since(start)+delay > policy.MaxElapsed {
			return &RetryBudgetError{Attempts: attempt, Err: err}
		}

		if onRetry != nil {
			onRetry(RetryAttempt{Attempt: attempt + 1, Delay: delay, Err: err})
		}
		if sleepErr := retrySleep(ctx, delay); sleepErr != nil {
			return joinCause(sleepErr, err)
		}
	}
}

// retryDelay returns the full-jitter delay ceiling for the given zero-based
// retry number.
func retryDelay(policy RetryPolicy, failedAttempt int) time.Duration {
	ceiling := policy.BaseDelay
	for i := 1; i < failedAttempt && ceiling < policy.MaxDelay; i++ {
		if ceiling > policy.MaxDelay/2 {
			ceiling = policy.MaxDelay
			break
		}
		ceiling *= 2
	}
	ceiling = min(ceiling, policy.MaxDelay)
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(retryJitter() * float64(ceiling))
}

// RetryBudgetError reports exhausted retries. It unwraps to both the budget
// sentinel and the last underlying error so callers can inspect either.
type RetryBudgetError struct {
	Attempts int
	Err      error
}

func (e *RetryBudgetError) Error() string {
	if e.Err == nil {
		return ErrRetryBudgetExhausted.Error()
	}
	return ErrRetryBudgetExhausted.Error() + ": " + e.Err.Error()
}

func (e *RetryBudgetError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrRetryBudgetExhausted}
	}
	return []error{ErrRetryBudgetExhausted, e.Err}
}

// permanentError marks an error as not retryable regardless of the wrapped
// error's classification.
type permanentError struct{ err error }

func (e *permanentError) Error() string   { return e.err.Error() }
func (e *permanentError) Unwrap() error   { return e.err }
func (e *permanentError) Retryable() bool { return false }

// Permanent wraps err so IsRetryable always reports false. It is used for
// local preconditions (bad configuration, corrupt source) that no amount of
// remote retrying can fix.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsRetryable reports whether err is worth retrying. Unknown errors are
// treated as permanent: retrying an authentication or validation failure only
// hides the real cause and burns quota.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// An explicit retry classification on the error chain wins over anything
	// inferred from transport details.
	var classifier interface{ Retryable() bool }
	if errors.As(err, &classifier) {
		return classifier.Retryable()
	}

	// HTTP status codes from SDK response errors.
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) {
		switch code := httpErr.HTTPStatusCode(); {
		case code == 408, code == 425, code == 429:
			return true
		case code >= 500:
			return true
		case code >= 400:
			return false
		}
	}

	// Provider error codes that are never transient.
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
			"ExpiredToken", "InvalidToken", "TokenRefreshRequired",
			"NoSuchBucket", "InvalidBucketName", "AccountProblem",
			"AllAccessDisabled", "NotSignedUp", "InvalidRequest",
			"InvalidArgument", "NoSuchUpload":
			return false
		}
	}

	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}

// IsAuthenticationError reports whether err is an authorization or
// authentication failure. Authentication failures must not be queued for
// retry: repeated bad credentials cannot become good, and retrying them can
// trigger provider lockouts.
func IsAuthenticationError(err error) bool {
	if err == nil {
		return false
	}

	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) && (httpErr.HTTPStatusCode() == 401 || httpErr.HTTPStatusCode() == 403) {
		return true
	}

	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
			"ExpiredToken", "InvalidToken", "TokenRefreshRequired",
			"AccountProblem", "AllAccessDisabled":
			return true
		}
	}

	return false
}

func joinCause(cause, err error) error {
	if err == nil {
		return cause
	}
	return errors.Join(cause, err)
}
