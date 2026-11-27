package integrity

import (
	"errors"
	"io"
)

// ReadCloser verifies complete chunks before releasing their bytes to the
// caller. A corrupt chunk is never returned as plaintext: Read returns the
// *Error and the mismatch hook (usually quarantine) runs exactly once.
type ReadCloser struct {
	rc       io.ReadCloser
	verifier *Verifier

	// ready holds verified but not yet consumed bytes from one chunk.
	ready []byte
	pos   int

	failure error
	closed  bool
}

// NewReadCloser wraps rc with manifest verification.
func NewReadCloser(rc io.ReadCloser, m *Manifest, side Side, opts ...VerifierOption) (*ReadCloser, error) {
	v, err := NewVerifier(m, side, opts...)
	if err != nil {
		return nil, err
	}
	return &ReadCloser{rc: rc, verifier: v}, nil
}

// NewValidatingReader wraps an io.Reader without Close.
func NewValidatingReader(r io.Reader, m *Manifest, side Side, opts ...VerifierOption) (*Reader, error) {
	v, err := NewVerifier(m, side, opts...)
	if err != nil {
		return nil, err
	}
	return &Reader{r: r, verifier: v}, nil
}

// Reader is the non-Closer variant of ReadCloser.
type Reader struct {
	r        io.Reader
	verifier *Verifier
	ready    []byte
	pos      int
	failure  error
}

// CacheHits returns chunk cache hits observed so far.
func (r *Reader) CacheHits() int { return r.verifier.CacheHits() }

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	if r == nil {
		return 0, io.ErrClosedPipe
	}
	if r.failure != nil {
		return 0, r.failure
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Return bytes already verified in the current chunk.
	if r.pos < len(r.ready) {
		n := copy(p, r.ready[r.pos:])
		r.pos += n
		if r.pos == len(r.ready) {
			r.ready = nil
			r.pos = 0
		}
		return n, nil
	}

	if r.verifier.idx >= len(r.verifier.manifest.Chunks) {
		// All expected chunks have been released. Check for trailing bytes on
		// the next call rather than doing extra I/O from Read.
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n > 0 {
			r.failure = r.verifier.failExtra()
			return 0, r.failure
		}
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		return 0, nil
	}

	expected := r.verifier.manifest.Chunks[r.verifier.idx]
	buf := make([]byte, expected.Size)
	n, err := io.ReadFull(r.r, buf)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			// Feed the partial bytes so accounting/error location stays
			// consistent, then convert to a truncation integrity error.
			_, _ = r.verifier.Write(buf[:n])
			if finishErr := r.verifier.Finish(); finishErr != nil {
				r.failure = finishErr
				return 0, finishErr
			}
			r.failure = err
			return 0, err
		}
		return 0, err
	}

	if _, err := r.verifier.Write(buf); err != nil {
		r.failure = err
		return 0, err
	}

	r.ready = buf
	r.pos = 0
	return r.Read(p)
}

// WriteTo lets io.Copy avoid allocating through small caller buffers while
// still preserving the verify-before-release guarantee.
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	var written int64
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			written += int64(wn)
			if werr != nil {
				return written, werr
			}
			if wn != n {
				return written, io.ErrShortWrite
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return written, nil
			}
			return written, err
		}
	}
}

// Read implements io.ReadCloser.
func (r *ReadCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.failure != nil {
		return 0, r.failure
	}
	if r.pos < len(r.ready) {
		n := copy(p, r.ready[r.pos:])
		r.pos += n
		if r.pos == len(r.ready) {
			r.ready = nil
			r.pos = 0
		}
		return n, nil
	}

	if r.verifier.idx >= len(r.verifier.manifest.Chunks) {
		var one [1]byte
		n, err := r.rc.Read(one[:])
		if n > 0 {
			r.failure = r.verifier.failExtra()
			return 0, r.failure
		}
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		return 0, nil
	}

	expected := r.verifier.manifest.Chunks[r.verifier.idx]
	buf := make([]byte, expected.Size)
	n, err := io.ReadFull(r.rc, buf)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			_, _ = r.verifier.Write(buf[:n])
			if finishErr := r.verifier.Finish(); finishErr != nil {
				r.failure = finishErr
				return 0, finishErr
			}
			r.failure = err
			return 0, err
		}
		return 0, err
	}
	if _, err := r.verifier.Write(buf); err != nil {
		r.failure = err
		return 0, err
	}
	r.ready = buf
	r.pos = 0
	return r.Read(p)
}

// Close closes the underlying reader. Unlike an explicit Finish, Close does
// not perform additional reads for unconsumed chunks; callers that need whole-
// file verification should read to EOF or call VerifyReader first.
func (r *ReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.rc == nil {
		return nil
	}
	return r.rc.Close()
}

// CacheHits returns chunk cache hits observed so far.
func (r *ReadCloser) CacheHits() int { return r.verifier.CacheHits() }
