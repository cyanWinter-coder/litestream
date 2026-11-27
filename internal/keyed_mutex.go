package internal

import (
	"context"
	"sync"
)

// KeyedMutex serializes work per string key while allowing work on different
// keys to proceed concurrently. It is used to guarantee that a retried upload
// of one object cannot overlap a new sync uploading the same object.
//
// Entries are reference-counted and removed when the last holder releases, so
// the map does not grow with the number of objects ever uploaded.
type KeyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	ch   chan struct{}
	refs int
}

// NewKeyedMutex returns an empty KeyedMutex.
func NewKeyedMutex() *KeyedMutex {
	return &KeyedMutex{locks: make(map[string]*keyedMutexEntry)}
}

// Lock acquires the lock for key, blocking until it is available or ctx is
// canceled. It returns the context error when canceled.
func (m *KeyedMutex) Lock(ctx context.Context, key string) error {
	entry := m.acquireEntry(key)
	select {
	case entry.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		m.releaseEntry(key, entry)
		return context.Cause(ctx)
	}
}

// Unlock releases the lock for key.
func (m *KeyedMutex) Unlock(key string) {
	m.mu.Lock()
	entry := m.locks[key]
	m.mu.Unlock()
	if entry == nil {
		return
	}

	select {
	case <-entry.ch:
	default:
		// Not held; releasing twice is a caller bug but must not deadlock.
	}
	m.releaseEntry(key, entry)
}

// TryLock acquires the lock for key without blocking. It reports false if
// another holder owns the key.
func (m *KeyedMutex) TryLock(key string) bool {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[string]*keyedMutexEntry)
	}
	entry := m.locks[key]
	if entry == nil {
		entry = &keyedMutexEntry{ch: make(chan struct{}, 1)}
		m.locks[key] = entry
	}
	entry.refs++
	m.mu.Unlock()

	select {
	case entry.ch <- struct{}{}:
		return true
	default:
		m.releaseEntry(key, entry)
		return false
	}
}

func (m *KeyedMutex) acquireEntry(key string) *keyedMutexEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks == nil {
		m.locks = make(map[string]*keyedMutexEntry)
	}
	entry := m.locks[key]
	if entry == nil {
		entry = &keyedMutexEntry{ch: make(chan struct{}, 1)}
		m.locks[key] = entry
	}
	entry.refs++
	return entry
}

func (m *KeyedMutex) releaseEntry(key string, entry *keyedMutexEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry.refs--
	if entry.refs <= 0 && m.locks[key] == entry {
		delete(m.locks, key)
	}
}
