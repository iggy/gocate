package index

import (
	"context"
	"sync"
)

// defaultMaxBytes is the default in-flight hashing budget for a run with the
// given number of workers: room for every worker to hold the most content a
// single hash can occupy at once.
func defaultMaxBytes(workers int) int64 {
	return int64(workers) * (hashBufferLimit + streamReadBuffer)
}

// residentBytes is the file content one hash of a file of the given size keeps
// in memory: all of it below hashBufferLimit (where a single buffered read
// feeds both hashes), a fixed read buffer above that.
func residentBytes(size int64) int64 {
	if size > hashBufferLimit {
		return streamReadBuffer
	}
	return size
}

// byteBudget bounds the file content the hashing workers hold at once. The walk
// acquires before starting a hash and the worker releases when it finishes, so
// the walk waits rather than queueing an unbounded number of readers.
//
// The budget charges residentBytes, never a file's whole size. Charging sizes
// charged a multi-gigabyte file for bytes it never reads, so a directory of a
// few huge files admitted one worker at a time and hashed on a single core.
type byteBudget struct {
	mu   sync.Mutex
	cond *sync.Cond
	used int64
	max  int64
}

// newByteBudget returns a budget of maxBytes in-flight content.
func newByteBudget(maxBytes int64) *byteBudget {
	b := &byteBudget{max: maxBytes}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// acquire takes n bytes of the budget, waiting until they are free or ctx is
// done, and returns ctx.Err() on cancellation. A budget with nothing in flight
// always admits, however large the request, so a budget smaller than a single
// charge can never stall the walk forever.
func (b *byteBudget) acquire(ctx context.Context, n int64) error {
	b.mu.Lock()
	for b.used != 0 && b.used+n > b.max {
		if ctx.Err() != nil {
			b.mu.Unlock()
			return ctx.Err()
		}
		b.cond.Wait()
	}
	b.used += n
	b.mu.Unlock()
	return nil
}

// release returns n bytes to the pool and wakes waiters.
func (b *byteBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	b.cond.Broadcast()
	b.mu.Unlock()
}

// wakeAll wakes every waiter without changing the budget, so a waiter can
// observe a cancellation instead of sleeping on a budget nobody will free.
func (b *byteBudget) wakeAll() {
	b.mu.Lock()
	b.cond.Broadcast()
	b.mu.Unlock()
}
