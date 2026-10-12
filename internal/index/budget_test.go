package index

import (
	"context"
	"testing"
	"time"
)

func TestResidentBytes(t *testing.T) {
	tests := []struct {
		name string
		size int64
		want int64
	}{
		{"empty", 0, 0},
		{"small", 4096, 4096},
		{"buffered limit", hashBufferLimit, hashBufferLimit},
		{"streamed", hashBufferLimit + 1, streamReadBuffer},
		{"huge", 1 << 40, streamReadBuffer},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := residentBytes(tc.size); got != tc.want {
				t.Errorf("residentBytes(%d) = %d, want %d", tc.size, got, tc.want)
			}
		})
	}

	// A single charge must always fit inside one worker's share of the default
	// budget, otherwise the default budget throttles instead of covering the run.
	if want := defaultMaxBytes(1); residentBytes(1<<40) > want {
		t.Errorf("one huge file charges %d bytes, more than the default budget %d", residentBytes(1<<40), want)
	}
}

// TestByteBudgetAdmitsEveryHugeFileAtOnce guards the throughput regression that
// charged a hash for a file's whole size: with a 256 MiB per worker budget, four
// multi-gigabyte files fought over the budget and hashed one at a time on one
// core, which is exactly the workload a directory of large files needs to run in
// parallel.
func TestByteBudgetAdmitsEveryHugeFileAtOnce(t *testing.T) {
	const workers = 12
	const hugeFile = 4 << 30

	b := newByteBudget(defaultMaxBytes(workers))
	acquired := make(chan error, 1)
	go func() {
		for range workers {
			if err := b.acquire(context.Background(), residentBytes(hugeFile)); err != nil {
				acquired <- err
				return
			}
		}
		acquired <- nil
	}()

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("default budget for %d workers cannot hold %d of them hashing huge files", workers, workers)
	}
}

func TestByteBudgetWaitsForRelease(t *testing.T) {
	ctx := context.Background()
	b := newByteBudget(10)
	if err := b.acquire(ctx, 10); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	acquired := make(chan error, 1)
	go func() { acquired <- b.acquire(ctx, 1) }()
	select {
	case err := <-acquired:
		t.Fatalf("acquire of 1 byte returned %v while the whole budget was held", err)
	case <-time.After(50 * time.Millisecond):
	}

	b.release(10)
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("acquire after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("release did not wake the waiting acquire")
	}
}

// TestByteBudgetAdmitsOversizedCharge checks the deadlock guard: a charge bigger
// than the entire budget still runs when nothing else is in flight, so a tiny
// MaxBytes slows a run down instead of stalling it.
func TestByteBudgetAdmitsOversizedCharge(t *testing.T) {
	b := newByteBudget(1)
	if err := b.acquire(context.Background(), 1<<20); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	b.release(1 << 20)
}

func TestByteBudgetAcquireCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := newByteBudget(1)
	if err := b.acquire(ctx, 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	cancel()
	done := make(chan error, 1)
	go func() { done <- b.acquire(ctx, 1) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("acquire on a cancelled context succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquire did not return the cancellation")
	}
}
