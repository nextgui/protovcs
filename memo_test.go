package protovcs

import (
	"context"
	"errors"
	"testing"
)

// TestAChunkBeingFetchedIsWaitedForNotFetchedAgain checks that a second read
// wanting a chunk another read is already fetching waits for that fetch rather
// than sending its own.
func TestAChunkBeingFetchedIsWaitedForNotFetchedAgain(t *testing.T) {
	ctx := context.Background()
	m := newMemo()
	id := ChunkID{1}

	started, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- m.warm(ctx, []ChunkID{id}, func(want []ChunkID) (map[ChunkID][]byte, error) {
			close(started)
			<-release
			return map[ChunkID][]byte{id: []byte("plain")}, nil
		})
	}()
	<-started

	second := make(chan error, 1)
	go func() {
		second <- m.warm(ctx, []ChunkID{id}, func(want []ChunkID) (map[ChunkID][]byte, error) {
			t.Errorf("the second read fetched %v while the first was fetching it", want)
			return nil, nil
		})
	}()
	close(release)

	if err := <-first; err != nil {
		t.Fatalf("first warm: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second warm: %v", err)
	}
	if got, ok := m.chunk(id); !ok || string(got) != "plain" {
		t.Fatalf("chunk = %q, %v; want the first read's plaintext", got, ok)
	}
}

// TestAFailedFetchIsRetriedByTheReadWaitingOnIt checks that a read waiting on
// another read's fetch is not failed by that fetch's error, and fetches the
// chunk itself instead.
func TestAFailedFetchIsRetriedByTheReadWaitingOnIt(t *testing.T) {
	ctx := context.Background()
	m := newMemo()
	id := ChunkID{2}

	started, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- m.warm(ctx, []ChunkID{id}, func([]ChunkID) (map[ChunkID][]byte, error) {
			close(started)
			<-release
			return nil, context.Canceled
		})
	}()
	<-started

	fetched := 0
	second := make(chan error, 1)
	go func() {
		second <- m.warm(ctx, []ChunkID{id}, func([]ChunkID) (map[ChunkID][]byte, error) {
			fetched++
			return map[ChunkID][]byte{id: []byte("plain")}, nil
		})
	}()
	close(release)

	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first warm = %v, want its own error", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second warm: %v", err)
	}
	if fetched != 1 {
		t.Fatalf("the waiting read fetched %d times, want 1", fetched)
	}
}
