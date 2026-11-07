package server_test

import (
	"context"
	"errors"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// The server reads nothing it stores. These are the mistakes it can still catch
// from the plaintext parts of a commit, and each one is a way a client could
// destroy a store without anything failing.

// commitOn stores one commit with the given parent and schema version, and
// returns its id.
func commitOn(t *testing.T, s *server.Server, parent protovcs.CommitID, schema uint32, chunkByte byte) protovcs.CommitID {
	t.Helper()
	ctx := context.Background()

	r, err := s.BeginWrite(ctx, storeID, 4, 4000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	c := chunk(chunkByte, 32)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{c}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	commit := &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		SchemaVersion: schema,
		Added:         [][]byte{c.ID.Bytes()},
		NamingNonce:   []byte{chunkByte},
	}
	if !parent.IsZero() {
		commit.Parents = [][]byte{parent.Bytes()}
	}

	id, err := s.PutCommit(ctx, storeID, r.ID, commit)
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	return id
}

// TestABranchCannotLeaveItsOwnHistoryByAccident is the guard against the most
// destructive client mistake: pointing a branch at unrelated history orphans
// everything in between, and the chunks only that history reached become
// collectable. Nothing about the write fails.
func TestABranchCannotLeaveItsOwnHistoryByAccident(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	first := commitOn(t, s, protovcs.CommitID{}, 1, 1)
	state, err := s.UpdateRef(ctx, storeID, []byte("main"), first, "", false)
	if err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	second := commitOn(t, s, first, 1, 2)
	state, err = s.UpdateRef(ctx, storeID, []byte("main"), second, state.CASToken, false)
	if err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	// A commit on a different root, which is what a client resetting a branch
	// or writing from stale state produces.
	unrelated := commitOn(t, s, protovcs.CommitID{}, 1, 3)

	if _, err := s.UpdateRef(ctx, storeID, []byte("main"), unrelated, state.CASToken, false); !errors.Is(err, protovcs.ErrNotFastForward) {
		t.Fatalf("moving a branch off its history returned %v, want ErrNotFastForward", err)
	}

	// The branch did not move.
	refs, err := s.ListRefs(ctx, storeID)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if refs[0].Commit != second {
		t.Fatal("a refused move changed the branch anyway")
	}
}

func TestForceAllowsLeavingHistoryDeliberately(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	first := commitOn(t, s, protovcs.CommitID{}, 1, 1)
	state, err := s.UpdateRef(ctx, storeID, []byte("main"), first, "", false)
	if err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	// Truncating history and rolling back are the operations that legitimately
	// do this, and they say so rather than being indistinguishable from a
	// mistake.
	unrelated := commitOn(t, s, protovcs.CommitID{}, 1, 2)
	if _, err := s.UpdateRef(ctx, storeID, []byte("main"), unrelated, state.CASToken, true); err != nil {
		t.Fatalf("a forced move was refused: %v", err)
	}
}

func TestALostRaceIsNotReportedAsANonFastForward(t *testing.T) {
	// A writer that lost a race retries; a writer that left its own history
	// gives up. Reporting the first as the second makes an ordinary collision
	// look like a fatal mistake.
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	first := commitOn(t, s, protovcs.CommitID{}, 1, 1)
	state, err := s.UpdateRef(ctx, storeID, []byte("main"), first, "", false)
	if err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	second := commitOn(t, s, first, 1, 2)
	if _, err := s.UpdateRef(ctx, storeID, []byte("main"), second, state.CASToken, false); err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	// A third writer still holding the first token, with a commit that would
	// have been a fine fast-forward from where it read.
	third := commitOn(t, s, first, 1, 3)
	if _, err := s.UpdateRef(ctx, storeID, []byte("main"), third, state.CASToken, false); !errors.Is(err, protovcs.ErrRaceLost) {
		t.Fatalf("a stale token returned %v, want ErrRaceLost", err)
	}
}

// TestAnOlderSchemaCannotBeWrittenOverANewerOne is the guard against a client
// that should have refused and did not. An old build left running for a year is
// the case it exists for.
func TestAnOlderSchemaCannotBeWrittenOverANewerOne(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	migrated := commitOn(t, s, protovcs.CommitID{}, 3, 1)

	ctx := context.Background()
	r, err := s.BeginWrite(ctx, storeID, 4, 4000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	c := chunk(9, 32)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{c}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	_, err = s.PutCommit(ctx, storeID, r.ID, &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		SchemaVersion: 2,
		Parents:       [][]byte{migrated.Bytes()},
		Added:         [][]byte{c.ID.Bytes()},
	})
	if !errors.Is(err, server.ErrSchemaTooOld) {
		t.Fatalf("writing schema 2 onto schema 3 returned %v, want ErrSchemaTooOld", err)
	}
}

func TestAnOperatorCanLockOutOldClients(t *testing.T) {
	ctx := context.Background()
	s, storage := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	// Set once every client has migrated. Until then this is zero and anything
	// is accepted.
	if _, err := storage.Update(ctx, storeID, func(st *pb.Store) error {
		st.MinWritableSchemaVersion = 3
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	r, err := s.BeginWrite(ctx, storeID, 4, 4000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	c := chunk(5, 32)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{c}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	_, err = s.PutCommit(ctx, storeID, r.ID, &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		SchemaVersion: 2,
		Added:         [][]byte{c.ID.Bytes()},
	})
	if !errors.Is(err, server.ErrSchemaTooOld) {
		t.Fatalf("writing below the floor returned %v, want ErrSchemaTooOld", err)
	}

	// At or above the floor is accepted.
	if _, err := s.PutCommit(ctx, storeID, r.ID, &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		SchemaVersion: 3,
		Added:         [][]byte{c.ID.Bytes()},
	}); err != nil {
		t.Fatalf("writing at the floor was refused: %v", err)
	}
}

func TestASchemaMayMoveForward(t *testing.T) {
	// The guard must not block a migration, which is the whole point of having
	// versions.
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	first := commitOn(t, s, protovcs.CommitID{}, 1, 1)
	if id := commitOn(t, s, first, 2, 2); id.IsZero() {
		t.Fatal("a commit moving the schema forward was refused")
	}
}
