package server

import (
	"context"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// Transport returns a client transport that calls this server directly.
//
// It is what the service handler is a thin wrapper around, and it lets a test
// exercise a real client against the real rules — quotas, reservations,
// compare-and-swap, and a commit that names a chunk nobody stored — without a
// network in the way. A test that only ever runs against a permissive fake
// proves nothing about production.
func (s *Server) Transport(storeID string) protovcs.Transport {
	return &directTransport{server: s, storeID: storeID}
}

type directTransport struct {
	server  *Server
	storeID string
}

func (t *directTransport) ListRefs(ctx context.Context) ([]protovcs.RefState, error) {
	return t.server.ListRefs(ctx, t.storeID)
}

func (t *directTransport) WatchRefs(ctx context.Context, deliver func([]protovcs.RefState) error) error {
	return t.server.WatchRefs(ctx, t.storeID, deliver)
}

func (t *directTransport) UpdateRef(ctx context.Context, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error) {
	return t.server.UpdateRef(ctx, t.storeID, name, commit, expected, force)
}

func (t *directTransport) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	return t.server.GetCommits(ctx, t.storeID, from, depth)
}

func (t *directTransport) PutCommit(ctx context.Context, commit *pb.Commit, reservation string) (protovcs.CommitID, error) {
	return t.server.PutCommit(ctx, t.storeID, reservation, commit)
}

func (t *directTransport) MissingBetween(ctx context.Context, have []protovcs.CommitID, want protovcs.CommitID) ([]protovcs.ChunkID, bool, error) {
	return t.server.MissingBetween(ctx, t.storeID, have, want)
}

func (t *directTransport) BeginWrite(ctx context.Context, chunks uint32, bytes uint64) (string, error) {
	r, err := t.server.BeginWrite(ctx, t.storeID, chunks, bytes)
	if err != nil {
		return "", err
	}
	return r.ID, nil
}

func (t *directTransport) HasChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.ChunkID, error) {
	return t.server.HasChunks(ctx, t.storeID, ids)
}

func (t *directTransport) PutChunks(ctx context.Context, reservation string, chunks []protovcs.SealedChunk) error {
	return t.server.PutChunks(ctx, t.storeID, reservation, chunks)
}

func (t *directTransport) GetChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	return t.server.GetChunks(ctx, t.storeID, ids)
}

func (t *directTransport) PackHint(ctx context.Context, commit protovcs.CommitID, ids []protovcs.ChunkID) (bool, error) {
	return t.server.PackHint(ctx, t.storeID, commit, ids)
}
