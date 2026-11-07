package protovcs

import (
	"context"

	"google.golang.org/protobuf/proto"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// PublishWithParentsForTest writes m to the branch as a commit with the given
// parents, so a test can build a merge or a branch that forks from another.
// Nothing outside the tests writes a commit with chosen parents.
func PublishWithParentsForTest(ctx context.Context, b *Branch, meta CommitMeta, parents []CommitID, m proto.Message) (CommitID, error) {
	_, state, err := b.Head(ctx)
	if err != nil && !isNoSuchRef(err) {
		return CommitID{}, err
	}
	if isNoSuchRef(err) {
		state = RefState{}
	}
	commits := make([]*pb.Commit, 0, len(parents))
	for _, id := range parents {
		c, err := b.store.commitByID(ctx, id)
		if err != nil {
			return CommitID{}, err
		}
		commits = append(commits, c)
	}
	refs, pending, err := b.store.itemRefs(m)
	if err != nil {
		return CommitID{}, err
	}
	return b.publishRefs(ctx, meta, commits, state, refs, pending)
}
