package objectstorage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// Packs: a second copy of the chunks a store's head reaches, a few megabytes
// to an object, so a cold read of the head costs a request per pack rather than
// one per chunk.
//
// Every packed chunk is also stored loose. Collection, usage and HasChunk read
// only the loose objects, and every failure on the pack path falls back to
// them, so packs change how fast a read is and nothing else.

// packIndexTTL is how long a process trusts the pack index it last read. A
// stale index costs a read the newest pack, never a wrong answer.
const packIndexTTL = 30 * time.Second

// fetchParallelism bounds how many objects one read fetches at once.
const fetchParallelism = 16

// packReadGap is the largest run of unwanted bytes a pack read reads through
// rather than splitting into two requests.
//
// A request costs a round trip and an operation charge, while bytes read in
// the same region as the bucket cost only transfer time. At tens of
// milliseconds a request and tens of megabytes a second, reading a megabyte
// costs about what one more request does. A full read of a head wants nearly
// every chunk of every pack, so it is one request per pack; a partial read
// wanting a few chunks far apart is not made to read the whole pack.
const packReadGap = 1 << 20

func packPrefix(storeID string) string     { return storePrefix(storeID) + "packs/" }
func packKey(storeID, name string) string  { return packPrefix(storeID) + name }
func packIndexKey(storeID string) string   { return storePrefix(storeID) + "packindex.pb" }
func packHintKey(storeID string) string    { return storePrefix(storeID) + "packhint.pb" }
func packRequestKey(storeID string) string { return storePrefix(storeID) + "packrequest.pb" }

// packLoc is where one chunk sits inside a pack.
type packLoc struct {
	pack   string
	offset int64
	length int64
}

type cachedIndex struct {
	locs   map[protovcs.ChunkID]packLoc
	loaded time.Time
}

// packCache holds each store's pack index, as last read.
type packCache struct {
	mu      sync.Mutex
	byStore map[string]cachedIndex
}

func newPackCache() *packCache { return &packCache{byStore: map[string]cachedIndex{}} }

func (c *packCache) forget(storeID string) {
	c.mu.Lock()
	delete(c.byStore, storeID)
	c.mu.Unlock()
}

// readPackIndex reads a store's pack index and its generation. A store with no
// index has an empty one at generation zero, which is what a first
// compare-and-swap expects.
func (s *Storage) readPackIndex(ctx context.Context, storeID string) (*pb.PackIndex, int64, error) {
	raw, gen, err := s.bucket.Get(ctx, packIndexKey(storeID))
	if errors.Is(err, ErrNoSuchObject) {
		return &pb.PackIndex{}, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	idx := &pb.PackIndex{}
	if err := proto.Unmarshal(raw, idx); err != nil {
		return nil, 0, fmt.Errorf("objectstorage: the pack index of %s does not parse: %w", storeID, err)
	}
	return idx, gen, nil
}

// locsOf maps every chunk in a pack still used for reads to where it sits.
func locsOf(idx *pb.PackIndex) map[protovcs.ChunkID]packLoc {
	out := map[protovcs.ChunkID]packLoc{}
	for _, p := range idx.GetPacks() {
		if p.GetRetiredAt() != nil {
			continue
		}
		for _, m := range p.GetMembers() {
			id, err := protovcs.ChunkIDFromBytes(m.GetChunkId())
			if err != nil {
				continue
			}
			out[id] = packLoc{pack: p.GetName(), offset: int64(m.GetOffset()), length: int64(m.GetLength())}
		}
	}
	return out
}

// packLocs returns where a store's packed chunks are. An index that cannot be
// read means no packs, because every chunk is also loose.
func (s *Storage) packLocs(ctx context.Context, storeID string) map[protovcs.ChunkID]packLoc {
	s.packs.mu.Lock()
	held, ok := s.packs.byStore[storeID]
	s.packs.mu.Unlock()
	if ok && time.Since(held.loaded) < packIndexTTL {
		return held.locs
	}

	idx, _, err := s.readPackIndex(ctx, storeID)
	if err != nil {
		return nil
	}
	locs := locsOf(idx)
	s.packs.mu.Lock()
	s.packs.byStore[storeID] = cachedIndex{locs: locs, loaded: time.Now()}
	s.packs.mu.Unlock()
	return locs
}

// GetChunks returns sealed chunks in the order asked for.
//
// Packed chunks are read with the others from the same pack, as one range
// covering all of them, because Cloud Storage serves one contiguous range per
// request. The rest are read loose. Reads run in parallel, up to
// fetchParallelism at once.
func (s *Storage) GetChunks(ctx context.Context, storeID string, ids []protovcs.ChunkID) ([][]byte, error) {
	got, err := s.fetch(ctx, storeID, ids, false)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = got[id]
	}
	return out, nil
}

// fetch reads chunks from packs where it can and loose otherwise. A chunk
// whose pack read fails is read loose. When skipMissing is set, a chunk that
// is in no pack and not loose is left out rather than failing the read.
func (s *Storage) fetch(ctx context.Context, storeID string, ids []protovcs.ChunkID, skipMissing bool) (map[protovcs.ChunkID][]byte, error) {
	locs := s.packLocs(ctx, storeID)

	byPack := map[string][]protovcs.ChunkID{}
	var loose []protovcs.ChunkID
	for _, id := range ids {
		if loc, ok := locs[id]; ok {
			byPack[loc.pack] = append(byPack[loc.pack], id)
		} else {
			loose = append(loose, id)
		}
	}

	got := make(map[protovcs.ChunkID][]byte, len(ids))
	var mu sync.Mutex
	var jobs []func() error

	for pack, members := range byPack {
		for _, run := range readRuns(members, locs) {
			jobs = append(jobs, s.readRun(ctx, storeID, pack, run, locs, got, &loose, &mu))
		}
	}
	if err := runBounded(jobs, fetchParallelism); err != nil {
		return nil, err
	}
	return s.fetchLoose(ctx, storeID, loose, got, skipMissing)
}

// readRuns splits the chunks wanted from one pack into runs read by one
// request each: sorted by offset, with a new run wherever the gap to the next
// wanted chunk is more than packReadGap.
func readRuns(members []protovcs.ChunkID, locs map[protovcs.ChunkID]packLoc) [][]protovcs.ChunkID {
	sorted := append([]protovcs.ChunkID(nil), members...)
	sort.Slice(sorted, func(i, j int) bool { return locs[sorted[i]].offset < locs[sorted[j]].offset })

	var runs [][]protovcs.ChunkID
	var end int64
	for i, id := range sorted {
		loc := locs[id]
		if i == 0 || loc.offset-end > packReadGap {
			runs = append(runs, nil)
		}
		runs[len(runs)-1] = append(runs[len(runs)-1], id)
		if e := loc.offset + loc.length; e > end {
			end = e
		}
	}
	return runs
}

// readRun returns a job reading one run of chunks from a pack with one range
// request. A pack that is gone or short puts the run's chunks on the loose
// list instead.
func (s *Storage) readRun(ctx context.Context, storeID, pack string, members []protovcs.ChunkID,
	locs map[protovcs.ChunkID]packLoc, got map[protovcs.ChunkID][]byte, loose *[]protovcs.ChunkID, mu *sync.Mutex) func() error {

	return func() error {
		lo, hi := int64(-1), int64(0)
		for _, id := range members {
			loc := locs[id]
			if lo < 0 || loc.offset < lo {
				lo = loc.offset
			}
			if end := loc.offset + loc.length; end > hi {
				hi = end
			}
		}
		span, err := s.bucket.GetRange(ctx, packKey(storeID, pack), lo, hi-lo)

		mu.Lock()
		defer mu.Unlock()
		if err != nil || int64(len(span)) < hi-lo {
			// The index named a pack that is gone or short. Read these
			// loose, and read the index again next time.
			s.packs.forget(storeID)
			*loose = append(*loose, members...)
			return nil
		}
		for _, id := range members {
			loc := locs[id]
			start := loc.offset - lo
			got[id] = span[start : start+loc.length]
		}
		return nil
	}
}

// fetchLoose reads chunks from their loose objects into got.
func (s *Storage) fetchLoose(ctx context.Context, storeID string, loose []protovcs.ChunkID,
	got map[protovcs.ChunkID][]byte, skipMissing bool) (map[protovcs.ChunkID][]byte, error) {

	var mu sync.Mutex
	var jobs []func() error
	for _, id := range loose {
		jobs = append(jobs, func() error {
			sealed, err := s.GetChunk(ctx, storeID, id)
			if skipMissing && errors.Is(err, protovcs.ErrNoSuchChunk) {
				return nil
			}
			if err != nil {
				return err
			}
			mu.Lock()
			got[id] = sealed
			mu.Unlock()
			return nil
		})
	}
	if err := runBounded(jobs, fetchParallelism); err != nil {
		return nil, err
	}
	return got, nil
}

// PutPackHint stores a store's latest hint, replacing any earlier one.
func (s *Storage) PutPackHint(ctx context.Context, storeID string, hint *pb.PackHintRecord) error {
	raw, err := proto.Marshal(hint)
	if err != nil {
		return err
	}
	return s.bucket.Put(ctx, packHintKey(storeID), raw)
}

// ClaimPackRequest records that a pass was asked for at now, unless one was
// asked for less than interval before.
//
// The record is one object written by compare-and-swap, so of several hints
// arriving together one makes the claim and the rest see it.
func (s *Storage) ClaimPackRequest(ctx context.Context, storeID string, now time.Time, interval time.Duration) (bool, error) {
	raw, gen, err := s.bucket.Get(ctx, packRequestKey(storeID))
	switch {
	case errors.Is(err, ErrNoSuchObject):
		gen = 0
	case err != nil:
		return false, err
	default:
		last := &timestamppb.Timestamp{}
		if err := proto.Unmarshal(raw, last); err == nil && now.Sub(last.AsTime()) < interval {
			return false, nil
		}
	}
	next, err := proto.Marshal(timestamppb.New(now))
	if err != nil {
		return false, err
	}
	if _, err := s.bucket.PutIfGeneration(ctx, packRequestKey(storeID), next, gen); err != nil {
		if errors.Is(err, ErrGenerationMismatch) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DropPacks deletes every pack a store has, its index and its hint.
func (s *Storage) DropPacks(ctx context.Context, storeID string) error {
	objects, err := s.bucket.List(ctx, packPrefix(storeID))
	if err != nil {
		return err
	}
	for _, o := range objects {
		if err := s.bucket.Delete(ctx, o.Key); err != nil {
			return err
		}
	}
	for _, key := range []string{packIndexKey(storeID), packRequestKey(storeID)} {
		if err := s.bucket.Delete(ctx, key); err != nil {
			return err
		}
	}
	s.packs.forget(storeID)
	return s.bucket.Delete(ctx, packHintKey(storeID))
}

// Pack runs one pass over a store's packs.
//
// In order: it writes the hinted chunks no pack holds as a delta, if there are
// enough of them; it rewrites packs that are mostly dead; it merges the
// smallest packs while there are too many. New packs are written before the
// index that names them, and the index is replaced by compare-and-swap, so a
// reader sees the old set or the new one. Packs the new index retires are
// deleted by a later pass, once the grace period has passed.
func (s *Storage) Pack(ctx context.Context, storeID string, policy server.PackPolicy) (server.PackResult, error) {
	var res server.PackResult

	hintRaw, _, err := s.bucket.Get(ctx, packHintKey(storeID))
	if errors.Is(err, ErrNoSuchObject) {
		return res, nil
	}
	if err != nil {
		return res, err
	}
	hint := &pb.PackHintRecord{}
	if err := proto.Unmarshal(hintRaw, hint); err != nil {
		return res, fmt.Errorf("objectstorage: the pack hint of %s does not parse: %w", storeID, err)
	}
	position := map[protovcs.ChunkID]int{}
	for _, raw := range hint.GetChunkIds() {
		id, err := protovcs.ChunkIDFromBytes(raw)
		if err != nil {
			continue
		}
		if _, ok := position[id]; !ok {
			position[id] = len(position)
		}
	}

	idx, gen, err := s.readPackIndex(ctx, storeID)
	if err != nil {
		return res, err
	}

	// Sort what the index holds into packs in use, retired packs still inside
	// the grace period, and retired packs to delete.
	var active, retiredKeep []*pb.Pack
	var expired []string
	for _, p := range idx.GetPacks() {
		switch {
		case p.GetRetiredAt() == nil:
			active = append(active, p)
		case policy.Now.Sub(p.GetRetiredAt().AsTime()) >= policy.Grace:
			expired = append(expired, p.GetName())
		default:
			retiredKeep = append(retiredKeep, p)
		}
	}

	packed := map[protovcs.ChunkID]bool{}
	for _, p := range active {
		for _, m := range p.GetMembers() {
			if id, err := protovcs.ChunkIDFromBytes(m.GetChunkId()); err == nil {
				packed[id] = true
			}
		}
	}

	// The delta: hinted chunks no pack holds. Read now, because the threshold
	// is on their bytes as well as their number. A hint naming chunks the store
	// does not hold costs nothing but its own place in the pack.
	var freshIDs []protovcs.ChunkID
	for id := range position {
		if !packed[id] {
			freshIDs = append(freshIDs, id)
		}
	}
	fresh, err := s.fetch(ctx, storeID, freshIDs, true)
	if err != nil {
		return res, err
	}
	var freshBytes uint64
	for _, b := range fresh {
		freshBytes += uint64(len(b))
	}
	buildFresh := len(fresh) > 0 &&
		(len(fresh) >= policy.MinNewChunks || freshBytes >= policy.MinNewBytes)

	// Compaction: a pack whose dead bytes reach the fraction is retired, and
	// its live chunks are written again.
	var stay, retiring []*pb.Pack
	var rebuildIDs []protovcs.ChunkID
	var rebuildBytes uint64
	retire := func(p *pb.Pack) {
		retiring = append(retiring, p)
		for _, m := range p.GetMembers() {
			id, err := protovcs.ChunkIDFromBytes(m.GetChunkId())
			if err != nil {
				continue
			}
			if _, live := position[id]; live {
				rebuildIDs = append(rebuildIDs, id)
				rebuildBytes += m.GetLength()
			}
		}
	}
	for _, p := range active {
		var dead uint64
		for _, m := range p.GetMembers() {
			id, err := protovcs.ChunkIDFromBytes(m.GetChunkId())
			if _, live := position[id]; err != nil || !live {
				dead += m.GetLength()
			}
		}
		if p.GetBytes() > 0 && float64(dead)/float64(p.GetBytes()) >= policy.DeadFraction {
			retire(p)
		} else {
			stay = append(stay, p)
		}
	}

	// Merging: while there would be too many packs, retire the smallest and
	// write its live chunks again.
	writeBytes := rebuildBytes
	if buildFresh || len(retiring) > 0 {
		writeBytes += freshBytes
	}
	sort.Slice(stay, func(i, j int) bool { return stay[i].GetBytes() < stay[j].GetBytes() })
	for len(stay) > 0 && len(stay)+packsFor(writeBytes, policy.PackBytes) > policy.MaxPacks {
		retire(stay[0])
		stay = stay[1:]
		writeBytes = rebuildBytes + freshBytes
	}

	if !buildFresh && len(retiring) == 0 {
		if len(fresh) > 0 {
			res.Skipped = "fewer new chunks than the threshold"
		}
		return res, s.finishPass(ctx, storeID, idx, gen, nil, expired, &res, policy)
	}

	var total uint64
	for _, p := range stay {
		total += p.GetBytes()
	}
	if total+writeBytes > policy.MaxTotalBytes {
		res.Skipped = "packs would exceed the store's allowance"
		return res, s.finishPass(ctx, storeID, idx, gen, nil, expired, &res, policy)
	}

	rebuild, err := s.fetch(ctx, storeID, rebuildIDs, true)
	if err != nil {
		return res, err
	}
	contents := rebuild
	if buildFresh || len(retiring) > 0 {
		for id, b := range fresh {
			contents[id] = b
		}
	}
	order := make([]protovcs.ChunkID, 0, len(contents))
	for id := range contents {
		order = append(order, id)
	}
	sort.Slice(order, func(i, j int) bool { return position[order[i]] < position[order[j]] })

	built, err := s.writePacks(ctx, storeID, order, contents, policy.PackBytes)
	if err != nil {
		return res, err
	}
	res.Built = len(built)
	res.Copied = len(order)

	// A rewritten pack can come out byte for byte the same as one it replaces,
	// and then it is the same object. It stays in use rather than being
	// retired, or deleting the retired entry later would delete it.
	builtNames := map[string]bool{}
	for _, p := range built {
		builtNames[p.GetName()] = true
	}
	retiredAt := timestamppb.New(policy.Now)
	var retired []*pb.Pack
	for _, p := range retiring {
		if builtNames[p.GetName()] {
			continue
		}
		p.RetiredAt = retiredAt
		retired = append(retired, p)
	}
	res.Retired = len(retired)

	next := &pb.PackIndex{}
	next.Packs = append(next.Packs, stay...)
	next.Packs = append(next.Packs, built...)
	next.Packs = append(next.Packs, retired...)
	for _, p := range retiredKeep {
		if !builtNames[p.GetName()] {
			next.Packs = append(next.Packs, p)
		}
	}
	return res, s.finishPass(ctx, storeID, idx, gen, next, expired, &res, policy)
}

// finishPass writes the next index if there is one, deletes the packs it no
// longer names, and deletes pack objects no index ever named.
//
// next is nil when the pass built nothing. The index is still rewritten if it
// held expired packs, so that it stops naming them before they are deleted.
func (s *Storage) finishPass(ctx context.Context, storeID string, idx *pb.PackIndex, gen int64,
	next *pb.PackIndex, expired []string, res *server.PackResult, policy server.PackPolicy) error {

	gone := map[string]bool{}
	for _, name := range expired {
		gone[name] = true
	}
	if next == nil && len(expired) > 0 {
		next = &pb.PackIndex{}
		for _, p := range idx.GetPacks() {
			if !gone[p.GetName()] {
				next.Packs = append(next.Packs, p)
			}
		}
	}
	if next == nil {
		next = idx
	} else {
		raw, err := proto.Marshal(next)
		if err != nil {
			return err
		}
		if _, err := s.bucket.PutIfGeneration(ctx, packIndexKey(storeID), raw, gen); err != nil {
			return fmt.Errorf("objectstorage: replacing the pack index of %s: %w", storeID, err)
		}
		s.packs.forget(storeID)
	}

	named := map[string]bool{}
	for _, p := range next.GetPacks() {
		named[p.GetName()] = true
	}
	objects, err := s.bucket.List(ctx, packPrefix(storeID))
	if err != nil {
		return err
	}
	for _, o := range objects {
		name := strings.TrimPrefix(o.Key, packPrefix(storeID))
		if named[name] {
			continue
		}
		// An unnamed pack is either retired and expired, or left by a pass that
		// stopped before its index write. Either way it is deleted once it is
		// older than the grace period, so a pass running now cannot lose a pack
		// it has written and not yet named.
		if !gone[name] && policy.Now.Sub(o.Updated) < policy.Grace {
			continue
		}
		if err := s.bucket.Delete(ctx, o.Key); err != nil {
			return err
		}
		res.Deleted++
	}
	return nil
}

// writePacks writes chunks, in the order given, as packs of up to max bytes.
func (s *Storage) writePacks(ctx context.Context, storeID string, order []protovcs.ChunkID,
	contents map[protovcs.ChunkID][]byte, max uint64) ([]*pb.Pack, error) {

	var out []*pb.Pack
	var body bytes.Buffer
	var members []*pb.PackMember
	flush := func() error {
		if body.Len() == 0 {
			return nil
		}
		// Every member's bytes are checked against the chunk they claim to be
		// before the pack is written, and the pack is read back after. A pack
		// that fails either is never named by an index, so a wrong offset or a
		// short write costs this pass and not a reader.
		for _, m := range members {
			id, err := protovcs.ChunkIDFromBytes(m.GetChunkId())
			if err != nil {
				return err
			}
			end := m.GetOffset() + m.GetLength()
			if end > uint64(body.Len()) || !bytes.Equal(body.Bytes()[m.GetOffset():end], contents[id]) {
				return fmt.Errorf("objectstorage: pack for %s would hold the wrong bytes for chunk %s", storeID, id)
			}
		}
		sum := sha256.Sum256(body.Bytes())
		name := hex.EncodeToString(sum[:])
		if err := s.bucket.Put(ctx, packKey(storeID, name), body.Bytes()); err != nil {
			return err
		}
		back, _, err := s.bucket.Get(ctx, packKey(storeID, name))
		if err != nil {
			return err
		}
		if !bytes.Equal(back, body.Bytes()) {
			return fmt.Errorf("objectstorage: pack %s for %s did not read back as written", name, storeID)
		}
		out = append(out, &pb.Pack{Name: name, Bytes: uint64(body.Len()), Members: members})
		body.Reset()
		members = nil
		return nil
	}
	for _, id := range order {
		b := contents[id]
		if body.Len() > 0 && uint64(body.Len()+len(b)) > max {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		members = append(members, &pb.PackMember{
			ChunkId: id.Bytes(),
			Offset:  uint64(body.Len()),
			Length:  uint64(len(b)),
		})
		body.Write(b)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

// packsFor is how many packs of max bytes hold n bytes.
func packsFor(n, max uint64) int {
	if n == 0 {
		return 0
	}
	return int((n + max - 1) / max)
}

// runBounded runs jobs, at most limit at once, and returns the first error.
func runBounded(jobs []func() error, limit int) error {
	sem := make(chan struct{}, limit)
	errs := make(chan error, len(jobs))
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(job func() error) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := job(); err != nil {
				errs <- err
			}
		}(job)
	}
	wg.Wait()
	close(errs)
	return <-errs
}
