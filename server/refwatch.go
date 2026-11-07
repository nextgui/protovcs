package server

import (
	"context"
	"sync"
	"time"

	protovcs "github.com/nextgui/protovcs"
)

// RefPollInterval is how often a store with at least one watcher is checked
// for a change.
//
// Storage is a directory or a bucket and neither tells anybody it changed, so a
// server that wants to know when a branch moved has to look. A check reads the
// generation of the store's record, which is one metadata read on a bucket, and
// lists the refs only when the generation moved. A watched store nobody writes
// to therefore costs a metadata read every ten seconds, about $0.10 a month on
// Cloud Storage, where listing every two seconds cost about $6.50.
//
// A move this server performed does not wait for the interval: UpdateRef wakes
// the poller. The interval is what covers a move performed by another replica,
// which nothing in this process saw, so it bounds how late such a move can be.
const RefPollInterval = 10 * time.Second

// RefRelistInterval is how often a watched store's refs are listed whether or
// not its record's generation moved.
//
// A check trusts that every move is followed by a write of the store's record.
// UpdateRef moves the ref and then writes the record, so a server that stops
// between the two leaves a move the generation never shows. This listing is
// what eventually reports it. It costs about $0.04 a month per watched store.
const RefRelistInterval = 5 * time.Minute

// WithRefPollInterval replaces how often a watched store is checked, for tests
// that would otherwise spend seconds waiting for a tick.
func (s *Server) WithRefPollInterval(d time.Duration) *Server {
	s.refPollInterval = d
	return s
}

// WithRefRelistInterval replaces how often a watched store is listed
// unconditionally, for tests of a move the generation does not show.
func (s *Server) WithRefRelistInterval(d time.Duration) *Server {
	s.refRelistInterval = d
	return s
}

// WatchStats is what a server is spending on watchers.
//
// Exists so a test can assert that a disconnected watcher released what it
// held, rather than assuming it. All three fields return to zero once the last
// watcher of every store has gone.
type WatchStats struct {
	// Stores with at least one watcher, which is the number of stores being
	// polled. N watchers of one store cost one poll, not N.
	Stores int

	// Subscriptions across every store.
	Watchers int

	// Poller goroutines that have not yet returned. Falls to zero slightly
	// after Stores does, because a poller is asked to stop and then stops.
	Pollers int
}

// WatchStats reports what this server is spending on watchers.
func (s *Server) WatchStats() WatchStats {
	s.watch.mu.Lock()
	defer s.watch.mu.Unlock()

	out := WatchStats{Stores: len(s.watch.stores), Pollers: s.watch.pollers}
	for _, sw := range s.watch.stores {
		out.Watchers += len(sw.subs)
	}
	return out
}

// WatchRefs calls deliver with the store's branches, and again every time one
// of them moves, until ctx is done or deliver returns an error.
//
// The first call to deliver carries the current state rather than waiting for a
// change. A watcher therefore needs no separate ListRefs, and cannot miss a
// move that lands between the two calls it would otherwise have made.
//
// deliver receives the whole ref set every time, not what changed. That is what
// lets a slow watcher be sent only the newest state instead of a queue of
// intermediate ones, and it means a repeated value is not an event: the server
// may deliver the same set twice and a caller that acts on the difference sees
// nothing.
//
// Returns ctx.Err() when the caller stopped, deliver's error when deliver
// stopped, and a storage error when the poll behind it failed. A failed poll
// ends every watch on that store rather than being retried silently, because a
// watch that has quietly stopped reporting looks exactly like a store nobody is
// writing to.
func (s *Server) WatchRefs(ctx context.Context, storeID string, deliver func([]protovcs.RefState) error) error {
	if _, err := s.storage.GetStore(ctx, storeID); err != nil {
		return err
	}

	sub := s.watch.subscribe(s, storeID)
	defer s.watch.unsubscribe(sub)

	// Read after subscribing, never before. Reading first would leave a gap
	// between the read and the subscription, and a move landing in it would be
	// reported by neither. Subscribing first can only produce a duplicate,
	// which the comparison below drops.
	refs, err := s.storage.ListRefs(ctx, storeID)
	if err != nil {
		return err
	}
	if err := deliver(refs); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case next, ok := <-sub.updates:
			if !ok {
				return sub.owner.reason()
			}
			if protovcs.SameRefs(refs, next) {
				continue
			}
			refs = next
			if err := deliver(refs); err != nil {
				return err
			}
		}
	}
}

// refWatchers is every open watch on this server, grouped by store.
//
// One mutex covers all of it. Everything under it is a map operation or a
// non-blocking channel send; the storage read a poll does happens outside.
type refWatchers struct {
	mu      sync.Mutex
	stores  map[string]*storeWatchers
	pollers int
}

func newRefWatchers() *refWatchers {
	return &refWatchers{stores: map[string]*storeWatchers{}}
}

// storeWatchers is one store's poller and the subscriptions it feeds.
type storeWatchers struct {
	storeID string

	// Stops the poller. Called when the last subscription goes.
	cancel context.CancelFunc

	// Buffered by one. A wake that arrives while one is already pending is
	// dropped, because the poller reads current state rather than a queue of
	// events.
	wake chan struct{}

	nextID int64
	subs   map[int64]*refSub

	// The last set published, so a poll that found nothing new wakes nobody.
	last     []protovcs.RefState
	haveLast bool

	// Why the poller stopped, read by a subscriber whose channel was closed.
	// Guarded by refWatchers.mu.
	err error
}

// reason is why the poller closed this store's subscriptions.
func (sw *storeWatchers) reason() error { return sw.err }

// refSub is one open watch.
type refSub struct {
	id      int64
	owner   *storeWatchers
	updates chan []protovcs.RefState
}

// subscribe registers a watch, starting the store's poller if it is the first.
func (w *refWatchers) subscribe(s *Server, storeID string) *refSub {
	w.mu.Lock()
	defer w.mu.Unlock()

	sw := w.stores[storeID]
	if sw == nil {
		ctx, cancel := context.WithCancel(context.Background())
		sw = &storeWatchers{
			storeID: storeID,
			cancel:  cancel,
			wake:    make(chan struct{}, 1),
			subs:    map[int64]*refSub{},
		}
		w.stores[storeID] = sw
		w.pollers++
		go w.poll(ctx, s, sw)
	}

	sub := &refSub{
		id:    sw.nextID,
		owner: sw,
		// Buffered by one and overwritten rather than queued. A watcher that
		// is not reading holds one ref set, whatever the store's write rate.
		updates: make(chan []protovcs.RefState, 1),
	}
	sw.nextID++
	sw.subs[sub.id] = sub
	return sub
}

// unsubscribe releases a watch, stopping the store's poller if it was the last.
//
// Safe to call after the poller has already closed the subscription, which is
// what happens when a poll fails.
func (w *refWatchers) unsubscribe(sub *refSub) {
	w.mu.Lock()
	defer w.mu.Unlock()

	sw := sub.owner
	delete(sw.subs, sub.id)
	if len(sw.subs) > 0 {
		return
	}
	// Identity rather than presence: a poller that has already exited may have
	// been replaced by a newer one for the same store.
	if w.stores[sw.storeID] == sw {
		delete(w.stores, sw.storeID)
	}
	sw.cancel()
}

// wake asks a store's poller to look now rather than at the next tick.
//
// Called after this server moved a ref, so a client watching through the same
// process is told immediately. A store nobody is watching has no poller and
// this does nothing.
func (w *refWatchers) wake(storeID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	sw := w.stores[storeID]
	if sw == nil {
		return
	}
	select {
	case sw.wake <- struct{}{}:
	default:
	}
}

// poll checks a store on a timer, or when something wakes it, and publishes
// what changed.
//
// The generation is read before the refs, never after. A move between the two
// reads then shows up as a generation the next check has not seen, and is
// listed then. Reading it after would record a generation whose move the
// listing had missed, and nothing would list again until the next write.
func (w *refWatchers) poll(ctx context.Context, s *Server, sw *storeWatchers) {
	ticker := time.NewTicker(s.refPollInterval)
	defer ticker.Stop()
	relist := time.NewTicker(s.refRelistInterval)
	defer relist.Stop()

	// Unknown until the first check, which therefore always lists. The
	// subscriber's own first listing may have happened before this goroutine
	// read anything, so a generation read now cannot vouch for it.
	var seen int64
	known := false

	for {
		always := true
		select {
		case <-ctx.Done():
			w.stop(sw, nil)
			return
		case <-sw.wake:
		case <-relist.C:
		case <-ticker.C:
			always = false
		}

		gen, err := s.storage.StoreGeneration(ctx, sw.storeID)
		if err == nil && (always || !known || gen != seen) {
			seen, known = gen, true
			var refs []protovcs.RefState
			if refs, err = s.storage.ListRefs(ctx, sw.storeID); err == nil {
				w.publish(sw, refs)
			}
		}
		if err != nil {
			// A cancelled context is the watchers leaving, not a failure.
			if ctx.Err() != nil {
				err = nil
			}
			w.stop(sw, err)
			return
		}
	}
}

// publish hands a new ref set to every subscription, dropping a state a slow
// watcher has not read yet.
func (w *refWatchers) publish(sw *storeWatchers, refs []protovcs.RefState) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if sw.haveLast && protovcs.SameRefs(sw.last, refs) {
		return
	}
	sw.last, sw.haveLast = refs, true

	for _, sub := range sw.subs {
		// Take back whatever is waiting before putting the newer state in.
		// Only this goroutine sends, so the slot cannot refill in between.
		select {
		case <-sub.updates:
		default:
		}
		select {
		case sub.updates <- refs:
		default:
		}
	}
}

// stop retires a poller and ends every watch it was feeding.
func (w *refWatchers) stop(sw *storeWatchers, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stores[sw.storeID] == sw {
		delete(w.stores, sw.storeID)
	}
	sw.err = err
	for id, sub := range sw.subs {
		close(sub.updates)
		delete(sw.subs, id)
	}
	w.pollers--
}
