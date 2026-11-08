package server

import (
	"fmt"
	"time"
	_ "time/tzdata" // quotaZone must load without the host's zone files

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// account applies a store's quota to its usage. Every check against a
// store's Quota and every change to its Usage goes through one, inside the
// storage update that writes the store's record, so a check and the change it
// allows land together.
//
// Usage is held in two kinds of counter. Bytes, objects, commits and refs are
// what the store holds. Reserved bytes and objects are what writes in flight
// have been promised, and count as used, so two writers are never both told
// there is room for the same last bytes.
type account struct {
	quota *pb.Quota
	usage *pb.Usage
	now   time.Time
}

// accountOf returns the account of a store record that is being updated. It
// gives the record a Usage if it has none, because the methods write to it.
func accountOf(st *pb.Store, now time.Time) account {
	if st.Usage == nil {
		st.Usage = &pb.Usage{}
	}
	return account{quota: st.GetQuota(), usage: st.Usage, now: now}
}

// reserve promises a write room for chunks chunks of bytes sealed bytes in
// all, or fails with ErrQuotaExceeded.
func (a account) reserve(chunks uint32, bytes uint64) error {
	q, u := a.quota, a.usage
	if u.GetReservations() >= MaxReservations {
		return fmt.Errorf("%w: the store has %d reservations open, and at most %d may be",
			ErrQuotaExceeded, u.GetReservations(), MaxReservations)
	}
	if max := q.GetMaxBytes(); max > 0 {
		if exceeds(max, addSat(u.GetBytes(), u.GetReservedBytes()), bytes) {
			return fmt.Errorf("%w: max_bytes is %d, using %d, this write needs %d",
				ErrQuotaExceeded, max, u.GetBytes()+u.GetReservedBytes(), bytes)
		}
	}
	if max := q.GetMaxObjects(); max > 0 {
		if exceeds(max, addSat(u.GetObjects(), u.GetReservedObjects()), uint64(chunks)) {
			return fmt.Errorf("%w: max_objects is %d, holding %d, this write needs %d",
				ErrQuotaExceeded, max, u.GetObjects()+u.GetReservedObjects(), chunks)
		}
	}
	if max := q.GetMaxBytesPerDay(); max > 0 {
		today := a.bytesToday()
		if exceeds(max, today, bytes) {
			return fmt.Errorf("%w: max_bytes_per_day is %d, written %d today, this write needs %d",
				ErrQuotaExceeded, max, today, bytes)
		}
	}
	u.ReservedObjects = addSat(u.ReservedObjects, uint64(chunks))
	u.ReservedBytes = addSat(u.ReservedBytes, bytes)
	u.Reservations++
	return nil
}

// release returns what a reservation was promised.
func (a account) release(r *Reservation) {
	a.usage.ReservedObjects = saturatingSub(a.usage.ReservedObjects, uint64(r.Chunks))
	a.usage.ReservedBytes = saturatingSub(a.usage.ReservedBytes, r.Bytes)
	if a.usage.Reservations > 0 {
		a.usage.Reservations--
	}
}

// storeChunks counts chunks that have been written. Their reservation already
// checked the limits.
func (a account) storeChunks(n uint32, bytes uint64) {
	a.usage.Objects = addSat(a.usage.Objects, uint64(n))
	a.usage.Bytes = addSat(a.usage.Bytes, bytes)
	a.addToday(bytes)
}

// storeCommit counts a commit record of size bytes, or fails with
// ErrQuotaExceeded. Commits are never collected and their payloads are
// opaque, so a commit is charged against the byte limits like a chunk.
// Otherwise a write token could store anything as commit payloads.
func (a account) storeCommit(size uint64) error {
	q, u := a.quota, a.usage
	if max := q.GetMaxCommits(); max > 0 && exceeds(max, u.GetCommits(), 1) {
		return fmt.Errorf("%w: max_commits is %d", ErrQuotaExceeded, max)
	}
	if max := q.GetMaxBytes(); max > 0 {
		if exceeds(max, addSat(u.GetBytes(), u.GetReservedBytes()), size) {
			return fmt.Errorf("%w: max_bytes is %d, using %d, this commit is %d",
				ErrQuotaExceeded, max, u.GetBytes()+u.GetReservedBytes(), size)
		}
	}
	if max := q.GetMaxBytesPerDay(); max > 0 {
		today := a.bytesToday()
		if exceeds(max, today, size) {
			return fmt.Errorf("%w: max_bytes_per_day is %d, written %d today, this commit is %d",
				ErrQuotaExceeded, max, today, size)
		}
	}
	u.Commits = addSat(u.Commits, 1)
	u.Bytes = addSat(u.Bytes, size)
	a.addToday(size)
	return nil
}

// unstoreCommit takes back a storeCommit whose record was not written.
// chargedAt is when storeCommit ran. Only that day's count gets the bytes
// back, because past midnight the count has already started again.
func (a account) unstoreCommit(size uint64, chargedAt time.Time) {
	u := a.usage
	u.Commits = saturatingSub(u.Commits, 1)
	u.Bytes = saturatingSub(u.Bytes, size)
	if u.BytesTodayDay == quotaDay(chargedAt) {
		u.BytesToday = saturatingSub(u.BytesToday, size)
	}
}

// deleteChunks counts chunks that collection removed. The day's count is
// left alone: it limits what is written, not what is held.
func (a account) deleteChunks(n uint64, bytes uint64) {
	a.usage.Objects = saturatingSub(a.usage.Objects, n)
	a.usage.Bytes = saturatingSub(a.usage.Bytes, bytes)
}

// setRefs records how many refs the store holds.
func (a account) setRefs(n uint32) {
	a.usage.Refs = n
}

// recount replaces the held and reserved counters with ones counted from
// storage. The day's count is kept, because storage does not record when
// each object was written.
func (a account) recount(counted *pb.Usage) {
	u := a.usage
	u.Objects = counted.GetObjects()
	u.Bytes = counted.GetBytes()
	u.Commits = counted.GetCommits()
	u.Refs = counted.GetRefs()
	u.ReservedObjects = counted.GetReservedObjects()
	u.ReservedBytes = counted.GetReservedBytes()
	u.Reservations = counted.GetReservations()
}

// bytesToday is what the usage says was written on the account's day. A
// count recorded for an earlier day is zero today.
//
// Nothing resets the counter at midnight. The sweep runs rarely, so the day
// is checked wherever the counter is read or added to.
func (a account) bytesToday() uint64 {
	if a.usage.GetBytesTodayDay() != quotaDay(a.now) {
		return 0
	}
	return a.usage.GetBytesToday()
}

func (a account) addToday(bytes uint64) {
	a.usage.BytesToday = addSat(a.bytesToday(), bytes)
	a.usage.BytesTodayDay = quotaDay(a.now)
}

// checkObjectSize refuses one chunk or commit record over the store's
// max_chunk_bytes. what names it in the error, as "chunk <id>".
func checkObjectSize(q *pb.Quota, what string, size uint64) error {
	if max := q.GetMaxChunkBytes(); max > 0 && size > max {
		return fmt.Errorf("%w: max_chunk_bytes is %d, %s is %d", ErrQuotaExceeded, max, what, size)
	}
	return nil
}

// checkNewRef refuses a new ref when the store already holds refs refs and
// that is its max_refs.
func checkNewRef(q *pb.Quota, refs int) error {
	if max := q.GetMaxRefs(); max > 0 && uint64(refs) >= uint64(max) {
		return fmt.Errorf("%w: max_refs is %d and the store has %d", ErrQuotaExceeded, max, refs)
	}
	return nil
}

// quotaZone is the time zone whose midnight starts a new day for
// max_bytes_per_day.
var quotaZone = mustLoadLocation("America/Los_Angeles")

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(fmt.Sprintf("protovcs: loading time zone %s: %v", name, err))
	}
	return loc
}

// quotaDay is the calendar day t falls on in quotaZone, as days since
// 1970-01-01.
//
// It counts calendar dates rather than dividing elapsed seconds, because a day
// in Los Angeles is 23 or 25 hours long when daylight saving starts or ends.
func quotaDay(t time.Time) int64 {
	y, m, d := t.In(quotaZone).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400
}

// exceeds reports whether used plus more is over max, without overflowing.
func exceeds(max, used, more uint64) bool {
	return used > max || more > max-used
}

// addSat adds two counters, saturating rather than wrapping.
func addSat(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}

func saturatingSub(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}
