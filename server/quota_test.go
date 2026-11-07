package server

import (
	"errors"
	"math"
	"testing"
	"time"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// noon is mid-day in quotaZone, so an hour either side is the same day.
var noon = time.Date(2026, 11, 6, 12, 0, 0, 0, quotaZone)

func storeWith(q *pb.Quota) *pb.Store { return &pb.Store{Quota: q} }

func TestAReservationCountsAsUsedUntilReleased(t *testing.T) {
	st := storeWith(&pb.Quota{MaxBytes: 100})
	if err := accountOf(st, noon).reserve(1, 60); err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	if err := accountOf(st, noon).reserve(1, 60); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("second reservation past max_bytes = %v, want ErrQuotaExceeded", err)
	}
	accountOf(st, noon).release(&Reservation{Chunks: 1, Bytes: 60})
	if err := accountOf(st, noon).reserve(1, 60); err != nil {
		t.Fatalf("reservation after the release: %v", err)
	}
}

func TestARefusedReservationChangesNothing(t *testing.T) {
	st := storeWith(&pb.Quota{MaxObjects: 1})
	if err := accountOf(st, noon).reserve(2, 10); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("reserve past max_objects = %v, want ErrQuotaExceeded", err)
	}
	if u := st.GetUsage(); u.GetReservedObjects() != 0 || u.GetReservedBytes() != 0 {
		t.Errorf("usage after a refusal = %v, want nothing reserved", u)
	}
}

func TestTheDailyCountStartsAgainAtMidnight(t *testing.T) {
	st := storeWith(&pb.Quota{MaxBytesPerDay: 100})
	accountOf(st, noon).storeChunks(1, 80)
	if err := accountOf(st, noon).reserve(1, 30); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("same-day reservation past max_bytes_per_day = %v, want ErrQuotaExceeded", err)
	}
	if err := accountOf(st, noon.AddDate(0, 0, 1)).reserve(1, 30); err != nil {
		t.Fatalf("next-day reservation: %v", err)
	}
}

func TestACommitRefundReturnsTheDayItWasChargedTo(t *testing.T) {
	st := storeWith(&pb.Quota{MaxBytesPerDay: 1000})
	if err := accountOf(st, noon).storeCommit(40); err != nil {
		t.Fatal(err)
	}
	accountOf(st, noon).unstoreCommit(40, noon)
	if u := st.GetUsage(); u.GetCommits() != 0 || u.GetBytes() != 0 || u.GetBytesToday() != 0 {
		t.Errorf("usage after a same-day refund = %v, want zero", u)
	}

	if err := accountOf(st, noon).storeCommit(40); err != nil {
		t.Fatal(err)
	}
	tomorrow := noon.AddDate(0, 0, 1)
	accountOf(st, tomorrow).storeChunks(1, 5)
	accountOf(st, tomorrow).unstoreCommit(40, noon)
	if got := st.GetUsage().GetBytesToday(); got != 5 {
		t.Errorf("today's count after refunding yesterday's commit = %d, want 5", got)
	}
}

func TestCommitsAreLimitedByCountAndBytes(t *testing.T) {
	st := storeWith(&pb.Quota{MaxCommits: 1, MaxBytes: 100})
	if err := accountOf(st, noon).storeCommit(10); err != nil {
		t.Fatal(err)
	}
	if err := accountOf(st, noon).storeCommit(10); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("commit past max_commits = %v, want ErrQuotaExceeded", err)
	}

	st = storeWith(&pb.Quota{MaxBytes: 100})
	if err := accountOf(st, noon).reserve(1, 95); err != nil {
		t.Fatal(err)
	}
	if err := accountOf(st, noon).storeCommit(10); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("commit past max_bytes with 95 reserved = %v, want ErrQuotaExceeded", err)
	}
}

func TestCountersNeitherWrapNorGoNegative(t *testing.T) {
	st := storeWith(&pb.Quota{MaxBytes: math.MaxUint64})
	st.Usage = &pb.Usage{Bytes: math.MaxUint64 - 1}
	if err := accountOf(st, noon).reserve(0, 10); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("reserve that would overflow = %v, want ErrQuotaExceeded", err)
	}
	accountOf(st, noon).storeChunks(1, 10)
	if got := st.GetUsage().GetBytes(); got != math.MaxUint64 {
		t.Errorf("bytes after an overflowing add = %d, want the maximum", got)
	}

	st = storeWith(nil)
	accountOf(st, noon).release(&Reservation{Chunks: 3, Bytes: 30})
	accountOf(st, noon).deleteChunks(3, 30)
	accountOf(st, noon).unstoreCommit(30, noon)
	if u := st.GetUsage(); u.GetReservedBytes() != 0 || u.GetObjects() != 0 || u.GetBytes() != 0 || u.GetCommits() != 0 {
		t.Errorf("usage after subtracting from zero = %v, want zero", u)
	}
}

func TestARecountKeepsTheDailyCount(t *testing.T) {
	st := storeWith(nil)
	accountOf(st, noon).storeChunks(2, 50)
	accountOf(st, noon).recount(&pb.Usage{Objects: 1, Bytes: 20, Commits: 3, Refs: 1, ReservedBytes: 7})
	u := st.GetUsage()
	if u.GetObjects() != 1 || u.GetBytes() != 20 || u.GetCommits() != 3 || u.GetRefs() != 1 || u.GetReservedBytes() != 7 {
		t.Errorf("usage after a recount = %v", u)
	}
	if u.GetBytesToday() != 50 {
		t.Errorf("bytes_today after a recount = %d, want 50", u.GetBytesToday())
	}
}

func TestZeroLimitsAreUnlimited(t *testing.T) {
	st := storeWith(&pb.Quota{})
	if err := accountOf(st, noon).reserve(math.MaxUint32, math.MaxUint64); err != nil {
		t.Errorf("reserve with no limits: %v", err)
	}
	if err := checkObjectSize(nil, "chunk x", math.MaxUint64); err != nil {
		t.Errorf("object size with no limit: %v", err)
	}
	if err := checkNewRef(nil, math.MaxInt32); err != nil {
		t.Errorf("new ref with no limit: %v", err)
	}
}

func TestObjectSizeAndRefLimits(t *testing.T) {
	q := &pb.Quota{MaxChunkBytes: 10, MaxRefs: 2}
	if err := checkObjectSize(q, "chunk x", 10); err != nil {
		t.Errorf("object at max_chunk_bytes: %v", err)
	}
	if err := checkObjectSize(q, "chunk x", 11); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("object over max_chunk_bytes = %v, want ErrQuotaExceeded", err)
	}
	if err := checkNewRef(q, 1); err != nil {
		t.Errorf("second ref with max_refs 2: %v", err)
	}
	if err := checkNewRef(q, 2); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("third ref with max_refs 2 = %v, want ErrQuotaExceeded", err)
	}
}
