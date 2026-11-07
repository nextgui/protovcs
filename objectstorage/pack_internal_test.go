package objectstorage

import (
	"testing"

	protovcs "github.com/nextgui/protovcs"
)

// TestReadRunsSplitAtLargeGaps checks that the chunks wanted from one pack are
// read as one range when they are close together, and as separate ranges when
// more than packReadGap of unwanted bytes lies between them.
func TestReadRunsSplitAtLargeGaps(t *testing.T) {
	var a, b, c, d protovcs.ChunkID
	a[0], b[0], c[0], d[0] = 1, 2, 3, 4
	locs := map[protovcs.ChunkID]packLoc{
		a: {pack: "p", offset: 0, length: 100},
		b: {pack: "p", offset: 100 + packReadGap, length: 100},       // exactly the gap: read through
		c: {pack: "p", offset: 200 + 2*packReadGap + 1, length: 100}, // one byte over: split
		d: {pack: "p", offset: 300 + 2*packReadGap + 1, length: 100}, // adjacent to c
	}

	runs := readRuns([]protovcs.ChunkID{d, c, b, a}, locs)
	if len(runs) != 2 {
		t.Fatalf("got %d runs, want 2: %v", len(runs), runs)
	}
	if len(runs[0]) != 2 || runs[0][0] != a || runs[0][1] != b {
		t.Errorf("first run is %v, want [a b] in offset order", runs[0])
	}
	if len(runs[1]) != 2 || runs[1][0] != c || runs[1][1] != d {
		t.Errorf("second run is %v, want [c d] in offset order", runs[1])
	}

	if got := readRuns([]protovcs.ChunkID{a}, locs); len(got) != 1 || len(got[0]) != 1 {
		t.Errorf("one chunk gave runs %v, want one run of one", got)
	}
}
