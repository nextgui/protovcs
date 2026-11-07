package protovcs

import (
	"strings"
	"testing"

	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// compileErr compiles a layout against Store and returns the error text, or ""
// when it compiled.
func compileErr(t *testing.T, l *pb.Layout) string {
	t.Helper()
	_, err := CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err == nil {
		return ""
	}
	return err.Error()
}

func base() *pb.Layout {
	return &pb.Layout{
		Name:          "teststore",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
	}
}

func TestCompileAcceptsAWorkingLayout(t *testing.T) {
	if msg := compileErr(t, testLayout()); msg != "" {
		t.Fatalf("a valid layout was refused: %s", msg)
	}
}

func TestCompileRefusesAFieldThatDoesNotExist(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "transactions",
		By:    []*pb.ShardKey{{Field: "id"}},
	}}
	msg := compileErr(t, l)
	if !strings.Contains(msg, "no field \"transactions\"") {
		t.Fatalf("error was %q, want it to name the missing field", msg)
	}
}

func TestCompileRefusesAShardKeyThatDoesNotExist(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "records",
		By:    []*pb.ShardKey{{Field: "merchant"}},
	}}
	msg := compileErr(t, l)
	if !strings.Contains(msg, "no field \"merchant\"") {
		t.Fatalf("error was %q, want it to name the missing shard key", msg)
	}
}

func TestCompileRefusesADateTransformOnAString(t *testing.T) {
	// The mistake this exists to catch: MONTH on a field that is not a date.
	// Caught here rather than at the first write.
	l := base()
	l.Splits = []*pb.Split{{
		Field: "records",
		By:    []*pb.ShardKey{{Field: "group", Transform: pb.Transform_TRANSFORM_MONTH}},
	}}
	msg := compileErr(t, l)
	if !strings.Contains(msg, "date transform") {
		t.Fatalf("error was %q, want it to say a date transform needs a date", msg)
	}
}

func TestCompileRefusesIdentityOnAMessage(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "records",
		By:    []*pb.ShardKey{{Field: "nested", Transform: pb.Transform_TRANSFORM_IDENTITY}},
	}}
	if msg := compileErr(t, l); !strings.Contains(msg, "no identity form") {
		t.Fatalf("error was %q, want it to refuse a message as a shard key", msg)
	}
}

func TestCompileRefusesHashingIntoNoBuckets(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "records",
		By:    []*pb.ShardKey{{Field: "group", Transform: pb.Transform_TRANSFORM_HASH}},
	}}
	if msg := compileErr(t, l); !strings.Contains(msg, "no buckets") {
		t.Fatalf("error was %q, want it to refuse a hash with no buckets", msg)
	}
}

func TestCompileRefusesSplittingANonRepeatedField(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "name",
		By:    []*pb.ShardKey{{Field: "id"}},
	}}
	if msg := compileErr(t, l); !strings.Contains(msg, "not repeated") {
		t.Fatalf("error was %q, want it to say the field is not repeated", msg)
	}
}

func TestCompileRefusesASplitWithNoShardKey(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{Field: "records"}}
	if msg := compileErr(t, l); !strings.Contains(msg, "no shard key") {
		t.Fatalf("error was %q, want it to say the split names no shard key", msg)
	}
}

func TestCompileRefusesTheSameFieldTwice(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{
		{Field: "records", By: []*pb.ShardKey{{Field: "group"}}},
		{Field: "records", By: []*pb.ShardKey{{Field: "id"}}},
	}
	if msg := compileErr(t, l); !strings.Contains(msg, "twice") {
		t.Fatalf("error was %q, want it to refuse splitting one field twice", msg)
	}
}

func TestCompileRefusesTheWrongRootMessage(t *testing.T) {
	l := base()
	l.RootMessage = "nextgui.protovcs.testpb.Record"
	if msg := compileErr(t, l); !strings.Contains(msg, "is for") {
		t.Fatalf("error was %q, want it to refuse a layout for another message", msg)
	}
}

func TestCompileRefusesOrderingByAMessage(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "records",
		By:    []*pb.ShardKey{{Field: "group"}},
		Order: []string{"nested"},
	}}
	if msg := compileErr(t, l); !strings.Contains(msg, "not a scalar") {
		t.Fatalf("error was %q, want it to refuse ordering by a message", msg)
	}
}

func TestCompileRefusesAQueryNoSplitCovers(t *testing.T) {
	// A query nothing can serve is not slow, it is a download of the whole
	// store. Refused at build time so it cannot be discovered in a browser.
	l := testLayout()
	l.Queries = []*pb.Query{{
		Name:  "RecordsByPayload",
		Field: "payload",
		By:    []*pb.Predicate{{Field: "payload", Op: pb.Op_OP_EQ}},
	}}
	if msg := compileErr(t, l); !strings.Contains(msg, "scan the whole store") {
		t.Fatalf("error was %q, want it to refuse an unservable query", msg)
	}
}

func TestCompileRefusesAQueryThatSkipsAShardKeyComponent(t *testing.T) {
	// Filtering on the second component with nothing on the first selects a
	// scatter across every value of the first, which is a scan in a query's
	// clothing.
	l := testLayout()
	l.Queries = []*pb.Query{{
		Name:  "RecordsByMonth",
		Field: "records",
		By:    []*pb.Predicate{{Field: "date", Op: pb.Op_OP_RANGE}},
	}}
	if msg := compileErr(t, l); !strings.Contains(msg, "at position 0") {
		t.Fatalf("error was %q, want it to name the component that was skipped", msg)
	}
}

func TestCompileRefusesARangeOverAHashedComponent(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "records",
		By:    []*pb.ShardKey{{Field: "group", Transform: pb.Transform_TRANSFORM_HASH, Buckets: 64}},
	}}
	l.Queries = []*pb.Query{{
		Name:  "RecordsByGroupRange",
		Field: "records",
		By:    []*pb.Predicate{{Field: "group", Op: pb.Op_OP_RANGE}},
	}}
	if msg := compileErr(t, l); !strings.Contains(msg, "has no order") {
		t.Fatalf("error was %q, want it to refuse a range over a hashed component", msg)
	}
}

func TestCompileAcceptsAServableQuery(t *testing.T) {
	l := testLayout()
	l.Queries = []*pb.Query{{
		Name:  "RecordsForGroupInRange",
		Field: "records",
		By: []*pb.Predicate{
			{Field: "group", Op: pb.Op_OP_EQ},
			{Field: "date", Op: pb.Op_OP_RANGE},
		},
	}}
	if msg := compileErr(t, l); msg != "" {
		t.Fatalf("a servable query was refused: %s", msg)
	}
}

func TestCompileRefusesADuplicateScope(t *testing.T) {
	l := testLayout()
	l.Scopes = []*pb.Scope{
		{Name: "Everything", Include: []*pb.ScopeInclude{{Field: "records"}}},
		{Name: "Everything", Include: []*pb.ScopeInclude{{Field: "accounts"}}},
	}
	if msg := compileErr(t, l); !strings.Contains(msg, "twice") {
		t.Fatalf("error was %q, want it to refuse a duplicate scope", msg)
	}
}

func TestCompileRefusesAScopeOverAFieldThatDoesNotExist(t *testing.T) {
	l := testLayout()
	l.Scopes = []*pb.Scope{{Name: "Ghost", Include: []*pb.ScopeInclude{{Field: "ghosts"}}}}
	if msg := compileErr(t, l); !strings.Contains(msg, "no field \"ghosts\"") {
		t.Fatalf("error was %q, want it to name the missing field", msg)
	}
}

func TestShardValuesSortTheWayTheValuesDo(t *testing.T) {
	// Item keys are sorted strings, and a range of them has to be a contiguous
	// walk. That only holds if string order matches value order, which is why
	// numbers are zero-padded and dates are written widest part first.
	c := compiledTestLayout(t)

	var last ItemKey
	for month := 1; month <= 12; month++ {
		key, err := c.ItemKeyFor("records", &testpb.Record{
			Group: "alpha",
			Date:  &testpb.Date{Year: 2026, Month: int32(month), Day: 1},
		})
		if err != nil {
			t.Fatalf("ItemKeyFor: %v", err)
		}
		if key <= last {
			t.Fatalf("month %d produced key %q, which does not sort after %q", month, key, last)
		}
		last = key
	}
}

func TestDateTransformsTruncate(t *testing.T) {
	for _, tc := range []struct {
		transform pb.Transform
		want      string
	}{
		{pb.Transform_TRANSFORM_YEAR, "records/2026"},
		{pb.Transform_TRANSFORM_QUARTER, "records/2026/Q3"},
		{pb.Transform_TRANSFORM_MONTH, "records/2026/08"},
		{pb.Transform_TRANSFORM_DAY, "records/2026/08/14"},
	} {
		l := base()
		l.Splits = []*pb.Split{{
			Field: "records",
			By:    []*pb.ShardKey{{Field: "date", Transform: tc.transform}},
		}}
		c, err := CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
		if err != nil {
			t.Fatalf("CompileLayout(%v): %v", tc.transform, err)
		}
		key, err := c.ItemKeyFor("records", &testpb.Record{
			Date: &testpb.Date{Year: 2026, Month: 8, Day: 14},
		})
		if err != nil {
			t.Fatalf("ItemKeyFor(%v): %v", tc.transform, err)
		}
		if string(key) != tc.want {
			t.Fatalf("%v produced %q, want %q", tc.transform, key, tc.want)
		}
	}
}

func TestHashTransformIsStableAndBounded(t *testing.T) {
	l := base()
	l.Splits = []*pb.Split{{
		Field: "records",
		By:    []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_HASH, Buckets: 16}},
	}}
	c, err := CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}

	seen := map[ItemKey]bool{}
	for i := 0; i < 500; i++ {
		rec := &testpb.Record{Id: strings.Repeat("x", i%17) + string(rune('a'+i%26))}
		a, err := c.ItemKeyFor("records", rec)
		if err != nil {
			t.Fatalf("ItemKeyFor: %v", err)
		}
		b, err := c.ItemKeyFor("records", rec)
		if err != nil {
			t.Fatalf("ItemKeyFor: %v", err)
		}
		if a != b {
			t.Fatalf("hashing %q gave %q then %q", rec.Id, a, b)
		}
		seen[a] = true
	}
	if len(seen) > 16 {
		t.Fatalf("hashing produced %d buckets, want at most 16", len(seen))
	}
	if len(seen) < 8 {
		t.Fatalf("hashing produced only %d of 16 buckets, which is too uneven to be a hash", len(seen))
	}
}

func TestADateWithNoMonthIsRefused(t *testing.T) {
	// An element that belongs to no period cannot be filed, and guessing a
	// period would put it somewhere nothing would look for it.
	c := compiledTestLayout(t)
	if _, err := c.ItemKeyFor("records", &testpb.Record{Group: "alpha"}); err == nil {
		t.Fatal("a record with no date was given an item key")
	}
}
