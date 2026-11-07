package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	storetest "github.com/nextgui/protovcs/cmd/protovcsgen/testdata/markedpb"
)

const neverStored = "protovcsgen.testdata.never_stored"

func TestALayoutReachingAMarkedMessageIsRefused(t *testing.T) {
	for _, c := range []struct {
		msg  protoreflect.ProtoMessage
		want string
	}{
		{&storetest.HoldsDirectly{}, "HoldsDirectly.secret"},
		{&storetest.HoldsNested{}, "HoldsNested.wrappers.secret"},
		{&storetest.HoldsInMap{}, "HoldsInMap.wrappers.secret"},
		{&storetest.HoldsInOneof{}, "Secret"},
		{&storetest.Recursive{}, "Recursive.wrapper.secret"},
	} {
		md := c.msg.ProtoReflect().Descriptor()
		err := checkUnmarked(md, protoregistry.GlobalFiles, []string{neverStored})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want a refusal naming %s", md.FullName(), err, c.want)
		}
	}
}

func TestAnUnmarkedLayoutPasses(t *testing.T) {
	md := (&storetest.Clean{}).ProtoReflect().Descriptor()
	if err := checkUnmarked(md, protoregistry.GlobalFiles, []string{neverStored}); err != nil {
		t.Fatalf("Clean: %v", err)
	}
	held := (&storetest.HoldsDirectly{}).ProtoReflect().Descriptor()
	if err := checkUnmarked(held, protoregistry.GlobalFiles, nil); err != nil {
		t.Fatalf("with no option named, nothing is checked: %v", err)
	}
}

func TestAnOptionThatIsNotABoolMessageOptionIsAnError(t *testing.T) {
	md := (&storetest.Clean{}).ProtoReflect().Descriptor()
	if err := checkUnmarked(md, protoregistry.GlobalFiles, []string{"protovcsgen.testdata.no_such_option"}); err == nil {
		t.Fatal("an option no descriptor set declares was accepted")
	}
}
