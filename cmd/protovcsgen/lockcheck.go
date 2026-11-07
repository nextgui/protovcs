package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// lockedMessage matches a message declaration in a schema lock.
//
// The lock is machine-written by stableproto with a fixed shape, one
// message per top-level form. Anchoring to the start of a line is what keeps
// this from matching the word inside a comment.
//
// If that format ever changes, this finds nothing, and finding nothing means
// every message is reported as uncovered and the build fails. That is the safe
// direction: a lock check that silently stops checking is worse than one that
// stops the build.
var lockedMessage = regexp.MustCompile(`(?m)^\(message\s+([A-Za-z0-9_.]+)`)

// checkLocked refuses a layout whose message is not covered by a schema lock.
//
// A chunk's name is a fingerprint of canonical bytes, and canonical bytes are
// field numbers and values. Renumbering or retyping a field therefore does two
// things at once: it changes what every stored byte in every existing store
// means, and it changes the name of every chunk holding it. Neither is
// recoverable, and neither announces itself — the store simply starts reading
// as something else.
//
// stableproto refuses those changes, but only for the files it was
// told to lock. A message reachable from the root that nobody locked is
// therefore the hole: protolock never looks at it, and a renumbering inside it
// passes every check this repository has. This closes that hole by requiring
// every message the layout can reach to appear in a lock.
//
// It deliberately does not re-check that the lock agrees with the descriptors.
// protolock already fails the build when a locked file and its lock disagree,
// so a covered message is a current one, and duplicating that check here would
// give two places to keep a policy.
func checkLocked(md protoreflect.MessageDescriptor, lockPaths []string) error {
	if len(lockPaths) == 0 {
		return fmt.Errorf(
			"layout over %s has no schema lock.\n"+
				"A chunk is named by a fingerprint of its canonical bytes, so renumbering or\n"+
				"retyping a field changes what every stored byte means and renames every chunk\n"+
				"holding it. Add a proto_schema_lock for the message and pass it as the lock\n"+
				"attribute of protovcs_layout_go.", md.FullName())
	}

	covered := map[string]bool{}
	for _, path := range lockPaths {
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading schema lock %s: %w", path, err)
		}
		for _, m := range lockedMessage.FindAllStringSubmatch(string(raw), -1) {
			covered[m[1]] = true
		}
	}

	reachable := map[string]protoreflect.MessageDescriptor{}
	collectReachable(md, reachable)

	var missing []string
	for name := range reachable {
		if covered[name] || wellKnown(name) {
			continue
		}
		missing = append(missing, name)
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)

	return fmt.Errorf(
		"layout over %s reaches %d message(s) that no schema lock covers:\n  %s\n\n"+
			"Every message a stored protobuf can reach has to be locked, because a\n"+
			"renumbered field inside an unlocked one changes what existing stores decode to\n"+
			"and renames every chunk holding it, with nothing reporting either.\n"+
			"Add these to the files attribute of the proto_schema_lock, regenerate it, and\n"+
			"pass the lock to protovcs_layout_go.",
		md.FullName(), len(missing), strings.Join(missing, "\n  "))
}

// wellKnown reports whether a message's schema is fixed by protobuf rather than
// by this repository.
//
// google.protobuf.Timestamp and its siblings cannot be renumbered by anybody
// here, and their compatibility is a guarantee of the protobuf project. Asking
// for a lock on one would be asking somebody to pin a file they do not own.
func wellKnown(name string) bool {
	return strings.HasPrefix(name, "google.protobuf.")
}

// collectReachable walks every message a stored instance of md can contain.
//
// Map entries are followed through their value type, and nested messages
// through their fields, because the bytes of all of them end up inside a chunk
// and every one of their field numbers is part of what a chunk name commits to.
func collectReachable(md protoreflect.MessageDescriptor, into map[string]protoreflect.MessageDescriptor) {
	name := string(md.FullName())
	if _, seen := into[name]; seen {
		return
	}
	// A map entry is a synthetic message the author never wrote and cannot
	// renumber, so it is walked through but not required to be locked.
	if !md.IsMapEntry() {
		into[name] = md
	}

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			continue
		}
		collectReachable(fd.Message(), into)
	}
}
