package protovcs

import "context"

// ProbePoint names a step inside an operation.
//
// The write path is a sequence with a specific order — chunks, then the commit,
// then the ref — and the order is the whole of its crash safety. A test that
// cannot stop between two of those steps cannot check that the order holds,
// which leaves the property this design most depends on asserted only by
// reading the code.
type ProbePoint string

const (
	// ProbeAfterHead fires once a writer has read the branch and before it has
	// built anything. Releasing another writer here produces the interleaving
	// that makes the first one lose its race, exactly rather than by luck.
	ProbeAfterHead ProbePoint = "after-head"

	// ProbeAfterUpload fires once every chunk is stored and before the commit
	// exists. A failure here is the crash that leaves chunks nothing points at,
	// which must be garbage rather than damage.
	ProbeAfterUpload ProbePoint = "after-upload"

	// ProbeAfterCommit fires once the commit is recorded and before the branch
	// moves. A failure here leaves a complete version that no branch reaches —
	// the other half of the same window, and the more dangerous half, because
	// the reverse order would leave a branch pointing at something incomplete.
	ProbeAfterCommit ProbePoint = "after-commit"

	// ProbeBeforeRefUpdate fires immediately before the compare-and-swap.
	ProbeBeforeRefUpdate ProbePoint = "before-ref-update"
)

// Probe observes the steps of an operation and may fail one.
//
// # Why this is an interface and not a build tag
//
// Compiling the hooks out of production would make the tested binary a
// different binary from the shipped one, which for a system whose failures are
// silent is the wrong trade: the paths worth instrumenting are the ones worth
// being sure about. What it costs instead is a nil check at four points, each
// reached once per commit, next to AES-GCM and a network round trip. That is
// not a cost, and calling it one would be optimising the wrong thing.
type Probe interface {
	// At is called at a named point. Returning an error makes the operation
	// fail there, which is how a crash is simulated.
	At(ctx context.Context, point ProbePoint) error
}

// WithProbe attaches a probe. Tests use it; nothing else should.
func (b *StoreBuilder) WithProbe(p Probe) *StoreBuilder {
	b.probe = p
	return b
}

// probeAt fires a point if a probe is attached.
func (s *Store) probeAt(ctx context.Context, point ProbePoint) error {
	if s.probe == nil {
		return nil
	}
	return s.probe.At(ctx, point)
}

// ProbeFunc adapts a function to [Probe].
type ProbeFunc func(ctx context.Context, point ProbePoint) error

// At implements [Probe].
func (f ProbeFunc) At(ctx context.Context, point ProbePoint) error { return f(ctx, point) }
