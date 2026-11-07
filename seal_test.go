package protovcs

import (
	"bytes"
	"errors"
	"testing"
)

func testKeyring(t *testing.T) Sealer {
	t.Helper()
	key := make([]byte, RootKeyLen)
	for i := range key {
		key[i] = byte(i)
	}
	s, err := NewKeyring(key)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	return s
}

func secondKeyring(t *testing.T) Sealer {
	t.Helper()
	key := make([]byte, RootKeyLen)
	for i := range key {
		key[i] = byte(255 - i)
	}
	s, err := NewKeyring(key)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	return s
}

func TestNewKeyringRefusesTheWrongKeyLength(t *testing.T) {
	for _, n := range []int{0, 16, 31, 33, 64} {
		if _, err := NewKeyring(make([]byte, n)); err == nil {
			t.Fatalf("a %d-byte key was accepted", n)
		}
	}
}

func TestSealOpenRoundTrip(t *testing.T) {
	k := testKeyring(t)
	plaintext := []byte("one item's canonical bytes")
	id := k.ChunkID(RoleItem, "records/2026/08/14", plaintext)

	sealed, err := k.Seal(id, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Contains(sealed, plaintext) {
		t.Fatal("the sealed form contains the plaintext")
	}

	opened, err := k.Open(id, sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("Open returned %q, want %q", opened, plaintext)
	}
}

func TestChunkIDIsAFingerprint(t *testing.T) {
	k := testKeyring(t)

	a := k.ChunkID(RoleItem, "records/2026/08", []byte("same"))
	b := k.ChunkID(RoleItem, "records/2026/08", []byte("same"))
	if a != b {
		t.Fatal("the same contents at the same key produced different names")
	}

	c := k.ChunkID(RoleItem, "records/2026/08", []byte("different"))
	if a == c {
		t.Fatal("different contents produced the same name")
	}
}

func TestChunkIDSeparatesRoles(t *testing.T) {
	// A manifest node and an item holding identical bytes must have different
	// names. Without this a server could return a manifest node where an item
	// was asked for and it would open, because both are sealed under one key.
	k := testKeyring(t)
	body := []byte("identical bytes")

	item := k.ChunkID(RoleItem, "records/2026/08", body)
	manifest := k.ChunkID(RoleManifest, "records/2026/08", body)
	schema := k.ChunkID(RoleSchema, "records/2026/08", body)

	if item == manifest || item == schema || manifest == schema {
		t.Fatal("two roles produced the same name for the same bytes")
	}
}

func TestChunkIDSeparatesKeys(t *testing.T) {
	k := testKeyring(t)
	body := []byte("identical bytes")

	if k.ChunkID(RoleItem, "records/2026/07", body) == k.ChunkID(RoleItem, "records/2026/08", body) {
		t.Fatal("the same bytes at two item keys produced the same name")
	}
}

func TestChunkIDCannotBeConfusedByASeparator(t *testing.T) {
	// The role and the key are length-prefixed rather than joined, so an item
	// key that looks like it contains a role boundary cannot be made to collide
	// with a different role and key.
	k := testKeyring(t)
	body := []byte("x")

	a := k.ChunkID(RoleItem, "a/b", body)
	b := k.ChunkID(Role("itema"), "/b", body)
	if a == b {
		t.Fatal("a role and key pair collided with a different pair")
	}
}

func TestChunkIDsDoNotCorrelateAcrossStores(t *testing.T) {
	// Keys are derived per store, so two stores holding identical data share no
	// names. This is what stops a server learning that two users hold the same
	// thing.
	first := testKeyring(t)
	second := secondKeyring(t)

	body := []byte("the very same contents")
	if first.ChunkID(RoleItem, "records/1", body) == second.ChunkID(RoleItem, "records/1", body) {
		t.Fatal("two stores named identical contents the same way")
	}
}

func TestOpenRefusesAChunkServedUnderAnotherName(t *testing.T) {
	// The attack this defends against: a server that cannot read two chunks can
	// still swap them, and without the name bound in, both would open and the
	// caller would be shown one month's data as another's.
	k := testKeyring(t)

	july := []byte("july's records")
	august := []byte("august's records")
	julyID := k.ChunkID(RoleItem, "records/2026/07", july)
	augustID := k.ChunkID(RoleItem, "records/2026/08", august)

	sealedJuly, err := k.Seal(julyID, july)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if _, err := k.Open(augustID, sealedJuly); !errors.Is(err, ErrTampered) {
		t.Fatalf("opening July's chunk as August's returned %v, want ErrTampered", err)
	}
}

func TestOpenRefusesAlteredBytes(t *testing.T) {
	k := testKeyring(t)
	plaintext := []byte("contents that must not be altered")
	id := k.ChunkID(RoleItem, "records/1", plaintext)

	sealed, err := k.Seal(id, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	for i := range sealed {
		altered := bytes.Clone(sealed)
		altered[i] ^= 0x01

		_, err := k.Open(id, altered)
		if err == nil {
			t.Fatalf("flipping bit 0 of byte %d was not detected", i)
		}
		// The version byte is checked before decryption and reports a format it
		// does not know, which must stay distinct: a user running an old client
		// must not be told their data was interfered with.
		if i == 0 {
			if !errors.Is(err, ErrUnsupportedFormat) {
				t.Fatalf("altering the version byte returned %v, want ErrUnsupportedFormat", err)
			}
			continue
		}
		if !errors.Is(err, ErrTampered) {
			t.Fatalf("altering byte %d returned %v, want ErrTampered", i, err)
		}
	}
}

func TestOpenRefusesTheWrongKey(t *testing.T) {
	first := testKeyring(t)
	second := secondKeyring(t)

	plaintext := []byte("contents")
	id := first.ChunkID(RoleItem, "records/1", plaintext)
	sealed, err := first.Seal(id, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if _, err := second.Open(id, sealed); !errors.Is(err, ErrTampered) {
		t.Fatalf("opening under the wrong key returned %v, want ErrTampered", err)
	}
}

func TestOpenRefusesTruncatedInput(t *testing.T) {
	k := testKeyring(t)
	plaintext := []byte("contents")
	id := k.ChunkID(RoleItem, "records/1", plaintext)
	sealed, err := k.Seal(id, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	for n := 0; n < len(sealed); n++ {
		if _, err := k.Open(id, sealed[:n]); err == nil {
			t.Fatalf("a chunk truncated to %d bytes opened", n)
		}
	}
}

func TestOpenRefusesAFutureFormat(t *testing.T) {
	k := testKeyring(t)
	plaintext := []byte("contents")
	id := k.ChunkID(RoleItem, "records/1", plaintext)
	sealed, err := k.Seal(id, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sealed[0] = FormatVersion + 1

	if _, err := k.Open(id, sealed); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("a newer format returned %v, want ErrUnsupportedFormat", err)
	}
}

func TestNonceIsFreshEveryTime(t *testing.T) {
	// Sealing the same chunk twice must not produce the same bytes. The name is
	// what deduplicates; identical ciphertext is not needed and reusing a nonce
	// with one key is what breaks the cipher.
	k := testKeyring(t)
	plaintext := []byte("contents")
	id := k.ChunkID(RoleItem, "records/1", plaintext)

	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		sealed, err := k.Seal(id, plaintext)
		if err != nil {
			t.Fatalf("Seal: %v", err)
		}
		nonce := string(sealed[1 : 1+nonceLen])
		if seen[nonce] {
			t.Fatal("a nonce was drawn twice")
		}
		seen[nonce] = true
	}
}

func TestRefNamesAreHashed(t *testing.T) {
	k := testKeyring(t)

	name := k.RefName("import/plaid-2026-08")
	if bytes.Contains(name, []byte("plaid")) {
		t.Fatal("the branch name reached the stored form")
	}
	if !bytes.Equal(name, k.RefName("import/plaid-2026-08")) {
		t.Fatal("the same branch name hashed two ways, so lookup by name would fail")
	}
	if bytes.Equal(name, k.RefName("main")) {
		t.Fatal("two branch names hashed the same")
	}
}

func TestVerifyChunkCatchesSubstitution(t *testing.T) {
	k := testKeyring(t)
	body := []byte("august")
	id := k.ChunkID(RoleItem, "records/2026/08", body)

	if err := VerifyChunk(k, RoleItem, "records/2026/08", id, body); err != nil {
		t.Fatalf("VerifyChunk on genuine contents: %v", err)
	}
	if err := VerifyChunk(k, RoleItem, "records/2026/07", id, body); !errors.Is(err, ErrTampered) {
		t.Fatalf("VerifyChunk under the wrong key returned %v, want ErrTampered", err)
	}
	if err := VerifyChunk(k, RoleManifest, "records/2026/08", id, body); !errors.Is(err, ErrTampered) {
		t.Fatalf("VerifyChunk under the wrong role returned %v, want ErrTampered", err)
	}
}

func TestPlainSealerStoresContentsAsTheyAre(t *testing.T) {
	s := PlainSealer()
	if s.Sealed() {
		t.Fatal("PlainSealer reports itself as sealed")
	}

	plaintext := []byte("readable contents")
	id := s.ChunkID(RoleItem, "records/1", plaintext)

	sealed, err := s.Seal(id, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if !bytes.Contains(sealed, plaintext) {
		t.Fatal("a plaintext store did not store the contents readably")
	}

	opened, err := s.Open(id, sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("Open returned %q, want %q", opened, plaintext)
	}
}

func TestPlainSealerStillNamesByContents(t *testing.T) {
	// A plaintext store gives up confidentiality and keeps everything else.
	// Deduplication, incremental fetch and cache validity all come from the
	// name being a fingerprint, and they still work here.
	s := PlainSealer()

	if s.ChunkID(RoleItem, "k", []byte("a")) != s.ChunkID(RoleItem, "k", []byte("a")) {
		t.Fatal("the same contents produced different names")
	}
	if s.ChunkID(RoleItem, "k", []byte("a")) == s.ChunkID(RoleItem, "k", []byte("b")) {
		t.Fatal("different contents produced the same name")
	}
	if s.ChunkID(RoleItem, "k", []byte("a")) == s.ChunkID(RoleManifest, "k", []byte("a")) {
		t.Fatal("two roles produced the same name")
	}
}

func TestPlainSealerRefusesAFutureFormat(t *testing.T) {
	s := PlainSealer()
	if _, err := s.Open(ChunkID{}, []byte{FormatVersion + 1, 0x00}); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("a newer format returned %v, want ErrUnsupportedFormat", err)
	}
}

func TestChunkIDFromBytesRefusesTheWrongLength(t *testing.T) {
	if _, err := ChunkIDFromBytes(make([]byte, ChunkIDLen)); err != nil {
		t.Fatalf("a correct length was refused: %v", err)
	}
	for _, n := range []int{0, 1, ChunkIDLen - 1, ChunkIDLen + 1} {
		if _, err := ChunkIDFromBytes(make([]byte, n)); err == nil {
			t.Fatalf("a %d-byte id was accepted", n)
		}
	}
}
