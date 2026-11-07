package gcsemulator_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"testing"

	"cloud.google.com/go/storage"
)

// customerKey and otherKey are two distinct 32-byte AES-256 keys, which is the
// only length Cloud Storage accepts for a customer-supplied key.
var (
	customerKey = []byte("0123456789abcdef0123456789abcdef")
	otherKey    = []byte("fedcba9876543210fedcba9876543210")
)

// keyDigest is the base64 SHA-256 the emulator records for a key, which is what
// Cloud Storage keeps and the only thing either of them keeps.
func keyDigest(key []byte) string {
	sum := sha256.Sum256(key)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// writeEncrypted puts one object under a customer-supplied key.
func writeEncrypted(t *testing.T, client *storage.Client, bucket, name, body string, key []byte) {
	t.Helper()

	w := client.Bucket(bucket).Object(name).Key(key).NewWriter(context.Background())
	if _, err := io.WriteString(w, body); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

// readObject reads an object back through the handle the caller prepared, which
// is how a test states which key, if any, the read presents.
func readObject(t *testing.T, obj *storage.ObjectHandle) (string, error) {
	t.Helper()

	r, err := obj.NewReader(context.Background())
	if err != nil {
		return "", err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	return string(data), err
}

// TestCustomerKeyRoundTrip checks the ordinary path: an object written under a
// customer-supplied key reads back through the same key.
func TestCustomerKeyRoundTrip(t *testing.T) {
	client, _ := newClient(t)
	writeEncrypted(t, client, "csek-bucket", "secret.txt", "hello", customerKey)

	got, err := readObject(t, client.Bucket("csek-bucket").Object("secret.txt").Key(customerKey))
	if err != nil {
		t.Fatalf("read with the key: %v", err)
	}
	if got != "hello" {
		t.Errorf("read %q, want %q", got, "hello")
	}
}

// TestCustomerKeyIsRecorded checks that the emulator keeps the key's digest and
// nothing more, so a test can assert that a server really did encrypt what it
// wrote rather than inferring it from the absence of an error.
func TestCustomerKeyIsRecorded(t *testing.T) {
	client, emu := newClient(t)
	writeEncrypted(t, client, "csek-bucket", "secret.txt", "hello", customerKey)
	write(t, client, "csek-bucket", "plain.txt", "hello")

	got, ok := emu.ObjectKeySHA256("csek-bucket", "secret.txt")
	if !ok {
		t.Fatal("ObjectKeySHA256 reports no such object")
	}
	if want := keyDigest(customerKey); got != want {
		t.Errorf("recorded digest = %q, want %q", got, want)
	}

	if got, ok := emu.ObjectKeySHA256("csek-bucket", "plain.txt"); !ok || got != "" {
		t.Errorf("ObjectKeySHA256 of an unencrypted object = %q, %t, want \"\", true", got, ok)
	}
	if _, ok := emu.ObjectKeySHA256("csek-bucket", "absent.txt"); ok {
		t.Error("ObjectKeySHA256 reports an object that was never written")
	}
}

// TestCustomerKeyMismatchesAreRefused covers every way a request and an object
// can disagree about the key. Each one is a 400 in Cloud Storage, and each one
// has to be a refusal here or a test that thinks it is checking encryption is
// checking nothing.
func TestCustomerKeyMismatchesAreRefused(t *testing.T) {
	client, _ := newClient(t)
	writeEncrypted(t, client, "csek-bucket", "secret.txt", "hello", customerKey)
	write(t, client, "csek-bucket", "plain.txt", "hello")

	encrypted := client.Bucket("csek-bucket").Object("secret.txt")
	plain := client.Bucket("csek-bucket").Object("plain.txt")

	cases := map[string]*storage.ObjectHandle{
		"encrypted object, no key":    encrypted,
		"encrypted object, wrong key": encrypted.Key(otherKey),
		"plain object, a key":         plain.Key(customerKey),
	}
	for name, obj := range cases {
		t.Run(name, func(t *testing.T) {
			if body, err := readObject(t, obj); err == nil {
				t.Errorf("read succeeded and returned %q, want a refusal", body)
			}
			if _, err := obj.Attrs(context.Background()); err == nil {
				t.Error("Attrs succeeded, want a refusal")
			}
		})
	}
}

// TestCustomerKeyDoesNotAffectListing checks that a listing works without the
// key, as it does in Cloud Storage. The reaper and the ref iterator both list
// before they read, and neither holds a client's key.
func TestCustomerKeyDoesNotAffectListing(t *testing.T) {
	client, _ := newClient(t)
	writeEncrypted(t, client, "csek-bucket", "a/one.txt", "1", customerKey)
	writeEncrypted(t, client, "csek-bucket", "a/two.txt", "2", customerKey)

	got := list(t, client, "csek-bucket", &storage.Query{Prefix: "a/"})
	if len(got) != 2 {
		t.Errorf("listing returned %v, want both encrypted objects", got)
	}
}

// TestCustomerKeyDeleteNeedsNoKey checks that deletion works without the key,
// as it does in Cloud Storage: the key protects the bytes, not the name.
func TestCustomerKeyDeleteNeedsNoKey(t *testing.T) {
	client, emu := newClient(t)
	writeEncrypted(t, client, "csek-bucket", "secret.txt", "hello", customerKey)

	if err := client.Bucket("csek-bucket").Object("secret.txt").Delete(context.Background()); err != nil {
		t.Fatalf("delete without the key: %v", err)
	}
	if _, ok := emu.ObjectKeySHA256("csek-bucket", "secret.txt"); ok {
		t.Error("the object survived a delete")
	}
}
