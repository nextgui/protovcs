package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"testing"
	"time"
)

var when = time.Unix(1_800_000_000, 0).UTC()

func tarball(t *testing.T, files []file) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := write(&buf, "protovcs-1.2.3", files, when); err != nil {
		t.Fatalf("write: %v", err)
	}
	return buf.Bytes()
}

func sample() []file {
	return []file{
		{path: "go.mod", mode: "100644", data: []byte("module x\n")},
		{path: "cmd/tool/main.go", mode: "100644", data: []byte("package main\n")},
		{path: "scripts/run.sh", mode: "100755", data: []byte("#!/bin/sh\n")},
		{path: "link", mode: "120000", data: []byte("go.mod")},
	}
}

func TestTheSameTreeGivesTheSameBytes(t *testing.T) {
	a := tarball(t, sample())
	shuffled := sample()
	shuffled[0], shuffled[3] = shuffled[3], shuffled[0]
	shuffled[1], shuffled[2] = shuffled[2], shuffled[1]
	if b := tarball(t, shuffled); !bytes.Equal(a, b) {
		t.Fatal("the same files in another order gave different bytes")
	}
}

func TestTheTarballHoldsTheTree(t *testing.T) {
	gz, err := gzip.NewReader(bytes.NewReader(tarball(t, sample())))
	if err != nil {
		t.Fatal(err)
	}
	if gz.Name != "" || !gz.ModTime.IsZero() {
		t.Fatalf("the gzip header carries name %q and time %v", gz.Name, gz.ModTime)
	}
	tr := tar.NewReader(gz)
	type entry struct {
		mode int64
		kind byte
		body string
	}
	got := map[string]entry{}
	var order []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !h.ModTime.Equal(when) || h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" {
			t.Errorf("%s has time %v, owner %d/%d %q/%q", h.Name, h.ModTime, h.Uid, h.Gid, h.Uname, h.Gname)
		}
		body, _ := io.ReadAll(tr)
		if h.Typeflag == tar.TypeSymlink {
			body = []byte(h.Linkname)
		}
		got[h.Name] = entry{h.Mode, h.Typeflag, string(body)}
		order = append(order, h.Name)
	}
	want := map[string]entry{
		"protovcs-1.2.3/":                 {0o755, tar.TypeDir, ""},
		"protovcs-1.2.3/cmd/":             {0o755, tar.TypeDir, ""},
		"protovcs-1.2.3/cmd/tool/":        {0o755, tar.TypeDir, ""},
		"protovcs-1.2.3/cmd/tool/main.go": {0o644, tar.TypeReg, "package main\n"},
		"protovcs-1.2.3/go.mod":           {0o644, tar.TypeReg, "module x\n"},
		"protovcs-1.2.3/link":             {0o777, tar.TypeSymlink, "go.mod"},
		"protovcs-1.2.3/scripts/":         {0o755, tar.TypeDir, ""},
		"protovcs-1.2.3/scripts/run.sh":   {0o755, tar.TypeReg, "#!/bin/sh\n"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %v", len(got), len(want), order)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %+v, want %+v", name, got[name], w)
		}
	}
	if order[0] != "protovcs-1.2.3/" || order[1] != "protovcs-1.2.3/cmd/" {
		t.Errorf("directories do not come before their contents: %v", order)
	}
}

func TestABadVersionIsRefused(t *testing.T) {
	if err := run("v0.0.1", "HEAD", t.TempDir()); err == nil {
		t.Fatal("a version with a leading v was accepted")
	}
}
