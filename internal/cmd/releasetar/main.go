// Command releasetar writes a release's source tarball, byte for byte the same
// every time it is run on the same commit.
//
// GitHub's own source archives are not guaranteed stable, and a build that
// pins an archive by hash breaks when one changes. This builds the archive
// from the commit's git tree rather than the working directory, so the files,
// their modes and their order come from git alone:
//
//   - entries are sorted by path, with a directory entry before its contents;
//   - every modification time is the commit's time, and owners are zero;
//   - the gzip header carries no name or time.
//
// The bytes depend on this program and on the Go version that compiles it,
// which the release workflow pins.
//
//	go run ./internal/cmd/releasetar -version 0.0.1 -out dist
//
// writes dist/protovcs-0.0.1.tar.gz and dist/protovcs-0.0.1.tar.gz.sha256 for
// HEAD, with every path under protovcs-0.0.1/.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	version := flag.String("version", "", "the release version, such as 0.0.1")
	rev := flag.String("rev", "HEAD", "the commit to archive")
	out := flag.String("out", "dist", "the directory to write the tarball and its checksum to")
	flag.Parse()
	if err := run(*version, *rev, *out); err != nil {
		fmt.Fprintf(os.Stderr, "releasetar: %v\n", err)
		os.Exit(1)
	}
}

var semver = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`)

func run(version, rev, out string) error {
	if !semver.MatchString(version) {
		return fmt.Errorf("-version %q is not a version such as 0.0.1", version)
	}
	files, when, err := readTree(rev)
	if err != nil {
		return err
	}
	name := "protovcs-" + version
	var buf bytes.Buffer
	if err := write(&buf, name, files, when); err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	tarball := filepath.Join(out, name+".tar.gz")
	if err := os.WriteFile(tarball, buf.Bytes(), 0o644); err != nil {
		return err
	}
	sum := sha256.Sum256(buf.Bytes())
	line := hex.EncodeToString(sum[:]) + "  " + name + ".tar.gz\n"
	if err := os.WriteFile(tarball+".sha256", []byte(line), 0o644); err != nil {
		return err
	}
	fmt.Print(line)
	return nil
}

// file is one entry of the commit's tree.
type file struct {
	path string
	mode string // git's: 100644, 100755 or 120000
	data []byte
}

// readTree reads every file in rev's tree, and rev's commit time.
func readTree(rev string) ([]file, time.Time, error) {
	ct, err := git("log", "-1", "--format=%ct", rev)
	if err != nil {
		return nil, time.Time{}, err
	}
	secs, err := strconv.ParseInt(strings.TrimSpace(string(ct)), 10, 64)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("reading %s's commit time: %w", rev, err)
	}

	listing, err := git("ls-tree", "-r", "-z", "--full-tree", rev)
	if err != nil {
		return nil, time.Time{}, err
	}
	var files []file
	for _, rec := range bytes.Split(bytes.TrimSuffix(listing, []byte{0}), []byte{0}) {
		meta, p, ok := strings.Cut(string(rec), "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 3 {
			return nil, time.Time{}, fmt.Errorf("unexpected ls-tree line %q", rec)
		}
		mode, kind, id := fields[0], fields[1], fields[2]
		if kind != "blob" {
			// A submodule has no content in this repository to archive.
			return nil, time.Time{}, fmt.Errorf("%s is a %s, which a release cannot hold", p, kind)
		}
		data, err := git("cat-file", "blob", id)
		if err != nil {
			return nil, time.Time{}, err
		}
		files = append(files, file{path: p, mode: mode, data: data})
	}
	return files, time.Unix(secs, 0).UTC(), nil
}

// write writes files as a gzipped tar under prefix/, in a fixed order with
// fixed metadata.
func write(w io.Writer, prefix string, files []file, when time.Time) error {
	gz, err := gzip.NewWriterLevel(w, gzip.BestCompression)
	if err != nil {
		return err
	}
	// gzip.Writer writes no name and a zero time unless told otherwise. Set
	// both anyway, so a change to its defaults cannot change the bytes.
	gz.Header = gzip.Header{OS: 255}
	tw := tar.NewWriter(gz)

	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	dirs := map[string]bool{}
	header := func(h *tar.Header) error {
		h.ModTime = when
		h.Uid, h.Gid = 0, 0
		h.Uname, h.Gname = "", ""
		h.Format = tar.FormatPAX
		return tw.WriteHeader(h)
	}
	var addDir func(string) error
	addDir = func(d string) error {
		if d == "." || dirs[d] {
			return nil
		}
		if err := addDir(path.Dir(d)); err != nil {
			return err
		}
		dirs[d] = true
		return header(&tar.Header{Typeflag: tar.TypeDir, Name: prefix + "/" + d + "/", Mode: 0o755})
	}
	if err := header(&tar.Header{Typeflag: tar.TypeDir, Name: prefix + "/", Mode: 0o755}); err != nil {
		return err
	}
	for _, f := range files {
		if err := addDir(path.Dir(f.path)); err != nil {
			return err
		}
		h := &tar.Header{Name: prefix + "/" + f.path}
		switch f.mode {
		case "100644":
			h.Typeflag, h.Mode, h.Size = tar.TypeReg, 0o644, int64(len(f.data))
		case "100755":
			h.Typeflag, h.Mode, h.Size = tar.TypeReg, 0o755, int64(len(f.data))
		case "120000":
			h.Typeflag, h.Mode, h.Linkname = tar.TypeSymlink, 0o777, string(f.data)
		default:
			return fmt.Errorf("%s has git mode %s, which a release cannot hold", f.path, f.mode)
		}
		if err := header(h); err != nil {
			return err
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := tw.Write(f.data); err != nil {
				return err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

func git(args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return nil, fmt.Errorf("git %s: %s", strings.Join(args, " "), strings.TrimSpace(stderr.String()))
		}
		return nil, err
	}
	return out, nil
}
