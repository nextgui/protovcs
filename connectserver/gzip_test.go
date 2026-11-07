package connectserver_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"runtime"
	"testing"
	"time"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// An unauthenticated caller sends a small gzip body that inflates far past
// DefaultMaxRequestBytes. The server must refuse it without inflating it.
func TestACompressedRequestCannotInflatePastItsLimit(t *testing.T) {
	f := newFixture(t)

	var body bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&body, gzip.BestCompression)
	zeros := make([]byte, 1<<20)
	for i := 0; i < 1024; i++ { // 1 GiB of zeros
		zw.Write(zeros)
	}
	zw.Close()
	t.Logf("compressed body: %d bytes", body.Len())

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	req, _ := http.NewRequest("POST", f.url+pb.StoreServiceHasChunksProcedure, &body)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Content-Encoding", "gzip")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	runtime.ReadMemStats(&after)
	t.Logf("status %d: %s; TotalAlloc grew %d MiB", resp.StatusCode, msg, (after.TotalAlloc-before.TotalAlloc)>>20)
	d := time.Since(start)
	t.Logf("server took %v", d)
	if d > 200*time.Millisecond {
		t.Errorf("an unauthenticated 1 MB request kept the server inflating for %v (connect discards the rest of the stream to report its size)", d)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 64<<20 {
		t.Fatalf("server allocated %d MiB for a %d-byte request", grew>>20, body.Len())
	}
}
