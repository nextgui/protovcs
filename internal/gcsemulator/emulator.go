package gcsemulator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ObjectMetadata represents a GCS object metadata entry in JSON.
type ObjectMetadata struct {
	Kind           string `json:"kind"`
	Name           string `json:"name"`
	Bucket         string `json:"bucket"`
	Generation     int64  `json:"generation,string"`
	Metageneration int64  `json:"metageneration,string"`
	Size           int64  `json:"size,string"`
	ContentType    string `json:"contentType"`
	Updated        string `json:"updated"`
}

// ObjectList represents the JSON response for listing objects.
type ObjectList struct {
	Kind  string           `json:"kind"`
	Items []ObjectMetadata `json:"items"`
}

type storedObject struct {
	meta ObjectMetadata
	data []byte

	// keyHash is the base64 SHA-256 digest of the customer-supplied encryption
	// key the object was written with, or empty for an object written without
	// one. Cloud Storage keeps exactly this and no more of the key, which is why
	// a later read has to present the key again. See customer_key.go.
	keyHash string
}

// Server is an in-process GCS emulator.
type Server struct {
	// clock supplies an object's update time. Replaceable so a test can age an
	// object past a grace period without waiting for one — which is what
	// protovcs collection needs to tell an abandoned upload from a live one.
	clock func() time.Time

	mu      sync.Mutex
	server  *httptest.Server
	objects map[string]*storedObject // key: bucket/objectName
	buckets map[string]bool
	genSeq  int64
}

// NewServer starts and returns a new GCS emulator test server on a port the
// operating system chooses.
func NewServer() *Server {
	s := NewHandler()
	s.server = httptest.NewServer(http.HandlerFunc(s.handleHTTP))
	return s
}

// NewHandler returns an emulator that is not listening anywhere, for a caller
// that wants to choose the address itself.
//
// [Server.URL] and [Server.Close] are meaningless on the result; serve
// [Server.Handler] instead. This exists so the emulator can be run as a
// standalone process on a known port, which is what lets the control plane and
// the data plane be pointed at one another by hand.
func NewHandler() *Server {
	return &Server{
		clock:   time.Now,
		objects: make(map[string]*storedObject),
		buckets: make(map[string]bool),
	}
}

// SetObjectUpdated backdates an object's update time.
//
// Real Cloud Storage stamps an object when it is written and never afterwards,
// so code that decides whether something is old enough to delete has to be
// tested against an object that is genuinely old. Waiting is not an option and
// rewriting the object would give it a new stamp, which is precisely what such
// code must not be fooled by. This is the seam.
func (s *Server) SetObjectUpdated(bucket, name string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := bucket + "/" + name
	obj, ok := s.objects[key]
	if !ok {
		return fmt.Errorf("gcsemulator: no object %s", key)
	}
	obj.meta.Updated = at.UTC().Format(time.RFC3339Nano)
	return nil
}

// SetClock replaces the source of an object's update time.
//
// Real Cloud Storage stamps every object with when it was written, and code
// that decides whether something is old enough to delete reads that stamp. A
// test of such code has to be able to age an object rather than wait for one,
// which is what this is for.
func (s *Server) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = now
}

// Handler serves the emulator's HTTP API.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.handleHTTP)
}

// URL returns the base HTTP URL of the emulator.
func (s *Server) URL() string {
	return s.server.URL
}

// Close shuts down the emulator test server.
func (s *Server) Close() {
	s.server.Close()
}

func (s *Server) handleHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")

	// Format: upload/storage/v1/b/{bucket}/o or storage/v1/b/{bucket}/o/... or b/{bucket}/o/...
	if strings.HasPrefix(path, "upload/") {
		s.handleUpload(w, r)
		return
	}
	// The trailing slash is absent when the request addresses the bucket
	// collection rather than one bucket, which is how a bucket is created.
	if strings.HasPrefix(path, "storage/v1/b") || path == "b" || strings.HasPrefix(path, "b/") {
		s.handleStorage(w, r)
		return
	}

	// Direct raw object access for presigned/direct GET/PUT
	if r.Method == http.MethodPut {
		s.handleRawPut(w, r)
		return
	}
	if r.Method == http.MethodGet {
		s.handleRawGet(w, r)
		return
	}

	http.Error(w, "not found", http.StatusNotFound)
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	bucket := ""
	for i, p := range parts {
		if p == "b" && i+1 < len(parts) {
			bucket = parts[i+1]
			break
		}
	}
	if b, err := url.PathUnescape(bucket); err == nil {
		bucket = b
	}
	// The query value and the header value arrive already decoded, from net/url
	// and net/http. Only the name taken from the request path below still needs
	// unescaping.
	//
	// Decoding either of the first two a second time changes the object name. A
	// caller storing an object called "a%2Fb" would have it filed under "a/b",
	// and would then fail to read back what it had just written. Real Cloud
	// Storage decodes once and allows a percent sign in an object name.
	name := r.URL.Query().Get("name")
	if name == "" {
		name = r.Header.Get("X-Goog-Upload-File-Name")
	}
	if name == "" {
		for i, p := range parts {
			if p == "o" && i+1 < len(parts) {
				name = strings.Join(parts[i+1:], "/")
				break
			}
		}
		if unescaped, err := url.PathUnescape(name); err == nil {
			name = unescaped
		}
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "multipart/") {
		parts := bytes.Split(data, []byte("\r\n\r\n"))
		if len(parts) >= 2 && name == "" {
			var m struct {
				Name string `json:"name"`
			}
			if idx := bytes.Index(parts[1], []byte("\r\n--")); idx != -1 {
				_ = json.Unmarshal(parts[1][:idx], &m)
			} else {
				_ = json.Unmarshal(parts[1], &m)
			}
			if m.Name != "" {
				name = m.Name
			}
		}
		if len(parts) >= 3 {
			rawContent := parts[2]
			if idx := bytes.Index(rawContent, []byte("\r\n--")); idx != -1 {
				data = rawContent[:idx]
			}
		}
	}

	// The customer-supplied key is read before anything is stored, so an upload
	// with malformed key headers writes nothing.
	customerKeyHash, err := readCustomerKey(r)
	if err != nil && !errors.Is(err, errNoCustomerKey) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ifGenMatch := r.URL.Query().Get("ifGenerationMatch")
	key := fmt.Sprintf("%s/%s", bucket, name)
	existing, exists := s.objects[key]

	if ifGenMatch != "" {
		matchGen, _ := strconv.ParseInt(ifGenMatch, 10, 64)
		if matchGen == 0 && exists {
			http.Error(w, "Precondition Failed (generation match 0 but exists)", http.StatusPreconditionFailed)
			return
		}
		if matchGen > 0 && (!exists || existing.meta.Generation != matchGen) {
			http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
			return
		}
	}

	s.genSeq++
	meta := ObjectMetadata{
		Kind:       "storage#object",
		Name:       name,
		Bucket:     bucket,
		Generation: s.genSeq,
		Size:       int64(len(data)),
		Updated:    s.clock().UTC().Format(time.RFC3339Nano),
	}

	s.objects[key] = &storedObject{
		meta:    meta,
		data:    data,
		keyHash: customerKeyHash.hash,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}

// handleBucket answers the two bucket-level routes the GitService provisioner
// uses: create a bucket, and ask whether one exists. Everything about a bucket
// except its name is discarded, because nothing the emulator serves behaves
// differently for a storage class or a lifecycle rule. It is called with s.mu
// held.
//
// name is empty for a create, which addresses the collection rather than one
// bucket.
func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request, name string) {
	switch {
	case name == "" && r.Method == http.MethodPost:
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
			http.Error(w, "bucket resource needs a name", http.StatusBadRequest)
			return
		}
		if s.buckets[req.Name] {
			// The real API reports a name already taken as a conflict, which is
			// what lets two provisioners race and still produce one bucket.
			http.Error(w, "bucket already exists", http.StatusConflict)
			return
		}
		s.buckets[req.Name] = true
		writeBucket(w, req.Name)

	case name != "" && r.Method == http.MethodGet:
		if !s.buckets[name] {
			http.Error(w, "no such bucket", http.StatusNotFound)
			return
		}
		writeBucket(w, name)

	default:
		http.Error(w, "unsupported bucket request", http.StatusBadRequest)
	}
}

// writeBucket answers with the minimal Bucket resource a caller needs to see
// that the bucket is there.
func writeBucket(w http.ResponseWriter, name string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"kind": "storage#bucket", "name": name})
}

func (s *Server) handleStorage(w http.ResponseWriter, r *http.Request) {
	rawPath := r.URL.RawPath
	if rawPath == "" {
		rawPath = r.URL.Path
	}
	p := strings.TrimPrefix(rawPath, "/")
	p = strings.TrimPrefix(p, "storage/v1/")
	p = strings.TrimPrefix(p, "b")
	p = strings.TrimPrefix(p, "/")
	parts := strings.Split(p, "/")

	s.mu.Lock()
	defer s.mu.Unlock()

	// Bucket-level requests: create is POST with no bucket in the path, get is
	// GET with only a bucket. Both are answered before the object routes below,
	// whose paths always carry a second segment.
	if len(parts) == 1 {
		name, _ := url.PathUnescape(parts[0])
		s.handleBucket(w, r, name)
		return
	}
	if len(parts) < 2 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	bucket, _ := url.PathUnescape(parts[0])
	subPath := strings.Join(parts[1:], "/")

	if subPath == "o" && r.Method == http.MethodGet {
		// List objects.
		//
		// startOffset and endOffset bound the listing lexicographically, the
		// half-open range [startOffset, endOffset). Without them a caller that
		// relies on the server to bound a listing gets everything under the
		// prefix and only notices in production, where the listing is large.
		// GitService's retention reaper is exactly that caller: it encodes the
		// idle timestamp in the object name and asks GCS for the names below a
		// cutoff.
		q := r.URL.Query()
		prefix := q.Get("prefix")
		startOffset := q.Get("startOffset")
		endOffset := q.Get("endOffset")

		var items []ObjectMetadata
		for k, obj := range s.objects {
			if strings.HasPrefix(k, bucket+"/") {
				objName := strings.TrimPrefix(k, bucket+"/")
				if prefix != "" && !strings.HasPrefix(objName, prefix) {
					continue
				}
				if startOffset != "" && objName < startOffset {
					continue
				}
				if endOffset != "" && objName >= endOffset {
					continue
				}
				items = append(items, obj.meta)
			}
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].Name < items[j].Name
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ObjectList{
			Kind:  "storage#objects",
			Items: items,
		})
		return
	}

	// Compose addresses the destination object and then a verb, so it is
	// answered before the plain object routes below — those would otherwise
	// read the trailing "/compose" as part of the object's name.
	if r.Method == http.MethodPost && strings.HasPrefix(subPath, "o/") && strings.HasSuffix(subPath, "/compose") {
		name := strings.TrimSuffix(strings.TrimPrefix(subPath, "o/"), "/compose")
		if unescaped, err := url.PathUnescape(name); err == nil {
			name = unescaped
		}
		s.handleCompose(w, r, bucket, name)
		return
	}

	if strings.HasPrefix(subPath, "o/") {
		objectName := strings.TrimPrefix(subPath, "o/")
		if unescaped, err := url.PathUnescape(objectName); err == nil {
			objectName = unescaped
		}
		key := fmt.Sprintf("%s/%s", bucket, objectName)
		obj, exists := s.objects[key]

		if r.Method == http.MethodDelete {
			ifGenMatch := r.URL.Query().Get("ifGenerationMatch")
			if ifGenMatch != "" {
				matchGen, _ := strconv.ParseInt(ifGenMatch, 10, 64)
				if !exists || obj.meta.Generation != matchGen {
					http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
					return
				}
			}
			delete(s.objects, key)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if r.Method == http.MethodGet {
			if !exists {
				http.Error(w, "Not Found", http.StatusNotFound)
				return
			}
			// Both the bytes and the metadata of a customer-encrypted object
			// need the key, as they do in Cloud Storage. Listing does not, which
			// is why the branch above this one has no such check.
			if !checkCustomerKey(w, r, obj.keyHash) {
				return
			}

			if r.URL.Query().Get("alt") == "media" {
				s.serveContent(w, r, obj.data, obj.meta.Generation)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(obj.meta)
			return
		}
	}

	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

// composeRequest is the body of a compose call.
//
// Only the source names are read. Cloud Storage also accepts a per-source
// generation and a destination metadata block, and nothing in this repository
// sends either.
type composeRequest struct {
	SourceObjects []struct {
		Name string `json:"name"`
	} `json:"sourceObjects"`
}

// maxComposeSources is how many objects Cloud Storage will join in one call.
//
// Enforced here because a caller that exceeds it must fail in a test the same
// way it would fail in production. An emulator that quietly joined 40 objects
// would hide the one bug this limit causes.
const maxComposeSources = 32

// handleCompose joins objects into dest, the way appending to an object is done
// on a store whose objects cannot be modified.
//
// The caller holds s.mu.
func (s *Server) handleCompose(w http.ResponseWriter, r *http.Request, bucket, dest string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req composeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.SourceObjects) == 0 {
		http.Error(w, "compose needs at least one source", http.StatusBadRequest)
		return
	}
	if len(req.SourceObjects) > maxComposeSources {
		http.Error(w, "too many source objects", http.StatusBadRequest)
		return
	}

	destKey := bucket + "/" + dest
	existing, exists := s.objects[destKey]

	// ifGenerationMatch=0 means the object must not exist, which is how a
	// caller creates one and refuses to overwrite. Any other value means it
	// must still be at that generation.
	if want := r.URL.Query().Get("ifGenerationMatch"); want != "" {
		wantGen, err := strconv.ParseInt(want, 10, 64)
		if err != nil {
			http.Error(w, "bad ifGenerationMatch", http.StatusBadRequest)
			return
		}
		if wantGen == 0 && exists {
			http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
			return
		}
		if wantGen != 0 && (!exists || existing.meta.Generation != wantGen) {
			http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
			return
		}
	}

	// Every source is read before dest is written. Naming dest as a source is
	// how an object is appended to, so writing it first would compose the
	// remaining sources onto bytes this call had already replaced.
	var joined []byte
	for _, src := range req.SourceObjects {
		obj, ok := s.objects[bucket+"/"+src.Name]
		if !ok {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		joined = append(joined, obj.data...)
	}

	s.genSeq++
	meta := ObjectMetadata{
		Kind:           "storage#object",
		Name:           dest,
		Bucket:         bucket,
		Generation:     s.genSeq,
		Metageneration: 1,
		Size:           int64(len(joined)),
		Updated:        s.clock().UTC().Format(time.RFC3339Nano),
	}
	s.objects[destKey] = &storedObject{meta: meta, data: joined}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(meta)
}

func (s *Server) handleRawPut(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	customerKeyHash, err := readCustomerKey(r)
	if err != nil && !errors.Is(err, errNoCustomerKey) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/")
	s.genSeq++
	s.objects[key] = &storedObject{
		meta: ObjectMetadata{
			Name:       key,
			Generation: s.genSeq,
			Size:       int64(len(data)),
			Updated:    s.clock().UTC().Format(time.RFC3339Nano),
		},
		data:    data,
		keyHash: customerKeyHash.hash,
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleRawGet(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/")
	obj, exists := s.objects[key]
	if !exists {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	if !checkCustomerKey(w, r, obj.keyHash) {
		return
	}
	s.serveContent(w, r, obj.data, obj.meta.Generation)
}

// serveContent writes an object's bytes, honouring a Range header.
//
// The generation goes out in X-Goog-Generation, which is where the Google
// Cloud Storage client reads it from when a caller does not pin one. A client
// doing compare-and-swap reads the bytes and that generation from this one
// response and writes back with a GenerationMatch precondition, so leaving the
// header off would hand it generation 0 and make the precondition unusable.
func (s *Server) serveContent(w http.ResponseWriter, r *http.Request, data []byte, generation int64) {
	w.Header().Set("X-Goog-Generation", strconv.FormatInt(generation, 10))
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
		rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeSpec, "-")
		if len(parts) == 2 {
			size := int64(len(data))
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			end := size - 1
			if parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}

			// A range starting at or past the end is unsatisfiable, and Cloud
			// Storage says so with 416 rather than by ignoring the header and
			// sending the object. Serving the whole object instead would be
			// worse than a wrong status: a reader tailing a log from the offset
			// it had reached would be handed the log from the beginning again,
			// and would print all of it a second time.
			if start >= size || start < 0 {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			// An end past the last byte is clamped, as Cloud Storage clamps it.
			// Asking for more than is there is how a reader says "everything
			// from here", not a mistake.
			if end >= size {
				end = size - 1
			}
			if start <= end {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
				w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(data[start : end+1])
				return
			}
		}
	}

	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// Clear removes all stored objects from the emulator.
func (s *Server) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects = make(map[string]*storedObject)
}
