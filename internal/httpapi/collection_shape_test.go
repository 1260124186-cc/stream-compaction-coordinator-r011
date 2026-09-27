package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// These checks pin the external shape of public collection fields: empty
// collections must always render as [] (or {} for maps), never null, and the
// shape must be identical across create responses, detail reads, list reads,
// restarts, and appends.

type shapeHarness struct {
	t         *testing.T
	statePath string
	ids       *idgen.Counter
	timeSrc   *clock.Fixed
}

func newShapeHarness(t *testing.T) *shapeHarness {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "state.json")
	return &shapeHarness{
		t:         t,
		statePath: statePath,
		ids:       &idgen.Counter{},
		timeSrc:   clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Second),
	}
}

// serve builds a fresh service over the same snapshot file, which mirrors a
// process restart when called again after mutations.
func (h *shapeHarness) serve() *httptest.Server {
	h.t.Helper()
	repo, err := storage.NewFileRepository(h.statePath)
	if err != nil {
		h.t.Fatalf("NewFileRepository: %v", err)
	}
	service, err := application.NewService(
		repo,
		h.ids,
		h.timeSrc,
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		h.t.Fatalf("NewService: %v", err)
	}
	api := httpapi.New(service, httpapi.Options{Logger: observability.Discard{}})
	server := httptest.NewServer(api.Handler())
	h.t.Cleanup(server.Close)
	return server
}

func (h *shapeHarness) call(
	server *httptest.Server,
	method string,
	path string,
	body any,
	wantStatus int,
) any {
	h.t.Helper()
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, server.URL+path, reader)
	if err != nil {
		h.t.Fatalf("build request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := server.Client().Do(request)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		h.t.Fatalf("read response: %v", err)
	}
	if response.StatusCode != wantStatus {
		h.t.Fatalf("%s %s returned %d, want %d: %s", method, path, response.StatusCode, wantStatus, data)
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		h.t.Fatalf("decode response for %s %s: %v", method, path, err)
	}
	return decoded
}

func field(t *testing.T, value any, path ...string) any {
	t.Helper()
	current := value
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("path %q: expected object at %q, got %T", strings.Join(path, "."), key, current)
		}
		child, exists := object[key]
		if !exists {
			t.Fatalf("path %q: missing key %q", strings.Join(path, "."), key)
		}
		current = child
	}
	return current
}

// requireArray fails when the value is null, missing, or not a JSON array.
func requireArray(t *testing.T, value any, path ...string) []any {
	t.Helper()
	raw := field(t, value, path...)
	array, ok := raw.([]any)
	if !ok {
		t.Fatalf("path %q: expected JSON array, got %T (%v)", strings.Join(path, "."), raw, raw)
	}
	if array == nil {
		t.Fatalf("path %q: collection rendered as null instead of []", strings.Join(path, "."))
	}
	return array
}

func requireObject(t *testing.T, value any, path ...string) map[string]any {
	t.Helper()
	raw := field(t, value, path...)
	object, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("path %q: expected JSON object, got %T (%v)", strings.Join(path, "."), raw, raw)
	}
	if object == nil {
		t.Fatalf("path %q: collection rendered as null instead of {}", strings.Join(path, "."))
	}
	return object
}

func requireString(t *testing.T, value any, path ...string) string {
	t.Helper()
	raw := field(t, value, path...)
	text, ok := raw.(string)
	if !ok || text == "" {
		t.Fatalf("path %q: expected non-empty string, got %T (%v)", strings.Join(path, "."), raw, raw)
	}
	return text
}

// checkStreamShape asserts the stream collection shape on a stream document.
func checkStreamShape(t *testing.T, stream any, where string, wantSealed []string) {
	t.Helper()
	sealed := requireArray(t, stream, "sealed_segment_ids")
	if len(sealed) != len(wantSealed) {
		t.Fatalf("%s: sealed_segment_ids = %v, want %v", where, sealed, wantSealed)
	}
	for index, want := range wantSealed {
		if sealed[index] != want {
			t.Fatalf("%s: sealed_segment_ids[%d] = %v, want %v", where, index, sealed[index], want)
		}
	}
}

// checkSegmentShape asserts the segment collection shape on a segment document.
func checkSegmentShape(t *testing.T, segment any, where string) {
	t.Helper()
	requireArray(t, segment, "source_segment_ids")
	requireObject(t, segment, "replica_acks")
}

// checkStreamReads runs the detail and list reads that must keep a stable
// collection shape at any point in the stream lifecycle.
func checkStreamReads(
	t *testing.T,
	server *httptest.Server,
	call func(*httptest.Server, string, string, any, int) any,
	streamID string,
	activeSegmentID string,
	wantSealed []string,
	wantSegmentCount int,
) {
	t.Helper()
	where := fmt.Sprintf("GET /v1/streams/%s", streamID)
	stream := call(server, http.MethodGet, "/v1/streams/"+streamID, nil, http.StatusOK)
	checkStreamShape(t, stream, where, wantSealed)

	where = fmt.Sprintf("GET /v1/segments/%s", activeSegmentID)
	segment := call(server, http.MethodGet, "/v1/segments/"+activeSegmentID, nil, http.StatusOK)
	checkSegmentShape(t, segment, where)

	where = fmt.Sprintf("GET /v1/streams/%s/segments", streamID)
	list := call(server, http.MethodGet, "/v1/streams/"+streamID+"/segments?limit=100", nil, http.StatusOK)
	items := requireArray(t, list, "items")
	if len(items) != wantSegmentCount {
		t.Fatalf("%s: items has %d entries, want %d", where, len(items), wantSegmentCount)
	}
	for index, item := range items {
		checkSegmentShape(t, item, fmt.Sprintf("%s items[%d]", where, index))
	}
}

func TestCollectionFieldsKeepArrayShape(t *testing.T) {
	h := newShapeHarness(t)

	// Group 1: creation response.
	server := h.serve()
	created := h.call(server, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       "ops",
		"name":            "events",
		"threshold_bytes": 100,
	}, http.StatusCreated)
	stream := field(t, created, "stream")
	segment := field(t, created, "segment")
	checkStreamShape(t, stream, "POST /v1/streams stream", nil)
	checkSegmentShape(t, segment, "POST /v1/streams segment")
	streamID := requireString(t, stream, "id")
	firstSegmentID := requireString(t, segment, "id")

	// Group 2: first reads, which already cross one persistence round.
	checkStreamReads(t, server, h.call, streamID, firstSegmentID, nil, 1)

	// Group 3: restart recovery over the same snapshot file.
	restarted := h.serve()
	checkStreamReads(t, restarted, h.call, streamID, firstSegmentID, nil, 1)

	// Group 4a: an append below the rollover threshold keeps collections empty.
	appended := h.call(restarted, http.MethodPost, "/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": 1,
		"record_count":      2,
		"payload_bytes":     40,
	}, http.StatusOK)
	checkStreamShape(t, field(t, appended, "stream"), "POST records stream", nil)
	checkSegmentShape(t, field(t, appended, "segment"), "POST records segment")
	if rolled, ok := field(t, appended, "rolled_over").(bool); !ok || rolled {
		t.Fatalf("POST records rolled_over = %v, want false", field(t, appended, "rolled_over"))
	}
	checkStreamReads(t, restarted, h.call, streamID, firstSegmentID, nil, 1)

	// Group 4b: an append that rolls over keeps order and content of the
	// sealed list while new collections stay empty arrays.
	rolled := h.call(restarted, http.MethodPost, "/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": 2,
		"record_count":      1,
		"payload_bytes":     60,
	}, http.StatusOK)
	checkStreamShape(t, field(t, rolled, "stream"), "POST rollover stream", []string{firstSegmentID})
	checkSegmentShape(t, field(t, rolled, "segment"), "POST rollover segment")
	if flag, ok := field(t, rolled, "rolled_over").(bool); !ok || !flag {
		t.Fatalf("POST rollover rolled_over = %v, want true", field(t, rolled, "rolled_over"))
	}
	activeSegmentID := requireString(t, field(t, rolled, "segment"), "id")
	checkStreamReads(t, restarted, h.call, streamID, activeSegmentID, []string{firstSegmentID}, 2)

	// A restart after the appends must serve identical shapes from the
	// recovered snapshot.
	recovered := h.serve()
	checkStreamReads(t, recovered, h.call, streamID, activeSegmentID, []string{firstSegmentID}, 2)

	// Business rules are unchanged: a stale revision is still a conflict.
	conflict := h.call(recovered, http.MethodPost, "/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": 1,
		"record_count":      1,
		"payload_bytes":     10,
	}, http.StatusConflict)
	if code := requireString(t, conflict, "error", "code"); code != "stale_revision" {
		t.Fatalf("stale append error code = %q, want stale_revision", code)
	}

	// The durable snapshot itself must keep empty arrays after saves, so a
	// later process never decodes null collections.
	data, err := os.ReadFile(h.statePath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	for _, fieldName := range []string{"sealed_segment_ids", "source_segment_ids", "replica_acks"} {
		if bytes.Contains(data, []byte(`"`+fieldName+`": null`)) {
			t.Fatalf("snapshot stores %q as null:\n%s", fieldName, data)
		}
	}
	if !bytes.Contains(data, []byte(`"source_segment_ids": []`)) {
		t.Fatalf("snapshot lost the empty source_segment_ids array:\n%s", data)
	}
}
