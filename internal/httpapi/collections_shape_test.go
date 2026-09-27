package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// arrayCollectionKeys are the public fields that must always serialize as
// JSON arrays, never null, even when they have no elements.
var arrayCollectionKeys = map[string]bool{
	"sealed_segment_ids":    true,
	"source_segment_ids":    true,
	"items":                 true,
	"aborted_plan_ids":      true,
	"recomputed_stream_ids": true,
}

// objectCollectionKeys are the public map fields that must always serialize as
// JSON objects, never null.
var objectCollectionKeys = map[string]bool{
	"replica_acks":       true,
	"source_generations": true,
}

func startTestServer(t *testing.T, statePath string, ids *idgen.Counter) *httptest.Server {
	t.Helper()
	repository, err := storage.NewFileRepository(statePath)
	if err != nil {
		t.Fatalf("NewFileRepository: %v", err)
	}
	service, err := application.NewService(
		repository,
		ids,
		clock.NewFixed(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), time.Second),
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	api := httpapi.New(service, httpapi.Options{})
	return httptest.NewServer(api.Handler())
}

func callJSON(t *testing.T, method string, url string, body any, wantStatus int) map[string]any {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		t.Fatalf("read response for %s %s: %v", method, url, err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s returned %d, want %d: %s", method, url, response.StatusCode, wantStatus, data)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode response for %s %s: %v\n%s", method, url, err, data)
	}
	return document
}

func requireCollectionShapes(t *testing.T, label string, document any) {
	t.Helper()
	switch value := document.(type) {
	case map[string]any:
		for key, item := range value {
			if arrayCollectionKeys[key] {
				if _, ok := item.([]any); !ok {
					t.Fatalf("%s: field %q must be a JSON array, got %T (%v)", label, key, item, item)
				}
			}
			if objectCollectionKeys[key] {
				if _, ok := item.(map[string]any); !ok {
					t.Fatalf("%s: field %q must be a JSON object, got %T (%v)", label, key, item, item)
				}
			}
			requireCollectionShapes(t, label, item)
		}
	case []any:
		for _, item := range value {
			requireCollectionShapes(t, label, item)
		}
	}
}

func requireObject(t *testing.T, label string, value any) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s must be a JSON object, got %T (%v)", label, value, value)
	}
	return object
}

func requireArray(t *testing.T, label string, value any) []any {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("%s must be a JSON array, got %T (%v)", label, value, value)
	}
	return items
}

func requireString(t *testing.T, label string, value any) string {
	t.Helper()
	text, ok := value.(string)
	if !ok || text == "" {
		t.Fatalf("%s must be a non-empty string, got %T (%v)", label, value, value)
	}
	return text
}

func requireEmptyArray(t *testing.T, label string, value any) {
	t.Helper()
	if items := requireArray(t, label, value); len(items) != 0 {
		t.Fatalf("%s must be an empty array, got %v", label, items)
	}
}

func TestCollectionFieldsKeepArrayShapeAcrossWorkflow(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	ids := &idgen.Counter{}

	server := startTestServer(t, statePath, ids)

	// --- Group 1: create a stream and its active segment -------------------
	created := callJSON(t, http.MethodPost, server.URL+"/v1/streams", map[string]any{
		"namespace":       "ops",
		"name":            "events",
		"threshold_bytes": 100,
	}, http.StatusCreated)
	requireCollectionShapes(t, "create stream", created)

	stream := requireObject(t, "create stream.stream", created["stream"])
	streamID := requireString(t, "stream.id", stream["id"])
	requireEmptyArray(t, "stream.sealed_segment_ids", stream["sealed_segment_ids"])
	segment := requireObject(t, "create stream.segment", created["segment"])
	segmentID := requireString(t, "segment.id", segment["id"])
	requireEmptyArray(t, "segment.source_segment_ids", segment["source_segment_ids"])
	if acks := requireObject(t, "segment.replica_acks", segment["replica_acks"]); len(acks) != 0 {
		t.Fatalf("segment.replica_acks must be an empty object, got %v", acks)
	}
	// Non-collection fields keep their existing values.
	if stream["revision"] != float64(1) {
		t.Fatalf("stream.revision = %v, want 1", stream["revision"])
	}
	if stream["active_segment_id"] != segmentID {
		t.Fatalf("stream.active_segment_id = %v, want %s", stream["active_segment_id"], segmentID)
	}
	if segment["state"] != "open" {
		t.Fatalf("segment.state = %v, want open", segment["state"])
	}

	// --- Group 2: first reads ----------------------------------------------
	readStream := callJSON(t, http.MethodGet, server.URL+"/v1/streams/"+streamID, nil, http.StatusOK)
	requireCollectionShapes(t, "get stream", readStream)
	requireEmptyArray(t, "get stream.sealed_segment_ids", readStream["sealed_segment_ids"])

	readSegment := callJSON(t, http.MethodGet, server.URL+"/v1/segments/"+segmentID, nil, http.StatusOK)
	requireCollectionShapes(t, "get segment", readSegment)
	requireEmptyArray(t, "get segment.source_segment_ids", readSegment["source_segment_ids"])

	listed := callJSON(t, http.MethodGet, server.URL+"/v1/streams/"+streamID+"/segments", nil, http.StatusOK)
	requireCollectionShapes(t, "list segments", listed)
	items := requireArray(t, "list segments.items", listed["items"])
	if len(items) != 1 {
		t.Fatalf("list segments returned %d items, want 1", len(items))
	}
	requireEmptyArray(t, "listed segment.source_segment_ids", requireObject(t, "item", items[0])["source_segment_ids"])

	emptyList := callJSON(t, http.MethodGet, server.URL+"/v1/streams/"+streamID+"/segments?states=retired", nil, http.StatusOK)
	requireCollectionShapes(t, "list segments without matches", emptyList)
	requireEmptyArray(t, "empty list.items", emptyList["items"])

	// --- Group 3: restart recovery -----------------------------------------
	server.Close()
	server = startTestServer(t, statePath, ids)
	defer server.Close()

	afterRestart := callJSON(t, http.MethodGet, server.URL+"/v1/streams/"+streamID, nil, http.StatusOK)
	requireCollectionShapes(t, "get stream after restart", afterRestart)
	requireEmptyArray(t, "after restart stream.sealed_segment_ids", afterRestart["sealed_segment_ids"])
	if !reflect.DeepEqual(afterRestart, readStream) {
		t.Fatalf("restart changed the stream document:\nbefore: %v\nafter:  %v", readStream, afterRestart)
	}

	// --- Group 4: one append, then a rollover append -----------------------
	appended := callJSON(t, http.MethodPost, server.URL+"/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": 1,
		"record_count":      5,
		"payload_bytes":     40,
	}, http.StatusOK)
	requireCollectionShapes(t, "append records", appended)
	if appended["rolled_over"] != false {
		t.Fatalf("append below threshold rolled over: %v", appended["rolled_over"])
	}
	appendedStream := requireObject(t, "append.stream", appended["stream"])
	requireEmptyArray(t, "append stream.sealed_segment_ids", appendedStream["sealed_segment_ids"])
	appendedSegment := requireObject(t, "append.segment", appended["segment"])
	requireEmptyArray(t, "append segment.source_segment_ids", appendedSegment["source_segment_ids"])

	reread := callJSON(t, http.MethodGet, server.URL+"/v1/streams/"+streamID, nil, http.StatusOK)
	requireCollectionShapes(t, "get stream after append", reread)
	requireEmptyArray(t, "after append stream.sealed_segment_ids", reread["sealed_segment_ids"])

	// The durable snapshot itself must keep empty collections as arrays.
	snapshot, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	var snapshotDocument any
	if err := json.Unmarshal(snapshot, &snapshotDocument); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	requireCollectionShapes(t, "persisted snapshot", snapshotDocument)

	// A rollover append keeps the order and content of non-empty collections.
	rolled := callJSON(t, http.MethodPost, server.URL+"/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": 2,
		"record_count":      7,
		"payload_bytes":     80,
	}, http.StatusOK)
	requireCollectionShapes(t, "append with rollover", rolled)
	if rolled["rolled_over"] != true {
		t.Fatalf("append reaching the threshold did not roll over: %v", rolled["rolled_over"])
	}
	rolledStream := requireObject(t, "rollover.stream", rolled["stream"])
	sealed := requireArray(t, "rollover stream.sealed_segment_ids", rolledStream["sealed_segment_ids"])
	if len(sealed) != 1 || sealed[0] != segmentID {
		t.Fatalf("sealed_segment_ids = %v, want [%s]", sealed, segmentID)
	}
	requireEmptyArray(t, "rollover segment.source_segment_ids", requireObject(t, "rollover.segment", rolled["segment"])["source_segment_ids"])

	// The recovery report exposes collection fields as arrays as well.
	recovered := callJSON(t, http.MethodPost, server.URL+"/v1/admin/recover", map[string]any{}, http.StatusOK)
	requireCollectionShapes(t, "recover", recovered)
	requireEmptyArray(t, "recover.aborted_plan_ids", recovered["aborted_plan_ids"])
	requireEmptyArray(t, "recover.recomputed_stream_ids", recovered["recomputed_stream_ids"])

	// --- Business conflict rules are unchanged ------------------------------
	stale := callJSON(t, http.MethodPost, server.URL+"/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": 1,
		"record_count":      1,
		"payload_bytes":     10,
	}, http.StatusConflict)
	if code := requireObject(t, "error", stale["error"])["code"]; code != "stale_revision" {
		t.Fatalf("stale append error code = %v, want stale_revision", code)
	}

	duplicate := callJSON(t, http.MethodPost, server.URL+"/v1/streams", map[string]any{
		"namespace":       "ops",
		"name":            "events",
		"threshold_bytes": 100,
	}, http.StatusConflict)
	if code := requireObject(t, "error", duplicate["error"])["code"]; code != "duplicate" {
		t.Fatalf("duplicate stream error code = %v, want duplicate", code)
	}

	invalid := callJSON(t, http.MethodPost, server.URL+"/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": 3,
		"record_count":      0,
		"payload_bytes":     10,
	}, http.StatusBadRequest)
	if code := requireObject(t, "error", invalid["error"])["code"]; code != "invalid_input" {
		t.Fatalf("zero-record append error code = %v, want invalid_input", code)
	}

	missing := callJSON(t, http.MethodGet, server.URL+"/v1/streams/str_missing", nil, http.StatusNotFound)
	if code := requireObject(t, "error", missing["error"])["code"]; code != "not_found" {
		t.Fatalf("missing stream error code = %v, want not_found", code)
	}
}
