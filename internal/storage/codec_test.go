package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func newCodecTestState(t *testing.T) *domain.State {
	t.Helper()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	state := domain.NewState(now)
	stream, segment, err := domain.NewStream("str_1", "seg_1", "ops", "events", 1024, now)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	state.Streams[stream.ID] = stream
	state.Segments[segment.ID] = segment
	return state
}

func TestMarshalStateEmitsEmptyCollectionsNotNull(t *testing.T) {
	data, err := MarshalState(newCodecTestState(t))
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}
	for _, field := range []string{
		"sealed_segment_ids",
		"source_segment_ids",
		"replica_acks",
		"audit",
	} {
		if bytes.Contains(data, []byte(fmt.Sprintf("%q: null", field))) {
			t.Fatalf("snapshot field %q must not be null:\n%s", field, data)
		}
	}
	for _, want := range []string{
		`"sealed_segment_ids": []`,
		`"source_segment_ids": []`,
		`"replica_acks": {}`,
		`"audit": []`,
	} {
		if !bytes.Contains(data, []byte(want)) {
			t.Fatalf("snapshot must contain %s:\n%s", want, data)
		}
	}
}

func TestUnmarshalStateNormalizesNullCollections(t *testing.T) {
	data, err := MarshalState(newCodecTestState(t))
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}

	// Rewrite every collection member to null, mimicking a snapshot written
	// before empty collections were normalized.
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode marshaled state: %v", err)
	}
	for _, raw := range document["streams"].(map[string]any) {
		raw.(map[string]any)["sealed_segment_ids"] = nil
	}
	for _, raw := range document["segments"].(map[string]any) {
		segment := raw.(map[string]any)
		segment["source_segment_ids"] = nil
		segment["replica_acks"] = nil
	}
	document["audit"] = nil
	legacy, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("encode legacy snapshot: %v", err)
	}

	state, err := UnmarshalState(legacy)
	if err != nil {
		t.Fatalf("UnmarshalState: %v", err)
	}
	for id, stream := range state.Streams {
		if stream.SealedSegmentIDs == nil {
			t.Errorf("stream %s sealed_segment_ids must decode as an empty slice, got nil", id)
		}
	}
	for id, segment := range state.Segments {
		if segment.SourceSegmentIDs == nil {
			t.Errorf("segment %s source_segment_ids must decode as an empty slice, got nil", id)
		}
		if segment.ReplicaAcks == nil {
			t.Errorf("segment %s replica_acks must decode as an empty map, got nil", id)
		}
	}
	if state.Audit == nil {
		t.Error("audit log must decode as an empty slice, got nil")
	}
	if err := domain.ValidateState(state); err != nil {
		t.Fatalf("normalized snapshot must pass validation: %v", err)
	}
}

func TestStateRoundTripKeepsCollectionShape(t *testing.T) {
	first, err := MarshalState(newCodecTestState(t))
	if err != nil {
		t.Fatalf("MarshalState: %v", err)
	}
	state, err := UnmarshalState(first)
	if err != nil {
		t.Fatalf("UnmarshalState: %v", err)
	}
	second, err := MarshalState(state)
	if err != nil {
		t.Fatalf("MarshalState after decode: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("snapshot round trip changed the document:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}
