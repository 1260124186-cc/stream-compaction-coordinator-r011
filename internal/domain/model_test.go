package domain

import (
	"reflect"
	"testing"
	"time"
)

var cloneTestNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func TestCloneStatePreservesEmptyCollections(t *testing.T) {
	state := NewState(cloneTestNow)
	stream, segment, err := NewStream("str_1", "seg_1", "ops", "events", 1024, cloneTestNow)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	state.Streams[stream.ID] = stream
	state.Segments[segment.ID] = segment

	clone := CloneState(state)

	clonedStream := clone.Streams[stream.ID]
	if clonedStream == nil {
		t.Fatal("cloned stream is missing")
	}
	if clonedStream.SealedSegmentIDs == nil {
		t.Error("sealed_segment_ids must remain an empty slice after cloning, got nil")
	}
	if len(clonedStream.SealedSegmentIDs) != 0 {
		t.Errorf("sealed_segment_ids must be empty, got %v", clonedStream.SealedSegmentIDs)
	}

	clonedSegment := clone.Segments[segment.ID]
	if clonedSegment == nil {
		t.Fatal("cloned segment is missing")
	}
	if clonedSegment.SourceSegmentIDs == nil {
		t.Error("source_segment_ids must remain an empty slice after cloning, got nil")
	}
	if clonedSegment.ReplicaAcks == nil {
		t.Error("replica_acks must remain an empty map after cloning, got nil")
	}
	if clone.Audit == nil {
		t.Error("audit log must remain an empty slice after cloning, got nil")
	}
}

func TestCloneStateCanonicalizesNilCollections(t *testing.T) {
	state := NewState(cloneTestNow)
	// Collection fields are intentionally nil, as decoded from a snapshot
	// written before empty collections were normalized.
	state.Streams["str_1"] = &Stream{ID: "str_1"}
	state.Segments["seg_1"] = &Segment{ID: "seg_1"}
	state.Plans["cmp_1"] = &CompactionPlan{ID: "cmp_1"}

	clone := CloneState(state)

	if clone.Streams["str_1"].SealedSegmentIDs == nil {
		t.Error("cloned stream sealed_segment_ids must be an empty slice, got nil")
	}
	if clone.Segments["seg_1"].SourceSegmentIDs == nil {
		t.Error("cloned segment source_segment_ids must be an empty slice, got nil")
	}
	if clone.Segments["seg_1"].ReplicaAcks == nil {
		t.Error("cloned segment replica_acks must be an empty map, got nil")
	}
	if clone.Plans["cmp_1"].SourceSegmentIDs == nil {
		t.Error("cloned plan source_segment_ids must be an empty slice, got nil")
	}
	if clone.Plans["cmp_1"].SourceGenerations == nil {
		t.Error("cloned plan source_generations must be an empty map, got nil")
	}
}

func TestCloneStateKeepsCollectionOrderAndContent(t *testing.T) {
	state := NewState(cloneTestNow)
	state.Streams["str_1"] = &Stream{
		ID:               "str_1",
		SealedSegmentIDs: []string{"seg_b", "seg_a", "seg_c"},
	}

	clone := CloneState(state)
	got := clone.Streams["str_1"].SealedSegmentIDs
	want := []string{"seg_b", "seg_a", "seg_c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sealed_segment_ids order or content changed: got %v, want %v", got, want)
	}

	clone.Streams["str_1"].SealedSegmentIDs[0] = "seg_z"
	if state.Streams["str_1"].SealedSegmentIDs[0] != "seg_b" {
		t.Fatal("clone must not share slice storage with the source state")
	}
}
