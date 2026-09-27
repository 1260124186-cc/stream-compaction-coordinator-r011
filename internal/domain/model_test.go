package domain

import (
	"testing"
	"time"
)

// CloneState is used by every read path and by the mutation commit, so it must
// preserve the empty-but-present collection semantics of the in-memory
// snapshot. An empty slice must stay an empty slice (never nil) so persisted
// snapshots and API responses keep rendering arrays instead of null.
func TestCloneStatePreservesEmptyCollections(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stream, segment, err := NewStream("str_1", "seg_1", "ops", "events", 100, now)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	plan := &CompactionPlan{
		ID:                     "cmp_1",
		StreamID:               stream.ID,
		SourceSegmentIDs:       []string{"seg_a", "seg_b"},
		SourceGenerations:      map[string]uint64{"seg_a": 1, "seg_b": 1},
		Range:                  Range{BaseOffset: 0, EndOffset: 10},
		State:                  PlanStateProposed,
		ExpectedStreamRevision: 1,
		Revision:               1,
		CreatedAt:              now,
		UpdatedAt:              now,
	}
	state := NewState(now)
	state.Streams[stream.ID] = stream
	state.Segments[segment.ID] = segment
	state.Plans[plan.ID] = plan

	cloned := CloneState(state)

	clonedStream := cloned.Streams[stream.ID]
	if clonedStream.SealedSegmentIDs == nil {
		t.Fatalf("cloned stream sealed_segment_ids became nil")
	}
	if len(clonedStream.SealedSegmentIDs) != 0 {
		t.Fatalf("cloned stream sealed_segment_ids = %v, want empty", clonedStream.SealedSegmentIDs)
	}

	clonedSegment := cloned.Segments[segment.ID]
	if clonedSegment.SourceSegmentIDs == nil {
		t.Fatalf("cloned segment source_segment_ids became nil")
	}
	if len(clonedSegment.SourceSegmentIDs) != 0 {
		t.Fatalf("cloned segment source_segment_ids = %v, want empty", clonedSegment.SourceSegmentIDs)
	}
	if clonedSegment.ReplicaAcks == nil {
		t.Fatalf("cloned segment replica_acks became nil")
	}

	clonedPlan := cloned.Plans[plan.ID]
	if clonedPlan.SourceSegmentIDs == nil {
		t.Fatalf("cloned plan source_segment_ids became nil")
	}
	if cloned.Audit == nil {
		t.Fatalf("cloned audit log became nil")
	}
}

// A state that already holds nil collections (for example decoded from an
// older snapshot that stored null) must be normalized back to empty
// collections by the clone, so one save heals the snapshot shape.
func TestCloneStateNormalizesNilCollections(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stream, segment, err := NewStream("str_1", "seg_1", "ops", "events", 100, now)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	stream.SealedSegmentIDs = nil
	segment.SourceSegmentIDs = nil

	state := NewState(now)
	state.Streams[stream.ID] = stream
	state.Segments[segment.ID] = segment

	cloned := CloneState(state)
	if cloned.Streams[stream.ID].SealedSegmentIDs == nil {
		t.Fatalf("nil sealed_segment_ids was not normalized to an empty slice")
	}
	if cloned.Segments[segment.ID].SourceSegmentIDs == nil {
		t.Fatalf("nil source_segment_ids was not normalized to an empty slice")
	}
}
