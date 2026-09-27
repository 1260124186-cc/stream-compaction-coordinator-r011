package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func MarshalState(state *domain.State) ([]byte, error) {
	if state == nil {
		return nil, fmt.Errorf("state is nil")
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func UnmarshalState(data []byte) (*domain.State, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state domain.State
	if err := decoder.Decode(&state); err != nil {
		return nil, err
	}
	if err := ensureEndOfDocument(decoder); err != nil {
		return nil, err
	}
	normalizeCollections(&state)
	return &state, nil
}

// normalizeCollections canonicalizes a decoded snapshot so collection fields
// are never nil. JSON "null" and absent members decode to nil, but the domain
// and the public API represent empty collections as empty arrays and objects.
func normalizeCollections(state *domain.State) {
	if state.Audit == nil {
		state.Audit = []domain.AuditEvent{}
	}
	for _, stream := range state.Streams {
		if stream != nil && stream.SealedSegmentIDs == nil {
			stream.SealedSegmentIDs = []string{}
		}
	}
	for _, segment := range state.Segments {
		if segment == nil {
			continue
		}
		if segment.SourceSegmentIDs == nil {
			segment.SourceSegmentIDs = []string{}
		}
		if segment.ReplicaAcks == nil {
			segment.ReplicaAcks = map[string]domain.ReplicaAck{}
		}
	}
	for _, plan := range state.Plans {
		if plan == nil {
			continue
		}
		if plan.SourceSegmentIDs == nil {
			plan.SourceSegmentIDs = []string{}
		}
		if plan.SourceGenerations == nil {
			plan.SourceGenerations = map[string]uint64{}
		}
	}
}

func ensureEndOfDocument(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("state snapshot contains trailing JSON content")
}
