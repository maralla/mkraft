package log

import (
	"testing"
)

func TestBatchSerialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entries := []*RaftLogEntry{
		{Term: 1, Commands: []byte("log1")},
		{Term: 2, Commands: []byte("log2")},
		{Term: 3, Commands: []byte("log3")},
	}
	serialized, err := serde.BatchSerialize(entries)
	if err != nil {
		t.Fatalf("failed to serialize log entries: %v", err)
	}
	t.Logf("serialized: %v", serialized)
}

func TestBatchDeserialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entries := []*RaftLogEntry{
		{Term: 1, Commands: []byte("log1")},
		{Term: 2, Commands: []byte("log2")},
		{Term: 3, Commands: []byte("log3")},
	}
	serialized, err := serde.BatchSerialize(entries)
	if err != nil {
		t.Fatalf("failed to serialize log entries: %v", err)
	}
	deserialized, err := serde.BatchDeserialize(serialized)
	if err != nil {
		t.Fatalf("failed to deserialize log entries: %v", err)
	}
	t.Logf("deserialized: %v", deserialized)
}

func TestLogSerialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entry := &RaftLogEntry{
		Term:     1,
		Commands: []byte("log1"),
	}
	serialized, err := serde.LogSerialize(entry)
	if err != nil {
		t.Fatalf("failed to serialize log entry: %v", err)
	}
	t.Logf("serialized: %v", serialized)
}

func TestLogDeserialize(t *testing.T) {
	serde := NewRaftSerdeImpl()
	entry := &RaftLogEntry{
		Term:     1,
		Commands: []byte("log1"),
	}
	serialized, err := serde.LogSerialize(entry)
	if err != nil {
		t.Fatalf("failed to serialize log entry: %v", err)
	}
	deserialized, err := serde.LogDeserialize(serialized)
	if err != nil {
		t.Fatalf("failed to deserialize log entry: %v", err)
	}
	t.Logf("deserialized: %v", deserialized)
}
