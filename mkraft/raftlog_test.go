package mkraft

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func withTempLogFile(t *testing.T, testFunc func(logs RaftLogsIface, filePath string)) {
	t.Helper()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "raftlog_test.log")
	logs := NewRaftLogsImplAndLoad(filePath)
	testFunc(logs, filePath)
}

func TestAppendLogsInBatchAndGetLogsFromIndex(t *testing.T) {
	withTempLogFile(t, func(logs RaftLogsIface, _ string) {
		commands := [][]byte{
			[]byte("cmd1"),
			[]byte("cmd2"),
			[]byte("cmd3"),
		}
		term := 2
		err := logs.AppendLogsInBatch(context.Background(), commands, term)
		if err != nil {
			t.Fatalf("AppendLogsInBatch failed: %v", err)
		}

		entries, err := logs.GetLogsFromIdx(1)
		if err != nil {
			t.Fatalf("GetLogsFromIndex failed: %v", err)
		}
		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(entries))
		}
		for i, entry := range entries {
			if string(entry.Commands) != string(commands[i]) {
				t.Errorf("entry %d: expected command %q, got %q", i, commands[i], entry.Commands)
			}
			if entry.Term != uint32(term) {
				t.Errorf("entry %d: expected term %d, got %d", i, term, entry.Term)
			}
			if entry.Index != uint64(i+1) {
				t.Errorf("entry %d: expected index %d, got %d", i, i+1, entry.Index)
			}
		}
	})
}

func TestGetPrevLogIndexAndTerm(t *testing.T) {
	withTempLogFile(t, func(logs RaftLogsIface, _ string) {
		index, term := logs.GetLastLogIdxAndTerm()
		if index != 0 || term != 0 {
			t.Errorf("expected (0,0) for empty logs, got (%d,%d)", index, term)
		}

		commands := [][]byte{[]byte("cmd")}
		termVal := 5
		if err := logs.AppendLogsInBatch(context.Background(), commands, termVal); err != nil {
			t.Fatalf("AppendLogsInBatch failed: %v", err)
		}
		index, term = logs.GetLastLogIdxAndTerm()
		if index != 1 || term != uint32(termVal) {
			t.Errorf("expected (1,%d), got (%d,%d)", termVal, index, term)
		}
	})
}

func TestGetLogsFromIndex_InvalidIndex(t *testing.T) {
	withTempLogFile(t, func(logs RaftLogsIface, _ string) {
		_, err := logs.GetLogsFromIdx(1)
		if err == nil {
			t.Error("expected error for index 1 on empty logs")
		}
	})
}

func TestPersistenceAndLoad(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "raftlog_persist.log")
	{
		logs := NewRaftLogsImplAndLoad(filePath)
		commands := [][]byte{[]byte("persist1"), []byte("persist2")}
		term := 7
		if err := logs.AppendLogsInBatch(context.Background(), commands, term); err != nil {
			t.Fatalf("AppendLogsInBatch failed: %v", err)
		}
		// Close file to flush
		if impl, ok := logs.(*SimpleRaftLogsImpl); ok {
			impl.file.Close()
		}
	}
	{
		logs := NewRaftLogsImplAndLoad(filePath)
		entries, err := logs.GetLogsFromIdx(1)
		fmt.Printf("entries: %v\n", entries)
		if err != nil {
			t.Fatalf("GetLogsFromIndex failed after reload: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries after reload, got %d", len(entries))
		}
		if string(entries[0].Commands) != "persist1" || string(entries[1].Commands) != "persist2" {
			t.Errorf("unexpected commands after reload: %q, %q", entries[0].Commands, entries[1].Commands)
		}
	}
}

func TestSerializeDeserialize(t *testing.T) {
	entry := RaftLogEntry{
		Index:    42,
		Term:     99,
		Commands: []byte("hello"),
	}
	impl := &SimpleRaftLogsImpl{}
	buf := impl.serialize(entry)
	// skip length prefix
	data := buf.Bytes()[4:]
	got, err := impl.deserialize(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}
	if got.Index != entry.Index || got.Term != entry.Term || string(got.Commands) != string(entry.Commands) {
		t.Errorf("deserialize mismatch: got %+v, want %+v", got, entry)
	}
}

func TestDeserialize_InvalidMarkers(t *testing.T) {
	impl := &SimpleRaftLogsImpl{}
	// Invalid start marker
	buf := []byte{'!', 0, 0, 0, 1, 0, 0, 0, 1, '#'}
	_, err := impl.deserialize(buf)
	if err == nil {
		t.Error("expected error for invalid start marker")
	}
	// Invalid end marker
	buf = []byte{'#', 0, 0, 0, 1, 0, 0, 0, 1, '!'}
	_, err = impl.deserialize(buf)
	if err == nil {
		t.Error("expected error for invalid end marker")
	}
}

func TestDeserialize_ShortBuffer(t *testing.T) {
	impl := &SimpleRaftLogsImpl{}
	buf := []byte{'#', '#'}
	_, err := impl.deserialize(buf)
	if err == nil {
		t.Error("expected error for short buffer")
	}
}
