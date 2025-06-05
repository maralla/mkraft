package log

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// This module is

// Case-1: test atomicity, and handling of corrupt writes
// test case: write logs twice, first partial write, second full write, when reading, should only read the second full write
// todo: how to make the write to disk partial?
// step-1: write log1, log2, log3, log4, log5; and then truncate the file to 3 bytes
// step-2: write log1, log2, log3, log4, log5; again;
// step-3: read the file, should only get 5 logs;
func TestRaftLogs_Atomicity(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	// step-1: write log1, log2, log3, log4, log5; and then truncate the file to 3 bytes
	err := raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2"), []byte("log3"), []byte("log4"), []byte("log5")}, 1)
	assert.NoError(t, err)

	// Type assert to access internal file field
	impl := raftLog.(*WALInspiredRaftLogsImpl)
	impl.file.Truncate(3)
	impl.file.Sync()
	// print the length of the file
	fi, err := impl.file.Stat()
	assert.NoError(t, err)
	fmt.Println(fi.Size())

	// step-2: write log1, log2, log3, log4, log5; again;
	err = raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2"), []byte("log3"), []byte("log4"), []byte("log5")}, 1)
	assert.NoError(t, err)

	// restart the raftLog
	impl.file.Close()
	raftLog = NewRaftLogsImplAndLoad("test.log", zap.NewNop(), NewRaftSerdeImpl())

	// step-3: read the file, should only get 5 logs;
	entries, err := raftLog.ReadLogsInBatchFromIdx(1)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(entries))
}

// Case-2: test race conditions
// test case: write logs in parallel, and check if the log sequence shall be correct

// Case-3: happy path
