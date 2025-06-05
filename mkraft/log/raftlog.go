package log

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"go.uber.org/zap"
)

// IMPLEMENTATION GAP:
// Since the paper doesn't specify the details of raftlog, my implementation refers to postgres's WAL in some ways.
// Since I need to do log compaction, while postgres's WAL doesn't, I need to make some new designs for the raftlog.
var _ RaftLogsIface = (*WALInspiredRaftLogsImpl)(nil)

type RaftLogsIface interface {
	// todo: shall change all uint/uint64 to types that really make sense in golang system, consider len(logs) cannot be uint64

	// the raft log iface is designed to be handled in batching from the first place
	// need to handle partial writes failure
	AppendLogsInBatch(ctx context.Context, commandList [][]byte, term uint32) error
	UpdateLogsInBatch(ctx context.Context, preLogIndex uint64, commandList [][]byte, term uint32) error
	ReadLogsInBatchFromIdx(index uint64) ([]*RaftLogEntry, error) // the index is included

	// logIndex starts from 1, so the first log is at index 1
	GetLastLogIdxAndTerm() (uint64, uint32)
	GetLastLogIdx() uint64
	GetTermByIndex(index uint64) (uint32, error)

	// @return: true if the preLogIndex and term match
	CheckPreLog(preLogIndex uint64, term uint32) bool
}

type CatchupLogs struct {
	LastLogIndex uint64
	LastLogTerm  uint32
	Entries      []*RaftLogEntry
}

func NewRaftLogsImplAndLoad(filePath string, logger *zap.Logger, serde RaftSerdeIface) RaftLogsIface {
	if serde == nil {
		serde = NewRaftSerdeImpl()
	}
	var file *os.File
	var err error
	if _, statErr := os.Stat(filePath); statErr == nil {
		file, err = os.OpenFile(filePath, os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
	} else {
		file, err = os.Create(filePath)
		if err != nil {
			panic(err)
		}
	}

	batchSeparator := byte('\x1D') // group separator
	raftLogs := &WALInspiredRaftLogsImpl{
		file:           file,
		mutex:          &sync.Mutex{},
		batchSeparater: batchSeparator,
		batchSize:      1024 * 8, // 8KB
		serde:          serde,
		logger:         logger,
	}
	err = raftLogs.initFromLogFile()
	if err != nil {
		panic(fmt.Sprintf("failed to load raft logs: %v", err))
	}
	return raftLogs
}

type RaftLogEntry struct {
	Term     uint32
	Commands []byte
}

// Each individual record in a WAL file is protected by a CRC-32C (32-bit) check that allows us to tell if record contents are correct.
// The CRC value is set when we write each WAL record and checked during crash recovery, archive recovery and replication.
type WALInspiredRaftLogsImpl struct {
	logs           []*RaftLogEntry
	file           *os.File
	mutex          *sync.Mutex
	logger         *zap.Logger
	serde          RaftSerdeIface
	batchSeparater byte
	batchSize      int
}

// if the index < 1, the term is 0
func (rl *WALInspiredRaftLogsImpl) GetTermByIndex(index uint64) (uint32, error) {
	if index == 0 {
		return 0, nil
	}
	if index > uint64(len(rl.logs)) {
		return 0, fmt.Errorf("invalid index: %d", index)
	}
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	sliceIndex := int(index) - 1
	return rl.logs[sliceIndex].Term, nil
}

func (rl *WALInspiredRaftLogsImpl) GetLastLogIdx() uint64 {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	// todo: since it uses slice, the uint64 is not necessary
	return uint64(len(rl.logs))
}

// index is included
func (rl *WALInspiredRaftLogsImpl) ReadLogsInBatchFromIdx(index uint64) ([]*RaftLogEntry, error) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	sliceIndex := int(index) - 1
	if sliceIndex < 0 || sliceIndex >= len(rl.logs) {
		return nil, fmt.Errorf("invalid index: %d", index)
	}
	logs := make([]*RaftLogEntry, len(rl.logs)-sliceIndex)
	copy(logs, rl.logs[sliceIndex:len(rl.logs)])
	return logs, nil
}

// index starts from 1
func (rl *WALInspiredRaftLogsImpl) GetLastLogIdxAndTerm() (uint64, uint32) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	if len(rl.logs) == 0 {
		return 0, 0
	}
	index := len(rl.logs)
	lastLog := rl.logs[index-1]

	return uint64(index), lastLog.Term
}

func (rl *WALInspiredRaftLogsImpl) AppendLogsInBatch(ctx context.Context, commandList [][]byte, term uint32) error {
	if len(commandList) == 0 {
		return nil
	}
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	return rl.unsafeAppendLogsInBatch(commandList, term)
}

func (rl *WALInspiredRaftLogsImpl) UpdateLogsInBatch(ctx context.Context, preLogIndex uint64, commandList [][]byte, term uint32) error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	if len(rl.logs) < int(preLogIndex) || rl.logs[preLogIndex-1].Term != term {
		return common.ErrPreLogNotMatch
	}

	// maki: here is a bit tricky
	// case-1: doesn't need overwrite the file if the logs are consistent with the leader
	if len(rl.logs) == int(preLogIndex) && rl.logs[preLogIndex-1].Term == term {
		return rl.unsafeAppendLogsInBatch(commandList, term)
	}

	rl.logger.Warn("raft log update: preLogIndex does not match, overwriting logs")

	// case-2: overwrite the previous log and append new logs
	// (1) overwirte the file from the preLogIndex
	// (2) overwrite the logs from the preLogIndex

	// Step: Get the memory logs to truncate
	// Truncate in-memory logs
	rl.logs = rl.logs[:preLogIndex]
	// offset of the file
	offset := 0
	for _, log := range rl.logs {
		buf, err := rl.serde.LogSerialize(log)
		if err != nil {
			return fmt.Errorf("failed to serialize log: %w", err)
		}
		offset += len(buf)
	}
	// todo: maintain the file size inztead of calculating it every time with logOffsets []int64
	err := rl.file.Truncate(int64(offset)) // truncate the file to the new size
	if err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}
	err = rl.file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync file after truncate: %w", err)
	}
	return rl.unsafeAppendLogsInBatch(commandList, term)
}

func (rl *WALInspiredRaftLogsImpl) CheckPreLog(preLogIndex uint64, term uint32) bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	return preLogIndex == uint64(len(rl.logs)) && rl.logs[preLogIndex-1].Term == uint32(term)
}

// load the logs from the file
// handle the corrupt partial data
func (rl *WALInspiredRaftLogsImpl) initFromLogFile() error {

	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	// if not logs, init the file and logs
	initLogsLength := 10_000
	fileInfo, err := rl.file.Stat()
	if err != nil && err == os.ErrNotExist {
		// create the file with 0666 permission
		file, err := os.Create(rl.file.Name())
		if err != nil {
			return err
		}
		os.Chmod(rl.file.Name(), os.FileMode(0666))
		rl.file = file
		rl.logs = make([]*RaftLogEntry, 0, initLogsLength)
		return nil
	}
	if err != nil {
		return err
	}

	// seek to the start of the file
	_, err = rl.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	buf := make([]byte, fileInfo.Size())
	readLen, err := io.ReadFull(rl.file, buf)
	if err != nil {
		return err
	}
	if readLen != int(fileInfo.Size()) {
		return fmt.Errorf("failed to read full file: %d", readLen)
	}
	// load the logs
	return rl.unsafeLoadLogs()
}

// read all the logs from the file into the memory of rl.logs
func (rl *WALInspiredRaftLogsImpl) unsafeLoadLogs() error {

	if len(rl.logs) > 0 {
		panic("loading logs when logs are not empty")
	}

	// todo: need log compaction
	reader := bufio.NewReaderSize(rl.file, rl.batchSize)
	var partial []byte

	for {
		chunk := make([]byte, rl.batchSize)
		n, err := reader.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if n > 0 {
			chunk = chunk[:n]
			all := append(partial, chunk...)

			parts := bytes.Split(all, []byte{rl.batchSeparater})
			for i := 1; i < len(parts)-1; i++ {
				if logEntries, err := rl.serde.BatchDeserialize(parts[i]); err != nil {
					fmt.Printf("Failed to deserialize: %v, pass this log entry\n", err)
				} else {
					rl.logs = append(rl.logs, logEntries...)
				}
			}

			if len(all) > 0 && all[len(all)-1] == rl.batchSeparater {
				// Ends with delimiter, last one is complete
				if logEntries, err := rl.serde.BatchDeserialize(parts[len(parts)-1]); err != nil {
					fmt.Printf("Failed to deserialize: %v, pass this log entry\n", err)
				} else {
					rl.logs = append(rl.logs, logEntries...)
				}
				partial = nil
			} else {
				// Keep partial
				partial = parts[len(parts)-1]
			}
		}
	}
	// Final leftover
	if len(partial) > 0 {
		if logEntries, err := rl.serde.BatchDeserialize(partial); err != nil {
			fmt.Printf("Failed to deserialize final part: %v, pass this log entry\n", err)
		} else {
			rl.logs = append(rl.logs, logEntries...)
		}
	}
	return nil
}

// the problem here is what about partial writes?
// for example we want to append 12345, then after 123 it crashes, and we retry, and end up with 12312345 which totally mess up the log
// so we need to serialize and crc the write as a whole, instead of a unit of it which is a log
// batchBinary+batchSeparator+batchBinary+batchSeparator+batchBinary
func (rl *WALInspiredRaftLogsImpl) unsafeAppendLogsInBatch(commandList [][]byte, term uint32) error {
	entries := make([]*RaftLogEntry, len(commandList))
	for idx, command := range commandList {
		entry := &RaftLogEntry{
			Term:     uint32(term),
			Commands: command,
		}
		entries[idx] = entry
	}
	bytesToWrite, err := rl.serde.BatchSerialize(entries)
	if err != nil {
		return fmt.Errorf("failed to serialize log entries: %w", err)
	}

	// batchSeparator+batchBinaryData
	allBytes := make([]byte, 0, 1+len(bytesToWrite))
	allBytes = append(allBytes, rl.batchSeparater)
	allBytes = append(allBytes, bytesToWrite...)

	if _, err := rl.file.Write(allBytes); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}
	rl.file.Sync() // forced to sync the file to disk
	rl.logs = append(rl.logs, entries...)
	return nil
}
