package mkraft

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

var _ RaftLogsIface = (*SimpleRaftLogsImpl)(nil)

type RaftLogsIface interface {
	GetLastLogIdxAndTerm() (uint64, uint32)
	GetLastLogIdx() uint64
	GetTermByIndex(index uint64) (uint32, error)
	// index is included
	GetLogsFromIdx(index uint64) ([]*RaftLogEntry, error)
	AppendLogsInBatch(ctx context.Context, commandList [][]byte, term int) error
}

func NewRaftLogsImplAndLoad(filePath string) RaftLogsIface {
	initLogsLength := 5000
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

	raftLogs := &SimpleRaftLogsImpl{
		logs:  make([]*RaftLogEntry, 0, initLogsLength),
		file:  file,
		mutex: &sync.Mutex{},
	}
	err = raftLogs.load()
	if err != nil {
		panic(fmt.Sprintf("failed to load raft logs: %v", err))
	}
	return raftLogs
}

type RaftLogEntry struct {
	Index    uint64
	Term     uint32
	Commands []byte
}

type SimpleRaftLogsImpl struct {
	logs  []*RaftLogEntry
	file  *os.File
	mutex *sync.Mutex
}

const LogMarker byte = '#'

// if the index < 1, the term is 0
func (rl *SimpleRaftLogsImpl) GetTermByIndex(index uint64) (uint32, error) {
	if index == 0 {
		return 0, nil
	}
	if index > uint64(len(rl.logs)) && index < 0 {
		return 0, fmt.Errorf("invalid index: %d", index)
	}
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	sliceIndex := int(index) - 1
	return rl.logs[sliceIndex].Term, nil
}

func (rl *SimpleRaftLogsImpl) GetLastLogIdx() uint64 {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	// todo: since it uses slice, the uint64 is not necessary
	return uint64(len(rl.logs))
}

// index is included
func (rl *SimpleRaftLogsImpl) GetLogsFromIdx(index uint64) ([]*RaftLogEntry, error) {
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
func (rl *SimpleRaftLogsImpl) GetLastLogIdxAndTerm() (uint64, uint32) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	if len(rl.logs) == 0 {
		return 0, 0
	}
	index := len(rl.logs)
	lastLog := rl.logs[index-1]

	return uint64(index), lastLog.Term
}

func (rl *SimpleRaftLogsImpl) AppendLogsInBatch(ctx context.Context, commandList [][]byte, term int) error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	var buffers bytes.Buffer
	entries := make([]*RaftLogEntry, len(commandList))

	for idx, command := range commandList {
		entry := &RaftLogEntry{
			// Index:    uint64(len(rl.logs) + idx + 1),
			Term:     uint32(term),
			Commands: command,
		}
		entries[idx] = entry
		// serialize: len#term, index, commands#
		var buf bytes.Buffer = rl.serialize(entry)
		buffers.Write(buf.Bytes())
	}

	if _, err := rl.file.Write(buffers.Bytes()); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}
	rl.file.Sync() // forced to sync the file to disk
	rl.logs = append(rl.logs, entries...)
	return nil
}

func (rl *SimpleRaftLogsImpl) load() error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	fileInfo, err := rl.file.Stat()
	if err != nil {
		return err
	}

	fileSize := fileInfo.Size()
	if fileSize == 0 {
		fmt.Println("file size is 0")
		return nil
	}

	_, err = rl.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	buf := make([]byte, fileSize)
	_, err = io.ReadFull(rl.file, buf)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(buf)
	fmt.Println("file size", fileSize)
	for reader.Len() > 0 {
		lengthBuf := make([]byte, 4)
		if err := binary.Read(reader, binary.BigEndian, &lengthBuf); err != nil {
			return fmt.Errorf("failed to read entry length: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		fmt.Println("length", length)

		dataBuf := make([]byte, length)
		if _, err := io.ReadFull(reader, dataBuf); err != nil {
			return fmt.Errorf("failed to read entry: %w", err)
		}

		entry, err := rl.deserialize(dataBuf)
		if err != nil {
			return fmt.Errorf("failed to deserialize entry: %w", err)
		}
		rl.logs = append(rl.logs, entry)
	}
	return nil
}

// [4 bytes: length][1 byte: marker][8 bytes: term][8 bytes: index][N bytes: command][1 byte: marker]
func (rl *SimpleRaftLogsImpl) serialize(entry *RaftLogEntry) bytes.Buffer {
	var inner bytes.Buffer
	var full bytes.Buffer

	inner.WriteByte(LogMarker)
	binary.Write(&inner, binary.BigEndian, entry.Term)
	binary.Write(&inner, binary.BigEndian, entry.Index)
	inner.Write(entry.Commands)
	inner.WriteByte(LogMarker)

	length := uint32(inner.Len())
	binary.Write(&full, binary.BigEndian, length)
	full.Write(inner.Bytes())
	return full
}

func (rl *SimpleRaftLogsImpl) deserialize(buf []byte) (*RaftLogEntry, error) {
	const (
		termSize   = 4
		indexSize  = 8
		headerSize = 1 + termSize + indexSize
		footerSize = 1
	)

	if len(buf) < headerSize+footerSize {
		return nil, fmt.Errorf("buffer too short to contain a log entry")
	}

	if buf[0] != LogMarker || buf[len(buf)-1] != LogMarker {
		return nil, fmt.Errorf("invalid log markers: start=%x end=%x", buf[0], buf[len(buf)-1])
	}

	reader := bytes.NewReader(buf[1 : len(buf)-1]) // skip markers

	var term uint32
	if err := binary.Read(reader, binary.BigEndian, &term); err != nil {
		return nil, fmt.Errorf("failed to read term: %w", err)
	}

	var index uint64
	if err := binary.Read(reader, binary.BigEndian, &index); err != nil {
		return nil, fmt.Errorf("failed to read index: %w", err)
	}

	commands, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read commands: %w", err)
	}

	return &RaftLogEntry{
		Term:     term,
		Index:    index,
		Commands: commands,
	}, nil
}
