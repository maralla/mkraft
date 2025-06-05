package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

type RaftSerdeIface interface {
	LogSerialize(entry *RaftLogEntry) ([]byte, error)
	LogDeserialize(data []byte) (*RaftLogEntry, error)

	BatchSerialize(entries []*RaftLogEntry) ([]byte, error)
	BatchDeserialize(data []byte) ([]*RaftLogEntry, error)
}

func NewRaftSerdeImpl() RaftSerdeIface {
	return &RaftSerdeImpl{
		LogSeparator: '#',
	}
}

const (
	LogSeparator   = '\x1F' //unit separator
	BatchSeparator = '\x1D' //group separator
)

type RaftSerdeImpl struct {
	LogSeparator   byte
	BatchSeparator byte
}

// crc[data]
func (rl *RaftSerdeImpl) BatchSerialize(entries []*RaftLogEntry) ([]byte, error) {

	data := make([]byte, 0, 4*1024)
	for _, entry := range entries {
		serialized, err := rl.LogSerialize(entry)
		// maki: I don't like this error handling style of Golang which interleave the error handling and the logic
		// so I've changed bytes.Buffer to []byte and removed the error handling
		if err != nil {
			return nil, fmt.Errorf("failed to serialize log entry: %w", err)
		}
		data = append(data, serialized...)
		data = append(data, rl.LogSeparator)
	}
	data = data[:len(data)-1] // remove the last separator

	crc := make([]byte, 4+1+len(data))
	binary.BigEndian.PutUint32(crc[:4], crc32.ChecksumIEEE(data))

	total := make([]byte, 0, 4+len(data))
	total = append(total, crc...)
	total = append(total, data...)
	return total, nil
}

// deserialize: crc[data]
func (rl *RaftSerdeImpl) BatchDeserialize(payload []byte) ([]*RaftLogEntry, error) {
	if len(payload) < 5 { // at least 4 bytes CRC + some data
		return nil, fmt.Errorf("payload too short")
	}

	sep := rl.BatchSeparator

	crcBytes := payload[:4]
	data := payload[4:]

	crcFromPayload := binary.BigEndian.Uint32(crcBytes)
	crcCalc := crc32.ChecksumIEEE(data)
	if crcFromPayload != crcCalc {
		return nil, fmt.Errorf("crc mismatch: expected %x got %x", crcFromPayload, crcCalc)
	}

	parts := bytes.Split(data, []byte{sep})

	var entries []*RaftLogEntry
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		entry, err := rl.LogDeserialize(part)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// [4 bytes: term][4 bytes: command length][N bytes: command]
func (rl *RaftSerdeImpl) LogSerialize(entry *RaftLogEntry) ([]byte, error) {
	var buf bytes.Buffer

	// Write term (4 bytes)
	err := binary.Write(&buf, binary.BigEndian, entry.Term)
	if err != nil {
		return nil, fmt.Errorf("failed to write term: %w", err)
	}

	// Write command length (4 bytes)
	cmdLen := uint32(len(entry.Commands))
	err = binary.Write(&buf, binary.BigEndian, cmdLen)
	if err != nil {
		return nil, fmt.Errorf("failed to write command length: %w", err)
	}

	// Write commands bytes
	_, err = buf.Write(entry.Commands)
	if err != nil {
		return nil, fmt.Errorf("failed to write commands: %w", err)
	}

	return buf.Bytes(), nil
}

func (rl *RaftSerdeImpl) LogDeserialize(buf []byte) (*RaftLogEntry, error) {
	headerLen := 8
	if len(buf) <= headerLen { // minimum length > 4 + 4
		return nil, fmt.Errorf("buffer too short to contain a log entry")
	}

	reader := bytes.NewReader(buf)

	var term uint32
	if err := binary.Read(reader, binary.BigEndian, &term); err != nil {
		return nil, fmt.Errorf("failed to read term: %w", err)
	}

	var cmdLen uint32
	if err := binary.Read(reader, binary.BigEndian, &cmdLen); err != nil {
		return nil, fmt.Errorf("failed to read command length: %w", err)
	}

	if cmdLen > uint32(len(buf)-headerLen) {
		return nil, fmt.Errorf("command length %d exceeds buffer size", cmdLen)
	}

	commands := make([]byte, cmdLen)
	if _, err := io.ReadFull(reader, commands); err != nil {
		return nil, fmt.Errorf("failed to read commands: %w", err)
	}

	return &RaftLogEntry{
		Term:     term,
		Commands: commands,
	}, nil
}
