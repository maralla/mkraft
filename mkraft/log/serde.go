package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

type RaftSerdeIface interface {
	LogSerialize(entry *RaftLogEntry) []byte
	LogDeserialize(data []byte) (*RaftLogEntry, error)

	BatchSerialize(entries []*RaftLogEntry) []byte
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
func (rl *RaftSerdeImpl) BatchSerialize(entries []*RaftLogEntry) []byte {
	var dataBuffer bytes.Buffer
	for _, entry := range entries {
		dataBuffer.Write(rl.LogSerialize(entry))
		dataBuffer.WriteByte(rl.LogSeparator)
	}
	data := dataBuffer.Bytes()

	var crcBuffer bytes.Buffer
	binary.Write(&crcBuffer, binary.BigEndian, crc32.ChecksumIEEE(data))
	crcBuffer.WriteByte(rl.BatchSeparator)
	crcBuffer.Write(data)
	crc := crcBuffer.Bytes()

	// [BatchSeparator]crc[data][BatchSeparator]
	var total bytes.Buffer
	total.Write(crc)
	total.Write(data)
	return total.Bytes()
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
func (rl *RaftSerdeImpl) LogSerialize(entry *RaftLogEntry) []byte {
	var buf bytes.Buffer

	// Write term (4 bytes)
	binary.Write(&buf, binary.BigEndian, entry.Term)

	// Write command length (4 bytes)
	cmdLen := uint32(len(entry.Commands))
	binary.Write(&buf, binary.BigEndian, cmdLen)

	// Write commands bytes
	buf.Write(entry.Commands)

	return buf.Bytes()
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
