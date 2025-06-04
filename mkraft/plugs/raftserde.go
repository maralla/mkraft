package plugs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type RaftSerdeIface interface {
	Serialize(entry *RaftLogEntry) []byte
	Deserialize(data []byte) (*RaftLogEntry, error)
}

func NewRaftSerdeImpl() RaftSerdeIface {
	return &RaftSerdeImpl{
		CommandMarker: '#',
	}
}

type RaftSerdeImpl struct {
	CommandMarker byte
}

// [4 bytes: length][1 byte: marker][8 bytes: term][--8 bytes: index--][N bytes: command][1 byte: marker]
// todo: add version to this
func (rl *RaftSerdeImpl) Serialize(entry *RaftLogEntry) []byte {
	var inner bytes.Buffer
	var full bytes.Buffer

	inner.WriteByte(rl.CommandMarker)
	binary.Write(&inner, binary.BigEndian, entry.Term)
	inner.Write(entry.Commands)
	inner.WriteByte(rl.CommandMarker)

	length := uint32(inner.Len())
	binary.Write(&full, binary.BigEndian, length)
	full.Write(inner.Bytes())
	return full.Bytes()
}

func (rl *RaftSerdeImpl) Deserialize(buf []byte) (*RaftLogEntry, error) {
	const (
		termSize   = 4
		indexSize  = 8
		headerSize = 1 + termSize + indexSize
		footerSize = 1
	)

	if len(buf) < headerSize+footerSize {
		return nil, fmt.Errorf("buffer too short to contain a log entry")
	}

	if buf[0] != rl.CommandMarker || buf[len(buf)-1] != rl.CommandMarker {
		return nil, fmt.Errorf("invalid log markers: start=%x end=%x", buf[0], buf[len(buf)-1])
	}

	reader := bytes.NewReader(buf[1 : len(buf)-1]) // skip markers

	var term uint32
	if err := binary.Read(reader, binary.BigEndian, &term); err != nil {
		return nil, fmt.Errorf("failed to read term: %w", err)
	}

	// var index uint64
	// if err := binary.Read(reader, binary.BigEndian, &index); err != nil {
	// 	return nil, fmt.Errorf("failed to read index: %w", err)
	// }

	commands, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read commands: %w", err)
	}

	return &RaftLogEntry{
		Term:     term,
		Commands: commands,
	}, nil
}
