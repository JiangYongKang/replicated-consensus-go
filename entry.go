package raft

import (
	"encoding/binary"
	"fmt"
	"io"
)

// OpType 是状态机命令的操作类型。读操作不进入日志。
type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Command 是一条要复制的写命令。
// ClientID + Seq 组成幂等键：同一请求超时重发不会被重复应用。
type Command struct {
	Op       OpType
	Key      string
	Value    string
	ClientID uint64
	Seq      uint64
}

// LogEntry 是日志中的一条记录。
type LogEntry struct {
	Index uint64
	Term  uint64
	Cmd   Command
}

func writeString(w io.Writer, s string) error {
	if len(s) > 0xffffffff {
		return fmt.Errorf("raft: string too long: %d", len(s))
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(s)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func readString(r io.Reader) (string, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return "", err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if int64(n) > int64(1<<30) {
		return "", fmt.Errorf("raft: string length implausible: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (c Command) encode(w io.Writer) error {
	if _, err := w.Write([]byte{byte(c.Op)}); err != nil {
		return err
	}
	if err := writeString(w, c.Key); err != nil {
		return err
	}
	if err := writeString(w, c.Value); err != nil {
		return err
	}
	var tail [16]byte
	binary.LittleEndian.PutUint64(tail[0:8], c.ClientID)
	binary.LittleEndian.PutUint64(tail[8:16], c.Seq)
	_, err := w.Write(tail[:])
	return err
}

func decodeCommand(r io.Reader) (Command, error) {
	var op [1]byte
	if _, err := io.ReadFull(r, op[:]); err != nil {
		return Command{}, err
	}
	key, err := readString(r)
	if err != nil {
		return Command{}, err
	}
	val, err := readString(r)
	if err != nil {
		return Command{}, err
	}
	var tail [16]byte
	if _, err := io.ReadFull(r, tail[:]); err != nil {
		return Command{}, err
	}
	return Command{
		Op:       OpType(op[0]),
		Key:      key,
		Value:    val,
		ClientID: binary.LittleEndian.Uint64(tail[0:8]),
		Seq:      binary.LittleEndian.Uint64(tail[8:16]),
	}, nil
}

// encodeEntry / decodeEntry 包含 Index、Term 与 Command。
func encodeEntry(w io.Writer, e LogEntry) error {
	var hdr [16]byte
	binary.LittleEndian.PutUint64(hdr[0:8], e.Index)
	binary.LittleEndian.PutUint64(hdr[8:16], e.Term)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	return e.Cmd.encode(w)
}

func decodeEntry(r io.Reader) (LogEntry, error) {
	var hdr [16]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return LogEntry{}, err
	}
	cmd, err := decodeCommand(r)
	if err != nil {
		return LogEntry{}, err
	}
	return LogEntry{
		Index: binary.LittleEndian.Uint64(hdr[0:8]),
		Term:  binary.LittleEndian.Uint64(hdr[8:16]),
		Cmd:   cmd,
	}, nil
}
