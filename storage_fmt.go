package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// 持久化文件布局（v1，全部小端）：
//
//	offset  size  field
//	0       4     magic = "RAFT"
//	4       4     CRC-32/IEEE，校验从 version 字段到文件末尾
//	8       2     format version (uint16)
//	10      8     currentTerm (uint64)
//	18      8     votedFor (int64，-1 表示未投票)
//	26      4     日志条目数 n (uint32)
//	30      ...   n 个 LogEntry（见 entry.go）
//
// 版本兼容策略：
//   - 仅接受 magic 匹配且 version <= PersistentDataVersion 的文件；
//   - version 更新（来自未来版本）返回 ErrUnsupportedVersionKind，拒绝静默接管；
//   - CRC 不匹配视为损坏，返回错误，避免用脏数据参与选举；
//   - 旧版本读取后由更高版本负责前向迁移（当前只有 v1）。
var raftMagic = [4]byte{'R', 'A', 'F', 'T'}

func encodePersistent(data *PersistentData) []byte {
	payload := new(bytes.Buffer)

	var ver [2]byte
	binary.LittleEndian.PutUint16(ver[:], data.Version)
	payload.Write(ver[:])

	var fixed [20]byte
	binary.LittleEndian.PutUint64(fixed[0:8], data.CurrentTerm)
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(data.VotedFor))
	binary.LittleEndian.PutUint32(fixed[16:20], uint32(len(data.Log)))
	payload.Write(fixed[:])

	for _, e := range data.Log {
		if err := encodeEntry(payload, e); err != nil {
			panic(err) // bytes.Buffer 不会失败
		}
	}

	out := make([]byte, 0, 4+4+payload.Len())
	out = append(out, raftMagic[:]...)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(payload.Bytes()))
	out = append(out, crc[:]...)
	out = append(out, payload.Bytes()...)
	return out
}

func decodePersistent(raw []byte) (*PersistentData, error) {
	if len(raw) < 8+2 {
		return nil, fmt.Errorf("raft: persisted data truncated: %d bytes", len(raw))
	}
	if !bytes.Equal(raw[0:4], raftMagic[:]) {
		return nil, fmt.Errorf("raft: bad magic in persisted data")
	}
	storedCRC := binary.LittleEndian.Uint32(raw[4:8])
	payload := raw[8:]
	if crc32.ChecksumIEEE(payload) != storedCRC {
		return nil, fmt.Errorf("raft: persisted data checksum mismatch (corrupt)")
	}

	r := bytes.NewReader(payload)
	var ver [2]byte
	if _, err := r.Read(ver[:]); err != nil {
		return nil, err
	}
	version := binary.LittleEndian.Uint16(ver[:])
	if version > PersistentDataVersion {
		return nil, newError(ErrUnsupportedVersionKind, -1,
			fmt.Sprintf("raft: persisted data version %d is newer than supported %d", version, PersistentDataVersion))
	}

	var fixed [20]byte
	if _, err := r.Read(fixed[:]); err != nil {
		return nil, fmt.Errorf("raft: persisted header truncated: %w", err)
	}
	data := &PersistentData{
		Version:     version,
		CurrentTerm: binary.LittleEndian.Uint64(fixed[0:8]),
		VotedFor:    int(int64(binary.LittleEndian.Uint64(fixed[8:16]))),
	}
	n := binary.LittleEndian.Uint32(fixed[16:20])

	// v1 是目前唯一旧版本：直接按 v1 布局读取。
	data.Log = make([]LogEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		e, err := decodeEntry(r)
		if err != nil {
			return nil, fmt.Errorf("raft: persisted log entry %d truncated: %w", i, err)
		}
		data.Log = append(data.Log, e)
	}
	return data, nil
}
