package raft

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// buildRawVersion 构造指定版本号的最小合法持久化文件（0 条日志）。
func buildRawVersion(version uint16, term uint64, votedFor int64) []byte {
	payload := new(bytes.Buffer)
	var ver [2]byte
	binary.LittleEndian.PutUint16(ver[:], version)
	payload.Write(ver[:])
	var fixed [20]byte
	binary.LittleEndian.PutUint64(fixed[0:8], term)
	binary.LittleEndian.PutUint64(fixed[8:16], uint64(votedFor))
	binary.LittleEndian.PutUint32(fixed[16:20], 0)
	payload.Write(fixed[:])

	out := []byte("RAFT")
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(payload.Bytes()))
	out = append(out, crc[:]...)
	out = append(out, payload.Bytes()...)
	return out
}
