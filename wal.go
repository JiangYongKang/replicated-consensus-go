package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// WAL 持久化格式（小端二进制，自描述）：
//
//	header  : magic(4B="RFLW") | version(1B) | flags(2B LE) | reserved(1B)
//	records : repeat( type(1B) | crc32(4B LE) | payloadLen(4B LE) | payload )
//
// type=1 HardState   payload: encoding/gob HardState
// type=2 EntryBatch  payload: encoding/gob []Entry
//
// 重放语义：HardState 记录“后者覆盖”，EntryBatch 按记录顺序 append；
// 损坏/截断（崩溃写到一半）的尾部记录在最后一条完整记录处停止，
// 并返回 ErrCorruptWAL 包装（调用方可据 errors.Is 决定是否接受部分重放）。
const (
	walMagic        = "RFLW"
	walVersion byte = 1
)

type recordType byte

const (
	recHardState recordType = 1
	recEntries   recordType = 2
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// FileWAL 通过本地文件模拟磁盘：内存视图来自启动重放，写路径 append-only；
// 当日志冲突需要截断时，用“临时文件 + rename”原子重写整个 WAL。
type FileWAL struct {
	path string
	f    *os.File
	view *MemoryStorage
}

var _ Storage = (*FileWAL)(nil)

// OpenFileWAL 打开或创建 WAL；文件存在时校验 magic/版本并重放全部记录。
func OpenFileWAL(path string) (*FileWAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	w := &FileWAL{path: path, f: f, view: NewMemoryStorage()}

	st, _ := f.Stat()
	if st.Size() == 0 {
		if err := w.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		return w, nil
	}
	if err := w.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

func (f *FileWAL) writeHeader() error {
	hdr := make([]byte, 8)
	copy(hdr[0:4], walMagic)
	hdr[4] = walVersion
	// flags(2B)=0, reserved(1B)=0
	if _, err := f.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	n, err := f.f.Write(hdr)
	if err != nil {
		return err
	}
	// 显式定位到 header 之后，后续记录从偏移 8 开始 append。
	if _, err := f.f.Seek(int64(n), io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (f *FileWAL) replay() error {
	if _, err := f.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(f.f, hdr); err != nil {
		return fmt.Errorf("%w: short header: %v", ErrNotWAL, err)
	}
	if string(hdr[0:4]) != walMagic {
		return fmt.Errorf("%w: bad magic %q", ErrNotWAL, string(hdr[0:4]))
	}
	ver := hdr[4]
	// 版本兼容策略：仅读取 <= 当前版本的文件；更高版本一律拒绝，
	// 避免旧程序误解新格式；v1 内部新增记录类型属于次版本扩展，
	// 未知类型跳过而非报错（向前演进预留）。
	if ver == 0 || ver > walVersion {
		return fmt.Errorf("%w: file version=%d supported<=1", ErrUnsupportedVersion, ver)
	}

	var replayErr error
	for {
		var head [9]byte
		n, err := io.ReadFull(f.f, head[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if n > 0 {
				replayErr = fmt.Errorf("%w: %d trailing header bytes ignored", ErrCorruptWAL, n)
			}
			break
		}
		if err != nil {
			return err
		}
		rt := recordType(head[0])
		crc := binary.LittleEndian.Uint32(head[1:5])
		plen := binary.LittleEndian.Uint32(head[5:9])
		if plen > 64*1024*1024 {
			return fmt.Errorf("%w: implausible record length %d", ErrCorruptWAL, plen)
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(f.f, payload); err != nil {
			replayErr = fmt.Errorf("%w: truncated record payload: %v", ErrCorruptWAL, err)
			break
		}
		if crc32.Checksum(payload, crcTable) != crc {
			replayErr = fmt.Errorf("%w: checksum mismatch", ErrCorruptWAL)
			break
		}
		if err := f.applyRecord(rt, payload); err != nil {
			return err
		}
	}

	// 截断可能存在的损坏尾部，使后续 append 干净。
	if replayErr != nil {
		pos, err := f.f.Seek(0, io.SeekCurrent)
		if err == nil {
			if err := f.f.Truncate(pos); err == nil {
				_ = f.f.Sync()
			}
		}
	}
	if _, err := f.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return replayErr
}

func (f *FileWAL) applyRecord(rt recordType, payload []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(payload))
	switch rt {
	case recHardState:
		var h HardState
		if err := dec.Decode(&h); err != nil {
			return fmt.Errorf("%w: hardstate: %v", ErrCorruptWAL, err)
		}
		return f.view.SaveHardState(h)
	case recEntries:
		var batch []Entry
		if err := dec.Decode(&batch); err != nil {
			return fmt.Errorf("%w: entries: %v", ErrCorruptWAL, err)
		}
		if len(batch) == 0 {
			return nil
		}
		prev := batch[0].Index - 1
		return f.view.Append(prev, batch)
	default:
		// 前向兼容：未知记录类型跳过。
		return nil
	}
}

func (f *FileWAL) appendRecord(rt recordType, payload []byte) error {
	var head [9]byte
	head[0] = byte(rt)
	binary.LittleEndian.PutUint32(head[1:5], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(head[5:9], uint32(len(payload)))
	if _, err := f.f.Write(head[:]); err != nil {
		return err
	}
	if _, err := f.f.Write(payload); err != nil {
		return err
	}
	return f.f.Sync()
}

func encodeGob(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *FileWAL) HardState() HardState { return f.view.HardState() }

func (f *FileWAL) SaveHardState(h HardState) error {
	payload, err := encodeGob(h)
	if err != nil {
		return err
	}
	if err := f.appendRecord(recHardState, payload); err != nil {
		return err
	}
	return f.view.SaveHardState(h)
}

func (f *FileWAL) Entries(lo, hi uint64) ([]Entry, error) {
	return f.view.Entries(lo, hi)
}

func (f *FileWAL) Entry(idx uint64) (Entry, error) {
	return f.view.Entry(idx)
}

func (f *FileWAL) LastIndex() uint64 { return f.view.LastIndex() }

func (f *FileWAL) TermAt(idx uint64) (uint64, error) { return f.view.TermAt(idx) }

// Append 在内存视图上应用同一幂等规则；若发生截断则先原子重写 WAL，
// 否则仅 append 新记录。
func (f *FileWAL) Append(prevIdx uint64, entries []Entry) error {
	before := f.view.LastIndex()

	// 幂等快速路径：与现有日志完全一致的尾部重复投递不落盘新记录。
	common := uint64(0)
	for _, e := range entries {
		if e.Index > before {
			break
		}
		existing, err := f.view.Entry(e.Index)
		if err == nil && existing.Term == e.Term && existing.Cmd == e.Cmd {
			common++
			continue
		}
		break
	}
	if common == uint64(len(entries)) && prevIdx+common <= before {
		return f.view.Append(prevIdx, entries)
	}

	if prevIdx+1 <= before {
		// 新条目会触碰已有区间，可能触发截断：直接重写整个 WAL 最安全。
		if err := f.view.Append(prevIdx, entries); err != nil {
			return err
		}
		return f.rewrite()
	}

	if err := f.view.Append(prevIdx, entries); err != nil {
		return err
	}
	after := f.view.LastIndex()
	newOnes, err := f.view.Entries(before+1, after+1)
	if err != nil {
		return err
	}
	if len(newOnes) == 0 {
		return nil
	}
	payload, err := encodeGob(newOnes)
	if err != nil {
		return err
	}
	return f.appendRecord(recEntries, payload)
}

// rewrite 用临时文件 + fsync + rename 原子替换 WAL。
func (f *FileWAL) rewrite() error {
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".wal-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	rollback := func() { tmp.Close(); os.Remove(tmpName) }

	hdr := make([]byte, 8)
	copy(hdr[0:4], walMagic)
	hdr[4] = walVersion
	if _, err := tmp.Write(hdr); err != nil {
		rollback()
		return err
	}
	hs, err := encodeGob(f.view.HardState())
	if err != nil {
		rollback()
		return err
	}
	if err := writeRecord(tmp, recHardState, hs); err != nil {
		rollback()
		return err
	}
	last := f.view.LastIndex()
	if last > 0 {
		all, err := f.view.Entries(1, last+1)
		if err != nil {
			rollback()
			return err
		}
		payload, err := encodeGob(all)
		if err != nil {
			rollback()
			return err
		}
		if err := writeRecord(tmp, recEntries, payload); err != nil {
			rollback()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		rollback()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		os.Remove(tmpName)
		return err
	}
	f.f.Close()
	nf, err := os.OpenFile(f.path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	f.f = nf
	if _, err := f.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func writeRecord(w io.Writer, rt recordType, payload []byte) error {
	var head [9]byte
	head[0] = byte(rt)
	binary.LittleEndian.PutUint32(head[1:5], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(head[5:9], uint32(len(payload)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func (f *FileWAL) Close() error { return f.f.Close() }
