package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// PersistentDataVersion 是当前持久化格式版本。
const PersistentDataVersion uint16 = 1

// PersistentData 是节点重启后必须恢复的数据：硬状态 + 日志。
type PersistentData struct {
	Version     uint16
	CurrentTerm uint64
	VotedFor    int // -1 表示本轮未投票
	Log         []LogEntry
}

// Persister 是持久化接口；测试使用内存实现，生产可使用文件实现。
type Persister interface {
	Load() (*PersistentData, error)
	Save(data *PersistentData) error
}

func defaultPersistentData() *PersistentData {
	return &PersistentData{Version: PersistentDataVersion, CurrentTerm: 0, VotedFor: -1}
}

// MemoryPersister 是进程内 Persister，用于确定性测试；重启节点时保留它即模拟“磁盘幸存”。
type MemoryPersister struct {
	mu  sync.Mutex
	raw []byte
}

func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

func (p *MemoryPersister) Load() (*PersistentData, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.raw) == 0 {
		return defaultPersistentData(), nil
	}
	return decodePersistent(append([]byte(nil), p.raw...))
}

func (p *MemoryPersister) Save(data *PersistentData) error {
	raw := encodePersistent(data)
	p.mu.Lock()
	p.raw = raw
	p.mu.Unlock()
	return nil
}

// Wipe 清空“磁盘”（用于模拟存储介质损坏/全新节点）。
func (p *MemoryPersister) Wipe() {
	p.mu.Lock()
	p.raw = nil
	p.mu.Unlock()
}

// FilePersister 将状态原子地写入单个文件（写临时文件 + fsync + rename）。
type FilePersister struct {
	mu sync.Mutex
	fn string
}

func NewFilePersister(path string) *FilePersister { return &FilePersister{fn: path} }

func (p *FilePersister) Load() (*PersistentData, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	raw, err := os.ReadFile(p.fn)
	if err != nil {
		if os.IsNotExist(err) {
			return defaultPersistentData(), nil
		}
		return nil, err
	}
	return decodePersistent(raw)
}

func (p *FilePersister) Save(data *PersistentData) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	raw := encodePersistent(data)
	dir := filepath.Dir(p.fn)
	tmp, err := os.CreateTemp(dir, ".raft-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, p.fn)
}
