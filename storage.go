package raft

// HardState 是必须持久化的、决定投票与日志安全性的最小状态。
type HardState struct {
	Term        uint64
	VoteFor     string
	CommitIndex uint64
}

// Storage 抽象 Raft 日志与硬状态的持久层。
// 所有方法均要求实现自身保证崩溃一致性；Index 从 1 开始，0 表示空日志。
type Storage interface {
	HardState() HardState
	SaveHardState(h HardState) error

	Entries(lo, hi uint64) ([]Entry, error) // 半开区间 [lo, hi)
	Entry(idx uint64) (Entry, error)
	LastIndex() uint64
	TermAt(idx uint64) (uint64, error) // idx==0 时返回 0, nil

	// Append 将 entries 追加到 prevIdx 之后；若与已有日志在 index/term 上冲突，
	// 则截断冲突点之后的旧日志。重复投递相同条目必须幂等。
	Append(prevIdx uint64, entries []Entry) error

	Close() error
}

var _ Storage = (*MemoryStorage)(nil)

// MemoryStorage 是进程内存储，用于不需要跨“重启”验证的单元测试。
type MemoryStorage struct {
	hard HardState
	log  []Entry // 下标 0 为哨兵空 Entry（Index=0）
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{log: []Entry{{}}}
}

func (m *MemoryStorage) HardState() HardState { return m.hard }

func (m *MemoryStorage) SaveHardState(h HardState) error { m.hard = h; return nil }

func (m *MemoryStorage) Entries(lo, hi uint64) ([]Entry, error) {
	if lo < 1 || hi < lo || hi > uint64(len(m.log)) {
		return nil, ErrCorruptWAL
	}
	out := make([]Entry, hi-lo)
	copy(out, m.log[lo:hi])
	return out, nil
}

func (m *MemoryStorage) Entry(idx uint64) (Entry, error) {
	if idx < 1 || idx >= uint64(len(m.log)) {
		return Entry{}, ErrCorruptWAL
	}
	return m.log[idx], nil
}

func (m *MemoryStorage) LastIndex() uint64 { return uint64(len(m.log)) - 1 }

func (m *MemoryStorage) TermAt(idx uint64) (uint64, error) {
	if idx == 0 {
		return 0, nil
	}
	e, err := m.Entry(idx)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

// Append 按 Raft 规则将 entries 接在 prevIdx 之后（entries 连续且
// entries[i].Index == prevIdx+1+i）：
//   - 与现有条目 index/term 相同：跳过（重复投递幂等）；
//   - 同位置 term 不同：截断该位置及之后日志再写入；
//   - 超出末尾：追加。
func (m *MemoryStorage) Append(prevIdx uint64, entries []Entry) error {
	if prevIdx >= uint64(len(m.log)) {
		return ErrCorruptWAL
	}
	for i, e := range entries {
		if want := prevIdx + uint64(i) + 1; e.Index != want {
			return ErrCorruptWAL
		}
		pos := e.Index
		if pos < uint64(len(m.log)) {
			if m.log[pos].Term == e.Term {
				continue // 幂等：任期一致视为同一位置已存在
			}
			m.log = m.log[:pos]
		}
		m.log = append(m.log, e)
	}
	return nil
}

func (m *MemoryStorage) Close() error { return nil }
