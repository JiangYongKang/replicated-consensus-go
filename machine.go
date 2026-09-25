package raft

import "sync"

// Result 是状态机应用或查询的结果。
type Result struct {
	Value string
	Found bool
}

type dedupKey struct {
	ClientID uint64
	Seq      uint64
}

// Machine 是一个确定性的 KV 状态机，并内置幂等响应缓存。
// 缓存保证：相同 (ClientID, Seq) 的重复投递/重试只会得到首次应用的结果。
type Machine struct {
	mu     sync.Mutex
	kv     map[string]string
	cache  map[dedupKey]Result
	maxSeq map[uint64]uint64
}

func NewMachine() *Machine {
	return &Machine{
		kv:     make(map[string]string),
		cache:  make(map[dedupKey]Result),
		maxSeq: make(map[uint64]uint64),
	}
}

// MaxSeq 返回某客户端已应用的最大请求序号，用于识别过期的旧请求。
func (m *Machine) MaxSeq(clientID uint64) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxSeq[clientID]
}

// Apply 把命令应用到状态机；重复的 (ClientID, Seq) 直接返回缓存结果，
// 不会再次执行写操作（exactly-once 语义）。
func (m *Machine) Apply(cmd Command) Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := dedupKey{cmd.ClientID, cmd.Seq}
	if r, ok := m.cache[k]; ok {
		return r
	}
	r := Result{}
	switch cmd.Op {
	case OpPut:
		m.kv[cmd.Key] = cmd.Value
	case OpDelete:
		_, r.Found = m.kv[cmd.Key]
		delete(m.kv, cmd.Key)
	default:
		// 未知操作不改变状态，也不缓存（正常情况下不会出现）。
		return r
	}
	m.cache[k] = r
	if cmd.Seq > m.maxSeq[cmd.ClientID] {
		m.maxSeq[cmd.ClientID] = cmd.Seq
	}
	return r
}

// Lookup 读取已应用状态。
func (m *Machine) Lookup(key string) Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.kv[key]
	return Result{Value: v, Found: ok}
}

// Cached 返回幂等键对应的已缓存响应。
func (m *Machine) Cached(clientID, seq uint64) (Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.cache[dedupKey{clientID, seq}]
	return r, ok
}
