package raft

import (
	"strconv"
	"sync"
)

// ApplyResult 是一条已提交命令应用到状态机后的结果。
type ApplyResult struct {
	Index     uint64
	Key       string
	Value     string
	Duplicate bool  // 命中幂等缓存，未再次改变状态
	Err       error // 命令自身被状态机拒绝（如冲突的旧请求）
}

// StateMachine 是确定性复制状态机：相同的已提交日志序列必须产生相同结果。
type StateMachine interface {
	Apply(e Entry) ApplyResult
	Read(key string) (string, bool)
	LastApplied() uint64
}

// SessionEntry 记录某客户端最后一次已应用请求及其结果，用于去重。
// 重启后随已提交日志重放而重建，无需单独持久化。
type SessionEntry struct {
	LastSeq uint64
	Cmd     Command
	Result  ApplyResult
}

// KVStore 是一个极简键值状态机：set 写字符串，inc 对十进制整数值自增。
type KVStore struct {
	mu       sync.Mutex
	data     map[string]string
	sessions map[string]SessionEntry
	applied  uint64
}

func NewKVStore() *KVStore {
	return &KVStore{data: map[string]string{}, sessions: map[string]SessionEntry{}}
}

// Apply 按已提交日志顺序应用命令。对同一 (ClientID, Seq) 的重复投递
// 返回首次结果并标记 Duplicate；序号倒退返回 ErrStaleRequest。
func (k *KVStore) Apply(e Entry) ApplyResult {
	k.mu.Lock()
	defer k.mu.Unlock()

	cmd := e.Cmd
	var r ApplyResult
	if s, ok := k.sessions[cmd.ClientID]; ok && cmd.ClientID != "" {
		switch {
		case cmd.Seq == s.LastSeq && sameCmd(s.Cmd, cmd):
			// 完全相同的重发：返回首次结论，绝不二次应用。
			r = s.Result
			r.Index = e.Index
			r.Duplicate = true
			k.applied = e.Index
			return r
		case cmd.Seq == s.LastSeq:
			// 同序号但命令体不同：明确拒绝为冲突的旧请求。
			r = ApplyResult{Index: e.Index, Key: cmd.Key, Err: ErrStaleRequest}
		case cmd.Seq < s.LastSeq:
			// 落后的旧请求残留：明确拒绝，绝不静默覆盖。
			r = ApplyResult{Index: e.Index, Key: cmd.Key, Err: ErrStaleRequest}
		default:
			r = k.applyNew(cmd)
		}
	} else {
		r = k.applyNew(cmd)
	}
	r.Index = e.Index

	// 条目已按日志顺序提交，即使命令体被拒绝也要推进已应用序号。
	k.applied = e.Index
	if cmd.ClientID != "" && r.Err == nil {
		k.sessions[cmd.ClientID] = SessionEntry{LastSeq: cmd.Seq, Cmd: cmd, Result: r}
	}
	return r
}

func (k *KVStore) applyNew(cmd Command) ApplyResult {
	r := ApplyResult{Index: 0, Key: cmd.Key}
	switch cmd.Kind {
	case CmdSet:
		k.data[cmd.Key] = cmd.Val
		r.Value = cmd.Val
	case CmdInc:
		v := int64(0)
		if cur, ok := k.data[cmd.Key]; ok && cur != "" {
			parsed, err := strconv.ParseInt(cur, 10, 64)
			if err != nil {
				r.Err = ErrStaleRequest
				break
			}
			v = parsed
		}
		v++
		s := strconv.FormatInt(v, 10)
		k.data[cmd.Key] = s
		r.Value = s
	case CmdGet:
		r.Value, _ = k.data[cmd.Key]
	default:
		r.Err = ErrStaleRequest
	}
	return r
}

func (k *KVStore) Read(key string) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.data[key]
	return v, ok
}

func (k *KVStore) LastApplied() uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.applied
}

// LookupSession 查询去重缓存（重启后由已提交日志重建）。
func (k *KVStore) LookupSession(client string) (SessionEntry, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	s, ok := k.sessions[client]
	return s, ok
}

func sameCmd(a, b Command) bool {
	return a.Kind == b.Kind && a.Key == b.Key && a.Val == b.Val
}
