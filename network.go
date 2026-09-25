package raft

import "sync"

// MsgType 是消息类型。
type MsgType uint8

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgAppend
	MsgAppendResp
)

// Message 是副本之间传递的 RPC 消息（请求与响应用同一结构，通过类型区分）。
type Message struct {
	Type MsgType
	From int
	To   int
	Term uint64

	// 选举
	VoteGranted bool

	// 日志复制
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
	Success      bool
	MatchIndex   uint64 // 响应方追加成功后的最新匹配下标

	// 读确认（领导者只读验证）
	ReadID uint64
}

// SimNetwork 是可控的回环网络：
//   - 分区期间的消息直接丢弃（不排队），模拟“请求超时”而非“延迟到达”；
//   - 所有消息在投递轮内同步交给目标节点，没有真实 goroutine/墙钟，
//     因此测试结果确定且可复现。
type SimNetwork struct {
	mu        sync.Mutex
	reachable map[int]map[int]bool
	pending   map[int][]Message
	readSeq   uint64
}

func NewSimNetwork() *SimNetwork {
	return &SimNetwork{
		reachable: make(map[int]map[int]bool),
		pending:   make(map[int][]Message),
	}
}

func (n *SimNetwork) register(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.reachable[id] == nil {
		n.reachable[id] = make(map[int]bool)
	}
	for other := range n.reachable {
		n.reachable[other][id] = true
		n.reachable[id][other] = true
	}
	n.reachable[id][id] = true
}

// Parted 表示从 from 到 to 的消息会被丢弃（单向分区）。
func (n *SimNetwork) Parted(from, to int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.reachable[from][to]
}

func (n *SimNetwork) cut(a, b int) {
	n.reachable[a][b] = false
}

// Partition 建立两个组之间的双向隔离；组内仍保持全连通。
func (n *SimNetwork) Partition(a, b []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, x := range a {
		for _, y := range b {
			n.cut(x, y)
			n.cut(y, x)
		}
	}
}

// Isolate 只隔离单个节点（与所有其他节点双向断连）。
func (n *SimNetwork) Isolate(id int, others []int) {
	n.Partition([]int{id}, others)
}

// Heal 恢复全连通。
func (n *SimNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for x := range n.reachable {
		for y := range n.reachable {
			n.reachable[x][y] = true
		}
	}
}

// send 由节点调用：立即按连通性决定投递或丢弃。
// 为保持“节点先全部 Tick、再统一投递”的确定性，消息先进入目标队列，
// 由 Deliver 在本轮末一次性处理。
func (n *SimNetwork) send(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.reachable[m.From][m.To] {
		return // 分区：静默丢弃，对端表现为超时
	}
	n.pending[m.To] = append(n.pending[m.To], m)
}

// Deliver 执行一轮投递：把本轮排队的消息交给目标节点处理。
// 投递期间新产生的回复（通常已在同一函数链内入队）会排空处理，
// 直到没有新消息，避免一轮内出现“响应迟到一个 tick”的人为延迟。
func (n *SimNetwork) Deliver(nodes map[int]*Node) {
	for {
		n.mu.Lock()
		var any bool
		batch := make(map[int][]Message)
		for id, q := range n.pending {
			if len(q) > 0 {
				batch[id] = q
				any = true
			}
			n.pending[id] = nil
		}
		n.mu.Unlock()
		if !any {
			return
		}
		// 按节点 ID 排序处理，保证顺序确定。
		ids := make([]int, 0, len(batch))
		for id := range batch {
			ids = append(ids, id)
		}
		for i := 1; i < len(ids); i++ {
			for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
				ids[j-1], ids[j] = ids[j], ids[j-1]
			}
		}
		for _, id := range ids {
			if nd := nodes[id]; nd != nil {
				for _, m := range batch[id] {
					nd.Step(m)
				}
			}
		}
	}
}

// NewReadID 返回一个单调递增的读确认标识。
func (n *SimNetwork) NewReadID() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.readSeq++
	return n.readSeq
}
