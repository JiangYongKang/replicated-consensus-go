package raft

import "sync"

// Transport 抽象副本间 RPC。所有投递都经过 Scheduler，
// 因而即使单机多副本，消息先后也完全确定、可复现。
type Transport interface {
	// Send 投递一条消息；分区期间消息应被丢弃（像真实网络分区一样）。
	Send(m Message)
	// Register 将一个节点绑定为消息接收者。
	Register(id string, h MessageHandler)
	// SetPartitions 直接替换可达规则：rules[a][b]==true 表示 a 可送达 b。
	SetPartitions(rules map[string]map[string]bool)
	// SetDelay 设置基础传播延迟（虚拟毫秒）。
	SetDelay(ms int64)
}

// MessageHandler 处理一条到达的副本间消息。
type MessageHandler func(m Message)

type receiver struct {
	id      string
	handler MessageHandler
}

// LoopTransport 是进程内回环传输：按 SimClock 延迟投递，可任意设置分区。
// 消息从不内联执行——始终经 Scheduler 排队，避免“调用栈嵌套改变执行顺序”。
type LoopTransport struct {
	mu        sync.Mutex
	sched     *Scheduler
	delay     int64
	receivers map[string]*receiver
	// rules[a][b]==true 表示 a 可以送达 b（双向需两条规则）。
	rules map[string]map[string]bool
}

func NewLoopTransport(sched *Scheduler) *LoopTransport {
	return &LoopTransport{sched: sched, delay: 1, receivers: map[string]*receiver{}, rules: map[string]map[string]bool{}}
}

func (t *LoopTransport) Register(id string, h MessageHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.receivers[id] = &receiver{id: id, handler: h}
}

func (t *LoopTransport) SetDelay(ms int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delay = ms
}

func (t *LoopTransport) SetPartitions(rules map[string]map[string]bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rules = cloneRules(rules)
}

// Send 按固定延迟把消息排入调度器；若发送方到接收方不可达则静默丢弃
// （模拟真实网络分区中丢失的报文，节点只能依靠超时感知）。
func (t *LoopTransport) Send(m Message) {
	t.mu.Lock()
	rcv, ok := t.receivers[m.To]
	if !ok {
		t.mu.Unlock()
		return
	}
	if t.rules != nil && !t.reachableLocked(m.From, m.To) {
		t.mu.Unlock()
		return
	}
	delay := t.delay
	h := rcv.handler
	t.mu.Unlock()

	t.sched.After(delay, func() { h(m) })
}

func (t *LoopTransport) reachableLocked(from, to string) bool {
	row, ok := t.rules[from]
	if !ok {
		return false
	}
	return row[to]
}

// FullyConnected 返回一个全连通分区规则表（含自身环回）。
func FullyConnected(ids []string) map[string]map[string]bool {
	rules := map[string]map[string]bool{}
	for _, a := range ids {
		row := map[string]bool{}
		for _, b := range ids {
			row[b] = true
		}
		rules[a] = row
	}
	return rules
}

// Isolated 复制规则表并将 id 与其他节点双向隔离（保留 id 的自身环回，
// 因为领导者仍可向自己提交并被多数派拒绝，从而验证少数派不服务写）。
func Isolated(base map[string]map[string]bool, ids []string, id string) map[string]map[string]bool {
	out := cloneRules(base)
	for _, other := range ids {
		if other == id {
			continue
		}
		if row := out[id]; row != nil {
			row[other] = false
		}
		if row := out[other]; row != nil {
			row[id] = false
		}
	}
	return out
}

func cloneRules(in map[string]map[string]bool) map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(in))
	for k, row := range in {
		nr := make(map[string]bool, len(row))
		for kk, vv := range row {
			nr[kk] = vv
		}
		out[k] = nr
	}
	return out
}
