package raft

import (
	"context"
	"sync"
)

// Client 在一组副本之上提供带会话去重的 KV 写读接口。
// 每个写请求携带单调递增的 (ClientID, Seq)：
//   - 成功后序号才前移；
//   - 超时/不确定结果时以相同 (ClientID, Seq) 重发，服务端命中幂等缓存，
//     绝不会被重复应用；
//   - 明确不可重试错误（无多数派）上抛给调用方。
type Client struct {
	mu sync.Mutex

	id     string
	nodes  map[string]*Node
	leader string
	seq    uint64

	sched      *Scheduler
	retryDelay int64
	maxRetry   int
}

func NewClient(id string, nodes map[string]*Node, sched *Scheduler) *Client {
	return &Client{id: id, nodes: nodes, sched: sched, retryDelay: 5, maxRetry: 50}
}

// Seq 返回当前已成功使用的最大写序号。
func (c *Client) Seq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seq
}

func (c *Client) Set(ctx context.Context, key, val string) *WriteFuture {
	return c.submit(Command{Kind: CmdSet, Key: key, Val: val})
}

func (c *Client) Inc(ctx context.Context, key string) *WriteFuture {
	return c.submit(Command{Kind: CmdInc, Key: key})
}

// Get 经由领导者的 ReadIndex 完成线性一致读；跟随者会被客户端改道。
func (c *Client) Get(ctx context.Context, key string) *ReadFuture {
	out := newReadFuture()
	c.attemptRead(key, 0, out)
	return out
}

func (c *Client) submit(cmd Command) *WriteFuture {
	c.mu.Lock()
	c.seq++
	cmd.ClientID = c.id
	cmd.Seq = c.seq
	c.mu.Unlock()

	out := newWriteFuture(nil)
	c.attemptWrite(cmd, 0, out)
	return out
}

var attemptWriteDepth int

func (c *Client) attemptWrite(cmd Command, attempt int, out *WriteFuture) {
	attemptWriteDepth++
	if attemptWriteDepth > 100 {
		panic("attemptWrite deep recursion attempt=" + u64(uint64(attempt)) + " target-seq=" + u64(cmd.Seq))
	}
	defer func() { attemptWriteDepth-- }()
	if attempt >= c.maxRetry {
		out.complete(ApplyResult{}, &WriteError{
			Reason: ReasonNoQuorum,
			Detail: "client retries exhausted",
		})
		return
	}
	target, node := c.pickNode()
	if node == nil {
		out.complete(ApplyResult{}, &WriteError{Reason: ReasonNoQuorum, Detail: "no contactable node"})
		return
	}
	f := node.Propose(context.Background(), cmd)

	// Attach 保证同步完成的拒绝（not_leader）也会立即触发重试路径。
	f.Attach(func() {
		err := f.Err()
		if err == nil {
			out.complete(f.Result(), nil)
			return
		}
		var we *WriteError
		var reason FailReason
		term, hint := node.Term(), ""
		if asWriteError(err, &we) {
			reason = we.Reason
			term = we.Term
			hint = we.LeaderHint
		}
		switch reason {
		case ReasonNotLeader:
			// 跟随者/旧领导拒绝：记住提示或轮询下一个节点，同一 Seq 重发（幂等）。
			c.rememberHint(hint, target)
			c.sched.After(c.retryDelay, func() { c.attemptWrite(cmd, attempt+1, out) })
		case ReasonNoQuorum, ReasonTimeout:
			// 不确定结果：必须以相同 (ClientID, Seq) 重试，服务端去重保证不重复应用。
			// 关键：不能信任旧领导自带的 hint（它可能已被分区），必须轮换探测。
			c.rememberHint("", target)
			c.sched.After(c.retryDelay, func() { c.attemptWrite(cmd, attempt+1, out) })
		default:
			// StaleRequest 等明确失败：可区分地交给调用方，不吞错。
			out.complete(f.Result(), &WriteError{
				Reason: reason, Term: term, LeaderHint: hint,
				Detail: "write rejected: " + string(reason),
			})
		}
	})
}

func (c *Client) attemptRead(key string, attempt int, out *ReadFuture) {
	if attempt >= c.maxRetry {
		out.complete("", false, &WriteError{Reason: ReasonNoQuorum, Detail: "client retries exhausted"})
		return
	}
	target, node := c.pickNode()
	if node == nil {
		out.complete("", false, &WriteError{Reason: ReasonNoQuorum, Detail: "no contactable node"})
		return
	}
	f := node.Read(context.Background(), key)
	// Attach 保证同步完成的拒绝（not_leader）也会立即触发重定向。
	f.Attach(func() {
		err := f.Err()
		if err == nil {
			v, ok, _ := f.Get()
			out.complete(v, ok, nil)
			return
		}
		var we *WriteError
		if asWriteError(err, &we) && (we.Reason == ReasonNotLeader ||
			we.Reason == ReasonNoQuorum || we.Reason == ReasonTimeout) {
			// not_leader 可以信任 hint；no_quorum/timeout 不能信任（旧领导可能被分区）。
			hint := we.LeaderHint
			if we.Reason == ReasonNoQuorum || we.Reason == ReasonTimeout {
				hint = ""
			}
			c.rememberHint(hint, target)
			c.sched.After(c.retryDelay, func() { c.attemptRead(key, attempt+1, out) })
			return
		}
		out.complete("", false, err)
	})
}

// pickNode 优先选择已知领导者，否则按 ID 序取下一个候选，便于确定性探测。
func (c *Client) pickNode() (string, *Node) {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()
	if leader != "" {
		if nd := c.nodes[leader]; nd != nil {
			return leader, nd
		}
	}
	for _, id := range sortedNodeIDs(c.nodes) {
		return id, c.nodes[id]
	}
	return "", nil
}

func (c *Client) rememberHint(hint, tried string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hint != "" {
		c.leader = hint
		return
	}
	// 无提示时轮询：选字典序上的下一个节点。
	ids := sortedNodeIDs(c.nodes)
	next := ""
	for i, id := range ids {
		if id == tried && i+1 < len(ids) {
			next = ids[i+1]
			break
		}
	}
	c.leader = next
}

func asWriteError(err error, target **WriteError) bool {
	if we, ok := err.(*WriteError); ok {
		*target = we
		return true
	}
	return false
}
