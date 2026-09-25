package raft

import (
	"fmt"
	"sync"
)

// Cluster 在一个进程内装配多个副本，驱动确定性时钟与消息投递。
type Cluster struct {
	mu      sync.Mutex
	net     *SimNetwork
	nodes   map[int]*Node
	pers    map[int]Persister
	ids     []int
	stopped bool

	electionBase   int
	electionSpread int
	heartbeat      int
}

// NewCluster 创建 n 个副本的进程内集群。
// n 应为奇数；选举超时按副本 ID 错开，保证不会出现无休止平票。
func NewCluster(n int) *Cluster {
	return NewClusterWithConfig(n, 20, 7, 5)
}

// NewClusterWithConfig 允许自定义时间参数（单位均为 tick）。
func NewClusterWithConfig(n, electionBase, electionSpread, heartbeat int) *Cluster {
	if n < 1 {
		panic("cluster must have at least 1 node")
	}
	c := &Cluster{
		net:            NewSimNetwork(),
		nodes:          make(map[int]*Node),
		pers:           make(map[int]Persister),
		electionBase:   electionBase,
		electionSpread: electionSpread,
		heartbeat:      heartbeat,
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, i)
	}
	for _, id := range c.ids {
		c.restartLocked(id)
	}
	return c
}

// restartLocked 用 Persister 中的数据（重新）创建节点。
func (c *Cluster) restartLocked(id int) {
	ps := c.pers[id]
	if ps == nil {
		ps = NewMemoryPersister()
		c.pers[id] = ps
	}
	peers := make([]int, 0, len(c.ids)-1)
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	nd, err := NewNode(Config{
		ID:             id,
		Peers:          peers,
		ElectionBase:   c.electionBase,
		ElectionSpread: c.electionSpread,
		Heartbeat:      c.heartbeat,
	}, c.net, ps)
	if err != nil {
		panic(err)
	}
	nd.drive = c.stepOnce
	c.nodes[id] = nd
}

func (c *Cluster) node(id int) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// stepOnce 推进一轮：所有节点先 Tick（按 ID 顺序），再统一投递消息。
func (c *Cluster) stepOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	for _, id := range c.ids {
		if nd := c.nodes[id]; nd != nil {
			nd.Tick()
		}
	}
	c.net.Deliver(c.nodes)
}

// Tick 是 stepOnce 的公开别名。
func (c *Cluster) Tick() { c.stepOnce() }

// Advance 推进 d 个 tick。
func (c *Cluster) Advance(d int) {
	for i := 0; i < d; i++ {
		c.stepOnce()
	}
}

// RunUntilLeader 推进时钟直到产生一个任期稳定的领导者，
// 超过 maxTicks 仍未选出则返回错误。
func (c *Cluster) RunUntilLeader(maxTicks int) (int, uint64, error) {
	for i := 0; i < maxTicks; i++ {
		c.stepOnce()
		term := uint64(0)
		leader := -1
		for _, id := range c.ids {
			if nd := c.nodes[id]; nd != nil {
				st, t := nd.State()
				if st == Leader {
					if leader == -1 || t > term {
						leader, term = id, t
					}
				}
			}
		}
		if leader >= 0 {
			// 再跑几个 tick，确认没有更高任期的节点把它拉下来。
			stable := true
			for j := 0; j < c.heartbeat+2; j++ {
				c.stepOnce()
				if st, t := c.nodes[leader].State(); st != Leader || t != term {
					stable = false
					break
				}
			}
			if stable {
				return leader, term, nil
			}
		}
	}
	return -1, 0, fmt.Errorf("no leader elected within %d ticks", maxTicks)
}

// LeaderID 返回当前自认为领导者且任期最高的副本，没有则为 -1。
func (c *Cluster) LeaderID() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	leader, term := -1, uint64(0)
	for _, id := range c.ids {
		if nd := c.nodes[id]; nd != nil {
			if st, t := nd.State(); st == Leader && t >= term {
				leader, term = id, t
			}
		}
	}
	return leader
}

// Partition 将副本分成两个互相不可达的组。
func (c *Cluster) Partition(a, b []int) { c.net.Partition(a, b) }
func (c *Cluster) Isolate(id int)       { c.net.Isolate(id, c.ids) }
func (c *Cluster) Heal()                { c.net.Heal() }

// Restart 用持久化数据重启某副本（模拟崩溃恢复）；wipe=true 则清空持久化。
func (c *Cluster) Restart(id int, wipe bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if nd := c.nodes[id]; nd != nil {
		nd.Stop()
	}
	if wipe {
		if ps, ok := c.pers[id].(*MemoryPersister); ok {
			ps.Wipe()
		}
	}
	c.restartLocked(id)
	return nil
}

func (c *Cluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	for _, nd := range c.nodes {
		nd.Stop()
	}
}

// Status 用于测试日志：返回某节点的角色、任期、提交与应用序号。
func (c *Cluster) Status(id int) (state State, term, commit, applied uint64) {
	nd := c.node(id)
	st, t := nd.State()
	return st, t, nd.CommitIndex(), nd.AppliedIndex()
}
