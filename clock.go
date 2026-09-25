package raft

import "sort"

// Clock 抽象时间源，测试中可替换为可控虚拟时钟。
type Clock interface {
	Now() int64
}

// SimClock 是毫秒粒度的可控虚拟时钟，不依赖任何墙钟或后台 goroutine。
type SimClock struct {
	t int64
}

func NewSimClock(start int64) *SimClock { return &SimClock{t: start} }

func (c *SimClock) Now() int64 { return c.t }

// Advance 仅推进时钟；事件触发由 Scheduler 负责。
func (c *SimClock) Advance(d int64) {
	if d < 0 {
		panic("raft: negative clock advance")
	}
	c.t += d
}

func (c *SimClock) Set(t int64) { c.t = t }

type timedEvent struct {
	id       int64
	at       int64
	seq      int64
	internal bool // 节点自维护事件（选举/心跳/在途请求超时）；静默期会自增殖
	fn       func()
}

// Scheduler 是基于 SimClock 的确定性事件循环：事件按 (at, seq) 排序触发，
// 同一时刻的事件严格按入队顺序执行，从而保证测试结论可重复。
type Scheduler struct {
	clock     *SimClock
	events    []timedEvent
	nextID    int64
	nextSeq   int64
	cancelled map[int64]bool
}

func NewScheduler(c *SimClock) *Scheduler {
	return &Scheduler{clock: c, cancelled: map[int64]bool{}}
}

// At 在虚拟时间 t 调度一次外部回调，返回可用于 Cancel 的事件 ID。
func (s *Scheduler) At(t int64, fn func()) int64 { return s.at(t, false, fn) }

// After 在相对延迟 d 后调度外部回调。
func (s *Scheduler) After(d int64, fn func()) int64 { return s.At(s.clock.Now()+d, fn) }

// AfterInternal 调度节点“内部”事件（选举/心跳/在途请求超时）。
// 内部事件在静默期不断自我增殖（例如永久分区中的重选），Advance 会跳过
// “内部事件连续区”，保证含永久分区的仿真也能确定终止。
func (s *Scheduler) AfterInternal(d int64, fn func()) int64 {
	return s.at(s.clock.Now()+d, true, fn)
}

func (s *Scheduler) at(t int64, internal bool, fn func()) int64 {
	s.nextID++
	s.nextSeq++
	s.events = append(s.events, timedEvent{id: s.nextID, at: t, seq: s.nextSeq, internal: internal, fn: fn})
	return s.nextID
}

func (s *Scheduler) Cancel(id int64) { s.cancelled[id] = true }

// Pending 返回未取消事件总数（含内部事件）。
func (s *Scheduler) Pending() int {
	n := 0
	for _, e := range s.events {
		if !s.cancelled[e.id] {
			n++
		}
	}
	return n
}

// PendingExternal 返回外部（测试/客户端）事件数，用于判断“是否还有事可做”。
func (s *Scheduler) PendingExternal() int {
	n := 0
	for _, e := range s.events {
		if !s.cancelled[e.id] && !e.internal {
			n++
		}
	}
	return n
}

func (s *Scheduler) sortedPeek() (timedEvent, bool) {
	sort.SliceStable(s.events, func(i, j int) bool {
		if s.events[i].at != s.events[j].at {
			return s.events[i].at < s.events[j].at
		}
		return s.events[i].seq < s.events[j].seq
	})
	for i, e := range s.events {
		if s.cancelled[e.id] {
			continue
		}
		return s.events[i], true
	}
	return timedEvent{}, false
}

// pop 取出并移除最近的未取消事件。
func (s *Scheduler) pop() (timedEvent, bool) {
	e, ok := s.sortedPeek()
	if !ok {
		return e, false
	}
	idx := -1
	for i, ev := range s.events {
		if ev.id == e.id {
			idx = i
			break
		}
	}
	s.events = append(s.events[:idx], s.events[idx+1:]...)
	delete(s.cancelled, e.id)
	return e, true
}

// Tick 触发最近一个未取消事件：先把时钟拨到其时刻，再执行回调。
func (s *Scheduler) Tick() bool {
	e, ok := s.pop()
	if !ok {
		return false
	}
	if e.at > s.clock.Now() {
		s.clock.Set(e.at)
	}
	e.fn()
	return true
}

// nextExternalTime 返回时刻在 (after, deadline] 内最近的外部事件时刻。
func (s *Scheduler) nextExternalTime(after, deadline int64) (int64, bool) {
	best := int64(0)
	found := false
	for _, e := range s.events {
		if s.cancelled[e.id] || e.internal {
			continue
		}
		if e.at > after && e.at <= deadline && (!found || e.at < best) {
			best, found = e.at, true
		}
	}
	return best, found
}

// RunUntil 触发所有时刻 <= deadline 的事件（含内部事件）。
// maxEvents 是硬上限，防止逻辑缺陷或永久分区自增殖事件导致死循环；
// 传入 <=0 表示使用默认上限。
func (s *Scheduler) RunUntil(deadline int64) int {
	return s.runUntil(deadline, 1_000_000)
}

func (s *Scheduler) runUntil(deadline int64, maxEvents int) int {
	n := 0
	for n < maxEvents {
		e, ok := s.sortedPeek()
		if !ok || e.at > deadline {
			break
		}
		s.Tick()
		n++
	}
	if s.clock.Now() < deadline {
		s.clock.Set(deadline)
	}
	return n
}

// RunFor 等价于 RunUntil(Now()+d)。
func (s *Scheduler) RunFor(d int64) int { return s.RunUntil(s.clock.Now() + d) }

// StepExternal 推进到并处理“外部事件”（网络投递/客户端重试）时刻 <= 该时刻的
// 全部事件（含同时刻内部定时器）。外部事件定义中包含“已到期（at<=now）”的事件。
// 若不存在任何外部事件返回 false。
func (s *Scheduler) StepExternal() bool {
	if _, ok := s.sortedPeek(); !ok {
		return false
	}
	t, found := s.nextExternalTime(s.clock.Now()-1, 1<<62)
	if !found {
		return false
	}
	for {
		cur, ok := s.sortedPeek()
		if !ok || cur.at > t {
			break
		}
		s.Tick()
	}
	if s.clock.Now() < t {
		s.clock.Set(t)
	}
	return true
}

// Settle 是确定性收敛驱动器：只要存在外部事件（网络消息/客户端重试），
// 就处理到最近外部事件时刻（该时刻之前的选举/心跳等内部事件照常触发）。
// 当外部事件清空（协议相对静默）即停止。永久分区中只剩内部重选时也会立即停止，
// 因此必然终止；调用方若还要等待内部超时，应使用 RunFor。
func (s *Scheduler) Settle(maxExternal int) int {
	if maxExternal <= 0 {
		maxExternal = 100000
	}
	n := 0
	for n < maxExternal {
		if !s.StepExternal() {
			break
		}
		n++
	}
	return n
}

// Stabilize 等价于 Settle（保留更直观的名字）。
func (s *Scheduler) Stabilize(maxSteps int) int { return s.Settle(maxSteps) }
