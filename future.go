package raft

import "sync"

// WriteFuture 表示一次写请求的异步结论。
// 确定性模拟环境下无后台 goroutine：结果在事件循环中被兑现，
// 调用方在推进调度器后通过 Done/Err/Get 观察。
type WriteFuture struct {
	mu     sync.Mutex
	done   chan struct{}
	result ApplyResult
	err    error
	finish func()
	fired  bool
	once   sync.Once
}

func newWriteFuture(onDone func()) *WriteFuture {
	return &WriteFuture{done: make(chan struct{}), finish: onDone}
}

// Attach 注册兑现回调；若结论已经产生（例如节点同步返回 not_leader），
// 则立即在当前调用栈执行回调，杜绝“先完成后挂回调导致永不重试”。
func (f *WriteFuture) Attach(cb func()) {
	f.mu.Lock()
	fired := f.fired
	if !fired {
		f.finish = cb
	}
	f.mu.Unlock()
	if fired {
		cb()
	}
}

func (f *WriteFuture) Done() <-chan struct{} { return f.done }

// Err 返回失败原因；尚未兑现时返回 ErrTimeout（语义：此刻尚无结论）。
func (f *WriteFuture) Err() error {
	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.err
	default:
		return ErrTimeout
	}
}

func (f *WriteFuture) Result() ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.result
}

// Get 阻塞等待结论（真实运行环境用）；模拟事件循环中请先推进 Scheduler。
func (f *WriteFuture) Get() (ApplyResult, error) {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.result, f.err
}

// complete 兑现结论，且保证只兑现一次（重复投递/超时竞争下结论不被覆盖）。
func (f *WriteFuture) complete(result ApplyResult, err error) {
	f.once.Do(func() {
		f.mu.Lock()
		f.result = result
		f.err = err
		f.fired = true
		cb := f.finish
		f.mu.Unlock()
		close(f.done)
		if cb != nil {
			cb()
		}
	})
}

// ReadFuture 表示一次线性一致读的异步结论。
type ReadFuture struct {
	mu     sync.Mutex
	done   chan struct{}
	value  string
	found  bool
	err    error
	finish func()
	fired  bool
	once   sync.Once
}

func newReadFuture() *ReadFuture { return &ReadFuture{done: make(chan struct{})} }

func (f *ReadFuture) Done() <-chan struct{} { return f.done }

func (f *ReadFuture) Err() error {
	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.err
	default:
		return ErrTimeout
	}
}

func (f *ReadFuture) Get() (string, bool, error) {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.value, f.found, f.err
}

// Attach 同 WriteFuture.Attach。
func (f *ReadFuture) Attach(cb func()) {
	f.mu.Lock()
	fired := f.fired
	if !fired {
		f.finish = cb
	}
	f.mu.Unlock()
	if fired {
		cb()
	}
}

func (f *ReadFuture) complete(value string, found bool, err error) {
	f.once.Do(func() {
		f.mu.Lock()
		f.value = value
		f.found = found
		f.err = err
		f.fired = true
		cb := f.finish
		f.mu.Unlock()
		close(f.done)
		if cb != nil {
			cb()
		}
	})
}
