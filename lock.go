package irontable

import (
	"sync"
	"time"
)

// lockWaitTimeout 是写入等待行锁的上限。
const lockWaitTimeout = 5 * time.Second

// rowLock 是单行互斥锁，支持超时等待与事务撤销唤醒。
type rowLock struct {
	mu    sync.Mutex
	owner *Txn
	sem   chan struct{} // 容量1的信号量：占位即持有
}

func newRowLock() *rowLock {
	return &rowLock{sem: make(chan struct{}, 1)}
}

// acquire 尝试为 tx 占有该锁。若 tx 已持有则直接成功。
// 返回 false 表示等待被 tx.done 中断或超时（调用方负责后续处理）。
func (l *rowLock) acquire(tx *Txn) bool {
	l.mu.Lock()
	if l.owner == tx {
		l.mu.Unlock()
		return true
	}
	l.mu.Unlock()

	timer := time.NewTimer(lockWaitTimeout)
	defer timer.Stop()
	select {
	case l.sem <- struct{}{}:
	case <-tx.done:
		return false
	case <-timer.C:
		return false
	}

	l.mu.Lock()
	l.owner = tx
	l.mu.Unlock()
	return true
}

// release 释放锁，只有持有者可释放。
func (l *rowLock) release(tx *Txn) {
	l.mu.Lock()
	if l.owner != tx {
		l.mu.Unlock()
		return
	}
	l.owner = nil
	l.mu.Unlock()
	select {
	case <-l.sem:
	default:
	}
}
