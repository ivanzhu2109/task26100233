package irontable

import "sync"

// txState 事务状态。
type txState int

const (
	stateActive txState = iota
	stateCommitted
	stateAborted
)

// Txn 代表一个事务。同一事务的调用须顺序执行，
// 但 Rollback 允许由其他线程并发调用以主动撤销。
type Txn struct {
	id       uint64
	db       *DB
	snapshot uint64

	mu        sync.Mutex
	state     txState
	writeSet  map[string]any
	heldLocks map[string]struct{}
	done      chan struct{} // 事务结束时关闭，用于唤醒等待中的写
}

// Get 读取一行：先读自己的写集，再读快照中的已提交版本。
// 读取从不等待行锁。
func (tx *Txn) Get(row string) (any, bool, error) {
	tx.mu.Lock()
	if tx.state != stateActive {
		tx.mu.Unlock()
		return nil, false, ErrTxnDone
	}
	if v, ok := tx.writeSet[row]; ok {
		tx.mu.Unlock()
		return v, true, nil
	}
	snapshot := tx.snapshot
	tx.mu.Unlock()

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	chain := tx.db.data[row]
	for i := len(chain) - 1; i >= 0; i-- {
		if chain[i].version <= snapshot {
			return chain[i].value, true, nil
		}
	}
	return nil, false, nil
}

// Put 覆盖一行。必要时等待行锁，最长5秒；
// 超时、冲突或被主动回滚都会撤销整笔事务。
func (tx *Txn) Put(row string, value any) error {
	tx.mu.Lock()
	if tx.state != stateActive {
		tx.mu.Unlock()
		return ErrTxnDone
	}
	if _, ok := tx.writeSet[row]; ok {
		tx.writeSet[row] = value
		tx.mu.Unlock()
		return nil
	}
	tx.mu.Unlock()

	tx.db.mu.Lock()
	lk, ok := tx.db.locks[row]
	if !ok {
		lk = newRowLock()
		tx.db.locks[row] = lk
	}
	tx.db.mu.Unlock()

	if !lk.acquire(tx) {
		tx.mu.Lock()
		defer tx.mu.Unlock()
		if tx.state != stateActive {
			return ErrAborted
		}
		tx.abortLocked()
		return ErrWaitTimeout
	}

	tx.mu.Lock()
	if tx.state != stateActive {
		tx.mu.Unlock()
		lk.release(tx)
		return ErrAborted
	}

	tx.db.mu.Lock()
	chain := tx.db.data[row]
	conflict := len(chain) > 0 && chain[len(chain)-1].version > tx.snapshot
	tx.db.mu.Unlock()
	if conflict {
		tx.mu.Unlock()
		lk.release(tx)
		tx.mu.Lock()
		if tx.state == stateActive {
			tx.abortLocked()
		}
		tx.mu.Unlock()
		return ErrConflict
	}

	tx.writeSet[row] = value
	tx.heldLocks[row] = struct{}{}
	tx.mu.Unlock()
	return nil
}

// Commit 整体提交全部更新，之后事务不可再使用。
func (tx *Txn) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stateActive {
		return ErrTxnDone
	}
	tx.db.mu.Lock()
	tx.db.version++
	for row, v := range tx.writeSet {
		tx.db.data[row] = append(tx.db.data[row], versionedValue{value: v, version: tx.db.version})
	}
	tx.db.mu.Unlock()
	tx.state = stateCommitted
	close(tx.done)
	tx.releaseAllLocked()
	return nil
}

// Rollback 撤销事务，释放全部行锁。可被其他线程调用，
// 等待中的 Put 会因此被唤醒并失败。
func (tx *Txn) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stateActive {
		return ErrTxnDone
	}
	tx.abortLocked()
	return nil
}

// abortLocked 将事务标记为已撤销并释放全部资源。调用方须持有 tx.mu。
func (tx *Txn) abortLocked() {
	tx.state = stateAborted
	close(tx.done)
	tx.writeSet = make(map[string]any)
	tx.releaseAllLocked()
}

// releaseAllLocked 释放事务持有的全部行锁。调用方须持有 tx.mu。
func (tx *Txn) releaseAllLocked() {
	for row := range tx.heldLocks {
		tx.db.mu.Lock()
		lk := tx.db.locks[row]
		tx.db.mu.Unlock()
		if lk != nil {
			lk.release(tx)
		}
	}
	tx.heldLocks = make(map[string]struct{})
}
