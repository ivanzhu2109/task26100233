package irontable

import "time"

// txnState 表示事务生命周期。
type txnState int

const (
	txnActive txnState = iota
	txnCommitted
	txnAborted
)

// Txn 是一次事务。同一事务的方法须按调用顺序执行，
// 但 Rollback 允许由其他线程并发调用。
type Txn struct {
	db       *DB
	id       uint64
	startSeq uint64
	writes   map[string]Value // 本事务已完成的更新
	state    txnState
	abortCh  chan struct{} // 回滚时关闭，用于唤醒等待中的写
}

// Get 读取一行：优先返回本事务的更新，否则返回开始时的已提交视图。
func (t *Txn) Get(key string) (Value, bool, error) {
	db := t.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return nil, false, err
	}
	if val, ok := t.writes[key]; ok {
		return val, true, nil
	}
	row := db.rows[key]
	if row == nil {
		return nil, false, nil
	}
	val, ok := row.valueAt(t.startSeq)
	return val, ok, nil
}

// Put 覆盖一个已存在的行；必要时等待行锁，超时或冲突则整笔回滚。
func (t *Txn) Put(key string, v Value) error {
	db := t.db
	var timer *time.Timer
	var timeout <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		db.mu.Lock()
		if err := t.checkActive(); err != nil {
			db.mu.Unlock()
			return err
		}
		row := db.rows[key]
		if row == nil {
			db.mu.Unlock()
			return ErrRowNotExists
		}
		if _, ok := t.writes[key]; ok {
			// 本事务已持有该行的锁，直接更新。
			t.writes[key] = v
			db.mu.Unlock()
			return nil
		}
		if row.holder == 0 {
			// 行空闲：若本事务开始后该行被他人提交过，整笔失败回滚。
			if row.latestVersion() > t.startSeq {
				t.abortLocked()
				db.mu.Unlock()
				return ErrConflict
			}
			row.holder = t.id
			t.writes[key] = v
			db.mu.Unlock()
			return nil
		}
		// 行被其他事务持有：等待释放、被回滚或超时。
		waitCh := row.ch
		db.mu.Unlock()

		if timer == nil {
			timer = time.NewTimer(db.waitTimeout)
			timeout = timer.C
		}
		select {
		case <-waitCh:
			// 锁已释放，重新竞争。
		case <-t.abortCh:
			return ErrTxnAborted
		case <-timeout:
			db.mu.Lock()
			if t.state == txnActive {
				t.abortLocked()
			}
			db.mu.Unlock()
			return ErrWaitTimeout
		}
	}
}

// Commit 将本事务的全部更新原子地提交。
func (t *Txn) Commit() error {
	db := t.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := t.checkActive(); err != nil {
		return err
	}
	db.seq++
	for key, val := range t.writes {
		row := db.rows[key]
		row.versions = append(row.versions, versionedValue{val: val, ver: db.seq})
		row.releaseLock()
	}
	t.state = txnCommitted
	return nil
}

// Rollback 撤销事务，可由其他线程调用以中断等待中的写。
func (t *Txn) Rollback() {
	db := t.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if t.state == txnActive {
		t.abortLocked()
	}
}

// checkActive 校验事务仍在进行；调用方须持有 db.mu。
func (t *Txn) checkActive() error {
	switch t.state {
	case txnActive:
		return nil
	case txnAborted:
		return ErrTxnAborted
	default:
		return ErrTxnFinished
	}
}

// abortLocked 回滚事务：释放全部行锁并唤醒等待者；调用方须持有 db.mu。
func (t *Txn) abortLocked() {
	t.state = txnAborted
	for key := range t.writes {
		if row := t.db.rows[key]; row != nil && row.holder == t.id {
			row.releaseLock()
		}
	}
	close(t.abortCh)
}
