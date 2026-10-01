package irontable

import "sync"

// versionedValue 是某行的一个已提交版本。
type versionedValue struct {
	value   any
	version uint64
}

// DB 是进程内存中的事务型行存储。行内容为不透明值。
type DB struct {
	mu       sync.Mutex
	data     map[string][]versionedValue // 每行按版本升序的已提交版本链
	version  uint64                      // 全局提交版本号，单调递增
	locks    map[string]*rowLock         // 行锁表
	nextTxID uint64
}

// NewDB 创建空库。初始数据由调用方通过 Seed 提供。
func NewDB() *DB {
	return &DB{
		data:  make(map[string][]versionedValue),
		locks: make(map[string]*rowLock),
	}
}

// Seed 用调用方提供的初始数据填充库，只能在没有任何事务前调用。
func (db *DB) Seed(rows map[string]any) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.version++
	for id, v := range rows {
		db.data[id] = []versionedValue{{value: v, version: db.version}}
	}
}

// Begin 开启一个事务，快照为当前已提交视图。
func (db *DB) Begin() *Txn {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.version++ // 保证每个事务快照号唯一且晚于已提交数据
	db.nextTxID++
	tx := &Txn{
		id:        db.nextTxID,
		db:        db,
		snapshot:  db.version,
		writeSet:  make(map[string]any),
		heldLocks: make(map[string]struct{}),
		done:      make(chan struct{}),
	}
	return tx
}
