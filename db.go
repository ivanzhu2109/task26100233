// Package irontable 提供进程内数据库事务执行库。
// 数据全部保存在内存中，支持快照读、行级写锁与原子提交。
package irontable

import (
	"errors"
	"sync"
	"time"
)

// 默认写等待上限。
const DefaultWaitTimeout = 5 * time.Second

// 库级错误。
var (
	ErrRowNotFound  = errors.New("irontable: row not found")
	ErrTxnFinished  = errors.New("irontable: transaction already finished")
	ErrTxnAborted   = errors.New("irontable: transaction aborted")
	ErrConflict     = errors.New("irontable: row was committed by another transaction")
	ErrWaitTimeout  = errors.New("irontable: timed out waiting for row lock")
	ErrRowNotExists = errors.New("irontable: cannot put a row that does not exist")
)

// Value 是行的业务内容，对本库完全透明。
type Value = any

// versionedValue 是某行的一次已提交版本。
type versionedValue struct {
	val Value
	ver uint64
}

// rowState 是一行的已提交版本链与写锁状态。
type rowState struct {
	versions []versionedValue // 按版本号升序
	holder   uint64           // 当前写锁持有事务 ID，0 表示空闲
	ch       chan struct{}    // 锁释放时关闭并替换，用于唤醒等待者
}

// DB 是内存事务库实例。
type DB struct {
	mu          sync.Mutex
	seq         uint64 // 全局提交序列号
	nextTxnID   uint64
	rows        map[string]*rowState
	waitTimeout time.Duration
}

// Option 是 DB 的可选配置。
type Option func(*DB)

// WithWaitTimeout 覆盖默认的写等待上限（主要供测试使用）。
func WithWaitTimeout(d time.Duration) Option {
	return func(db *DB) { db.waitTimeout = d }
}

// NewDB 用调用方提供的初始数据创建实例。
func NewDB(initial map[string]Value, opts ...Option) *DB {
	db := &DB{
		seq:         1, // 初始数据视为版本 1 的一次提交
		rows:        make(map[string]*rowState, len(initial)),
		waitTimeout: DefaultWaitTimeout,
	}
	for _, opt := range opts {
		opt(db)
	}
	for key, val := range initial {
		db.rows[key] = &rowState{
			versions: []versionedValue{{val: val, ver: 1}},
			ch:       make(chan struct{}),
		}
	}
	return db
}

// Begin 开启一个新事务，其读视图固定为当前已提交序列号。
func (db *DB) Begin() *Txn {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.nextTxnID++
	return &Txn{
		db:       db,
		id:       db.nextTxnID,
		startSeq: db.seq,
		writes:   make(map[string]Value),
		abortCh:  make(chan struct{}),
	}
}

// valueAt 返回不超过 seq 的最新已提交版本。
func (r *rowState) valueAt(seq uint64) (Value, bool) {
	// versions 按 ver 升序，线性回扫即可（每行版本数通常很小）。
	for i := len(r.versions) - 1; i >= 0; i-- {
		if r.versions[i].ver <= seq {
			return r.versions[i].val, true
		}
	}
	return nil, false
}

// latestVersion 返回该行的最新已提交版本号。
func (r *rowState) latestVersion() uint64 {
	return r.versions[len(r.versions)-1].ver
}

// releaseLock 释放行的写锁并唤醒等待者；调用方须持有 db.mu。
func (r *rowState) releaseLock() {
	r.holder = 0
	close(r.ch)
	r.ch = make(chan struct{})
}
