package irontable

import "errors"

var (
	// ErrTxnDone 表示事务已提交或已回滚，不能再执行任何操作。
	ErrTxnDone = errors.New("irontable: transaction already finished")
	// ErrConflict 表示目标行在本事务开始后被其他事务提交过更新，整笔事务必须回滚。
	ErrConflict = errors.New("irontable: write conflict, transaction aborted")
	// ErrWaitTimeout 表示等待行锁超过上限，整笔事务已被撤销。
	ErrWaitTimeout = errors.New("irontable: lock wait timeout, transaction aborted")
	// ErrAborted 表示事务被其他线程主动回滚。
	ErrAborted = errors.New("irontable: transaction aborted")
)
