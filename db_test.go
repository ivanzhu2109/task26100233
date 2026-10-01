package irontable

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustGet(t *testing.T, txn *Txn, key string, want Value) {
	t.Helper()
	got, ok, err := txn.Get(key)
	if err != nil {
		t.Fatalf("Get(%s) err: %v", key, err)
	}
	if !ok {
		t.Fatalf("Get(%s) not found", key)
	}
	if got != want {
		t.Fatalf("Get(%s) = %v, want %v", key, got, want)
	}
}

// 多行提交前后：旧事务保持旧视图，新事务看到完整新视图，逐行核对。
func TestSnapshotIsolationAcrossCommit(t *testing.T) {
	db := NewDB(map[string]Value{"a": 1, "b": 2, "c": 3})

	oldTxn := db.Begin()
	writer := db.Begin()
	for _, k := range []string{"a", "b", "c"} {
		if err := writer.Put(k, 10); err != nil {
			t.Fatalf("writer Put(%s): %v", k, err)
		}
	}

	// 提交前：旧事务与其他新事务都看不到未提交的更新。
	mid := db.Begin()
	for k, want := range map[string]Value{"a": 1, "b": 2, "c": 3} {
		mustGet(t, oldTxn, k, want)
		mustGet(t, mid, k, want)
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("writer Commit: %v", err)
	}

	// 提交后：旧事务视图不变，新事务看到完整的新版本（不混杂）。
	for k, want := range map[string]Value{"a": 1, "b": 2, "c": 3} {
		mustGet(t, oldTxn, k, want)
		mustGet(t, mid, k, want)
	}
	newTxn := db.Begin()
	for _, k := range []string{"a", "b", "c"} {
		mustGet(t, newTxn, k, 10)
	}
}

// 事务读取自己改过的行必须得到自己的最新值。
func TestReadYourOwnWrites(t *testing.T) {
	db := NewDB(map[string]Value{"a": 1})
	txn := db.Begin()
	if err := txn.Put("a", 42); err != nil {
		t.Fatalf("Put: %v", err)
	}
	mustGet(t, txn, "a", 42)
	// 其他事务仍看到旧值。
	mustGet(t, db.Begin(), "a", 1)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	mustGet(t, db.Begin(), "a", 42)
}

// 持有者提交后，等待者整笔失败：冲突错误、此前更新全部撤销、行锁全部释放。
func TestHolderCommitsWaiterConflicts(t *testing.T) {
	db := NewDB(map[string]Value{"x": 1, "y": 1})
	holder := db.Begin()
	if err := holder.Put("x", 2); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	waiter := db.Begin()
	if err := waiter.Put("y", 2); err != nil { // 等待者先改另一行
		t.Fatalf("waiter Put(y): %v", err)
	}
	putDone := make(chan error, 1)
	go func() { putDone <- waiter.Put("x", 3) }()

	// 等待者应当正在阻塞，而不是立即失败。
	select {
	case err := <-putDone:
		t.Fatalf("waiter Put returned too early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := holder.Commit(); err != nil {
		t.Fatalf("holder Commit: %v", err)
	}
	if err := <-putDone; !errors.Is(err, ErrConflict) {
		t.Fatalf("waiter Put = %v, want ErrConflict", err)
	}

	// 整笔回滚：y 的更新不可见，且 x、y 的锁都已释放。
	check := db.Begin()
	mustGet(t, check, "x", 2)
	mustGet(t, check, "y", 1)
	if err := check.Put("y", 9); err != nil {
		t.Fatalf("y should be unlocked: %v", err)
	}
	if err := check.Put("x", 9); err != nil {
		t.Fatalf("x should be unlocked: %v", err)
	}
	if err := check.Commit(); err != nil {
		t.Fatalf("check Commit: %v", err)
	}
}

// 持有者回滚后，等待者应能继续更新并提交。
func TestHolderRollbackWaiterSucceeds(t *testing.T) {
	db := NewDB(map[string]Value{"x": 1})
	holder := db.Begin()
	if err := holder.Put("x", 2); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	waiter := db.Begin()
	putDone := make(chan error, 1)
	go func() { putDone <- waiter.Put("x", 3) }()
	time.Sleep(100 * time.Millisecond) // 确保等待者已进入等待

	holder.Rollback()
	if err := <-putDone; err != nil {
		t.Fatalf("waiter Put: %v", err)
	}
	if err := waiter.Commit(); err != nil {
		t.Fatalf("waiter Commit: %v", err)
	}
	mustGet(t, db.Begin(), "x", 3)
}

// 目标行被改回原值也算冲突：等待者仍须整笔失败。
func TestValueChangedBackStillConflicts(t *testing.T) {
	db := NewDB(map[string]Value{"x": 1})
	waiter := db.Begin()

	t1 := db.Begin()
	if err := t1.Put("x", 2); err != nil {
		t.Fatalf("t1 Put: %v", err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 Commit: %v", err)
	}
	t2 := db.Begin()
	if err := t2.Put("x", 1); err != nil { // 改回原值
		t.Fatalf("t2 Put: %v", err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("t2 Commit: %v", err)
	}

	if err := waiter.Put("x", 5); !errors.Is(err, ErrConflict) {
		t.Fatalf("waiter Put = %v, want ErrConflict", err)
	}
	// 事务已失败，不能再产生有效更新。
	if err := waiter.Put("x", 6); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("aborted waiter Put = %v, want ErrTxnAborted", err)
	}
	if err := waiter.Commit(); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("aborted waiter Commit = %v, want ErrTxnAborted", err)
	}
	mustGet(t, db.Begin(), "x", 1)
}

// 写等待超时：整笔撤销，释放全部行，后续事务可继续更新。
func TestWaitTimeoutAbortsWholeTxn(t *testing.T) {
	db := NewDB(map[string]Value{"x": 1, "y": 1}, WithWaitTimeout(200*time.Millisecond))
	holder := db.Begin()
	if err := holder.Put("x", 2); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	waiter := db.Begin()
	if err := waiter.Put("y", 2); err != nil {
		t.Fatalf("waiter Put(y): %v", err)
	}
	start := time.Now()
	if err := waiter.Put("x", 3); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("waiter Put(x) = %v, want ErrWaitTimeout", err)
	}
	if d := time.Since(start); d < 200*time.Millisecond || d > 2*time.Second {
		t.Fatalf("wait duration %v out of expected range", d)
	}

	// 无部分更新残留，行锁全部释放。
	holder.Rollback()
	check := db.Begin()
	mustGet(t, check, "x", 1)
	mustGet(t, check, "y", 1)
	if err := check.Put("x", 7); err != nil {
		t.Fatalf("x should be unlocked: %v", err)
	}
	if err := check.Put("y", 7); err != nil {
		t.Fatalf("y should be unlocked: %v", err)
	}
	if err := check.Commit(); err != nil {
		t.Fatalf("check Commit: %v", err)
	}
	mustGet(t, db.Begin(), "x", 7)
	mustGet(t, db.Begin(), "y", 7)
}

// 两个事务交叉占用行后双双超时：无部分更新残留，所有行可被新事务更新。
func TestCrossHoldTimeout(t *testing.T) {
	db := NewDB(map[string]Value{"a": 1, "b": 1}, WithWaitTimeout(200*time.Millisecond))
	t1 := db.Begin()
	t2 := db.Begin()
	if err := t1.Put("a", 10); err != nil {
		t.Fatalf("t1 Put(a): %v", err)
	}
	if err := t2.Put("b", 20); err != nil {
		t.Fatalf("t2 Put(b): %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = t1.Put("b", 11) }()
	go func() { defer wg.Done(); errs[1] = t2.Put("a", 21) }()
	wg.Wait()
	// 至少一方超时回滚并释放行；另一方可能随即成功（尚未提交）。
	if !errors.Is(errs[0], ErrWaitTimeout) && !errors.Is(errs[1], ErrWaitTimeout) {
		t.Fatalf("neither cross Put timed out: %v, %v", errs[0], errs[1])
	}

	// 超时方整笔回滚：无部分更新残留；收尾后所有行可继续更新。
	t1.Rollback()
	t2.Rollback()
	check := db.Begin()
	mustGet(t, check, "a", 1)
	mustGet(t, check, "b", 1)
	if err := check.Put("a", 100); err != nil {
		t.Fatalf("a should be unlocked: %v", err)
	}
	if err := check.Put("b", 100); err != nil {
		t.Fatalf("b should be unlocked: %v", err)
	}
	if err := check.Commit(); err != nil {
		t.Fatalf("check Commit: %v", err)
	}
	mustGet(t, db.Begin(), "a", 100)
	mustGet(t, db.Begin(), "b", 100)
}

// 主动回滚等待中的事务：回滚 500ms 内返回，等待调用此后 500ms 内结束。
func TestActiveRollbackOfWaitingTxn(t *testing.T) {
	db := NewDB(map[string]Value{"x": 1, "y": 1}) // 默认 5 秒等待上限
	holder := db.Begin()
	if err := holder.Put("x", 2); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	waiter := db.Begin()
	if err := waiter.Put("y", 2); err != nil {
		t.Fatalf("waiter Put(y): %v", err)
	}
	putDone := make(chan error, 1)
	go func() { putDone <- waiter.Put("x", 3) }()
	time.Sleep(100 * time.Millisecond) // 确保等待者已进入等待

	rbStart := time.Now()
	waiter.Rollback()
	if d := time.Since(rbStart); d > 500*time.Millisecond {
		t.Fatalf("Rollback took %v, want < 500ms", d)
	}
	waitStart := time.Now()
	if err := <-putDone; !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("waiter Put = %v, want ErrTxnAborted", err)
	}
	if d := time.Since(waitStart); d > 500*time.Millisecond {
		t.Fatalf("waiting Put took %v to finish after Rollback, want < 500ms", d)
	}

	// 无残留：y 的更新不可见，x 仍由持有者占有。
	mustGet(t, db.Begin(), "y", 1)
	holder.Rollback()
	check := db.Begin()
	mustGet(t, check, "x", 1)
	if err := check.Put("x", 9); err != nil {
		t.Fatalf("x should be unlocked: %v", err)
	}
	if err := check.Put("y", 9); err != nil {
		t.Fatalf("y should be unlocked: %v", err)
	}
	if err := check.Commit(); err != nil {
		t.Fatalf("check Commit: %v", err)
	}
}

// 写入持续等待期间，其他事务对不相关行的读写不受影响；读取被占用的行正常返回。
func TestWaitingWriterDoesNotBlockOthers(t *testing.T) {
	initial := map[string]Value{"locked": 0}
	for i := 0; i < 100; i++ {
		initial[fmt.Sprintf("row-%d", i)] = i
	}
	db := NewDB(initial) // 默认 5 秒等待上限

	holder := db.Begin()
	if err := holder.Put("locked", 1); err != nil {
		t.Fatalf("holder Put: %v", err)
	}
	waiter := db.Begin()
	putDone := make(chan error, 1)
	go func() { putDone <- waiter.Put("locked", 2) }()
	time.Sleep(100 * time.Millisecond) // 等待者进入 5 秒等待

	start := time.Now()
	worker := db.Begin()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("row-%d", i)
		mustGet(t, worker, key, i)
		if err := worker.Put(key, i+1000); err != nil {
			t.Fatalf("worker Put(%s): %v", key, err)
		}
	}
	// 读取被占用的行：返回本事务视图中的旧值。
	mustGet(t, worker, "locked", 0)
	if err := worker.Commit(); err != nil {
		t.Fatalf("worker Commit: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("100 reads/writes took %v, want < 1s", d)
	}

	// 收尾：释放持有者，等待者随后应冲突失败（持有者提交）或成功（回滚）。
	holder.Rollback()
	if err := <-putDone; err != nil {
		t.Fatalf("waiter Put after holder rollback: %v", err)
	}
	waiter.Rollback()
}

// 已结束的事务不能再次产生有效更新。
func TestFinishedTxnRejectsOps(t *testing.T) {
	db := NewDB(map[string]Value{"a": 1})
	txn := db.Begin()
	if err := txn.Put("a", 2); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := txn.Put("a", 3); !errors.Is(err, ErrTxnFinished) {
		t.Fatalf("Put after Commit = %v, want ErrTxnFinished", err)
	}
	if _, _, err := txn.Get("a"); !errors.Is(err, ErrTxnFinished) {
		t.Fatalf("Get after Commit = %v, want ErrTxnFinished", err)
	}
	if err := txn.Commit(); !errors.Is(err, ErrTxnFinished) {
		t.Fatalf("second Commit = %v, want ErrTxnFinished", err)
	}
	txn.Rollback() // 已提交后回滚应为无操作
	mustGet(t, db.Begin(), "a", 2)
}

// 规模冒烟：10 万行、64 个并发事务、每个事务覆盖 100 行。
func TestScale(t *testing.T) {
	const rows, txns, writesPerTxn = 100_000, 64, 100
	initial := make(map[string]Value, rows)
	for i := 0; i < rows; i++ {
		initial[fmt.Sprintf("r%d", i)] = i
	}
	db := NewDB(initial)

	var wg sync.WaitGroup
	errCh := make(chan error, txns)
	for w := 0; w < txns; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			txn := db.Begin()
			for j := 0; j < writesPerTxn; j++ {
				key := fmt.Sprintf("r%d", worker*writesPerTxn+j)
				if err := txn.Put(key, worker*1000+j); err != nil {
					errCh <- fmt.Errorf("worker %d Put: %w", worker, err)
					return
				}
			}
			if err := txn.Commit(); err != nil {
				errCh <- fmt.Errorf("worker %d Commit: %w", worker, err)
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	check := db.Begin()
	for w := 0; w < txns; w++ {
		for j := 0; j < writesPerTxn; j++ {
			mustGet(t, check, fmt.Sprintf("r%d", w*writesPerTxn+j), w*1000+j)
		}
	}
	mustGet(t, check, fmt.Sprintf("r%d", rows-1), rows-1) // 未触及的行保持原值
}
