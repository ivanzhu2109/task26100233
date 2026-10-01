package irontable

import (
	"testing"
	"time"
)

// 两个事务交叉占用行后双双等待超时：两笔都必须整体撤销，
// 没有部分更新残留，释放的行可被新事务继续更新。
func TestCrossLockTimeout(t *testing.T) {
	db := seededDB(map[string]any{"a": "a0", "b": "b0"})

	t1 := db.Begin()
	t2 := db.Begin()
	if err := t1.Put("a", "a1"); err != nil {
		t.Fatalf("t1 Put a: %v", err)
	}
	if err := t2.Put("b", "b1"); err != nil {
		t.Fatalf("t2 Put b: %v", err)
	}

	start := time.Now()
	err1 := make(chan error, 1)
	err2 := make(chan error, 1)
	go func() { err1 <- t1.Put("b", "t1-b") }()
	go func() { err2 <- t2.Put("a", "t2-a") }()

	got1, got2 := <-err1, <-err2
	elapsed := time.Since(start)
	if elapsed > 8*time.Second {
		t.Fatalf("cross-lock resolution took %v, want about 5s", elapsed)
	}

	// 至少一方超时撤销；另一方可能因对方撤销而继续成功。
	timedOut := map[*Txn]bool{t1: got1 == ErrWaitTimeout, t2: got2 == ErrWaitTimeout}
	if !timedOut[t1] && !timedOut[t2] {
		t.Fatalf("cross-lock puts = %v, %v, want at least one ErrWaitTimeout", got1, got2)
	}
	for tx, err := range map[*Txn]error{t1: got1, t2: got2} {
		if err != nil && err != ErrWaitTimeout {
			t.Fatalf("unexpected put error: %v", err)
		}
		if timedOut[tx] {
			if err := tx.Commit(); err != ErrTxnDone {
				t.Fatalf("timed-out tx commit = %v, want ErrTxnDone", err)
			}
		} else if err := tx.Commit(); err != nil {
			t.Fatalf("surviving tx commit: %v", err)
		}
	}

	// 无部分更新残留：被撤销方的任何更新都不可见。
	check := db.Begin()
	wantA, wantB := "a0", "b0"
	if !timedOut[t1] {
		wantA, wantB = "a1", "t1-b"
	}
	if !timedOut[t2] {
		wantB = "b1"
		if !timedOut[t1] {
			t.Fatal("both txs survived cross-lock, impossible")
		}
		// t2 存活的唯一可能是 t1 先撤销，此时 t2 对 a 的写入生效。
		wantA = "t2-a"
	}
	if got := mustGet(t, check, "a"); got != wantA {
		t.Fatalf("a = %v, want %v", got, wantA)
	}
	if got := mustGet(t, check, "b"); got != wantB {
		t.Fatalf("b = %v, want %v", got, wantB)
	}

	// 所有行锁已释放，新事务可继续更新。
	fresh := db.Begin()
	if err := fresh.Put("a", "a9"); err != nil {
		t.Fatalf("fresh Put a: %v", err)
	}
	if err := fresh.Put("b", "b9"); err != nil {
		t.Fatalf("fresh Put b: %v", err)
	}
	if err := fresh.Commit(); err != nil {
		t.Fatalf("fresh commit: %v", err)
	}
}

// 交叉占用时主动回滚其中一笔：被回滚方整体撤销，
// 另一笔的等待调用解除并继续完成提交。
func TestCrossLockActiveRollback(t *testing.T) {
	db := seededDB(map[string]any{"a": "a0", "b": "b0"})

	t1 := db.Begin()
	t2 := db.Begin()
	if err := t1.Put("a", "a1"); err != nil {
		t.Fatalf("t1 Put a: %v", err)
	}
	if err := t2.Put("b", "b1"); err != nil {
		t.Fatalf("t2 Put b: %v", err)
	}

	putDone := make(chan error, 1)
	go func() { putDone <- t1.Put("b", "t1-b") }()
	time.Sleep(200 * time.Millisecond) // 确保 t1 进入等待

	// 主动回滚 t2：t1 的等待应立即解除并拿到 b。
	if err := t2.Rollback(); err != nil {
		t.Fatalf("t2 rollback: %v", err)
	}
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("t1 Put b = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("t1 Put b did not return after t2 rollback")
	}
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}

	// t2 整体撤销：b0 被 t1 覆盖为 t1-b，t2 对 b 的更新不可见。
	check := db.Begin()
	if got := mustGet(t, check, "a"); got != "a1" {
		t.Fatalf("a = %v, want a1", got)
	}
	if got := mustGet(t, check, "b"); got != "t1-b" {
		t.Fatalf("b = %v, want t1-b", got)
	}
	if err := t2.Put("a", "x"); err != ErrTxnDone {
		t.Fatalf("t2 Put after rollback = %v, want ErrTxnDone", err)
	}
}

// 主动回滚正在等待写入的事务：Rollback 500ms 内返回，
// 等待调用在此后 500ms 内结束，且事务不遗留有效更改。
func TestRollbackWaitingTxn(t *testing.T) {
	db := seededDB(map[string]any{"row": "v0", "other": "o0"})

	holder := db.Begin()
	if err := holder.Put("row", "v1"); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	waiter := db.Begin()
	if err := waiter.Put("other", "o1"); err != nil {
		t.Fatalf("waiter Put other: %v", err)
	}
	putDone := make(chan error, 1)
	go func() { putDone <- waiter.Put("row", "v2") }()
	time.Sleep(200 * time.Millisecond) // 确保 waiter 进入等待

	rbStart := time.Now()
	if err := waiter.Rollback(); err != nil {
		t.Fatalf("waiter rollback: %v", err)
	}
	if elapsed := time.Since(rbStart); elapsed > 500*time.Millisecond {
		t.Fatalf("Rollback took %v, want < 500ms", elapsed)
	}

	waitStart := time.Now()
	select {
	case err := <-putDone:
		if err != ErrAborted {
			t.Fatalf("waiter Put = %v, want ErrAborted", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waiting Put did not finish within 500ms after rollback")
	}
	_ = waitStart

	// 等待条件解除后事务不能重新生效：持有者提交后，waiter 的更改仍不可见。
	if err := holder.Commit(); err != nil {
		t.Fatalf("holder commit: %v", err)
	}
	check := db.Begin()
	if got := mustGet(t, check, "row"); got != "v1" {
		t.Fatalf("row = %v, want v1", got)
	}
	if got := mustGet(t, check, "other"); got != "o0" {
		t.Fatalf("other = %v, want o0 (waiter writes must be discarded)", got)
	}

	// 行锁全部释放，新事务可更新。
	fresh := db.Begin()
	if err := fresh.Put("row", "v9"); err != nil {
		t.Fatalf("fresh Put row: %v", err)
	}
	if err := fresh.Put("other", "o9"); err != nil {
		t.Fatalf("fresh Put other: %v", err)
	}
	if err := fresh.Commit(); err != nil {
		t.Fatalf("fresh commit: %v", err)
	}
}
