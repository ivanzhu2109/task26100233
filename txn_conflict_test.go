package irontable

import (
	"testing"
	"time"
)

// 持有者提交后，等待者获得行时必须整笔失败并回滚，且不能覆盖新值。
func TestHolderCommitsWaiterConflicts(t *testing.T) {
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

	// 等 waiter 确实进入等待后再提交持有者。
	time.Sleep(200 * time.Millisecond)
	if err := holder.Commit(); err != nil {
		t.Fatalf("holder commit: %v", err)
	}

	select {
	case err := <-putDone:
		if err != ErrConflict {
			t.Fatalf("waiter Put = %v, want ErrConflict", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter Put did not return after holder commit")
	}

	// 整笔事务已回滚：other 行的更新也不可见。
	check := db.Begin()
	if got := mustGet(t, check, "row"); got != "v1" {
		t.Fatalf("row = %v, want v1", got)
	}
	if got := mustGet(t, check, "other"); got != "o0" {
		t.Fatalf("other = %v, want o0 (waiter must be fully aborted)", got)
	}
	if err := waiter.Commit(); err != ErrTxnDone {
		t.Fatalf("waiter commit after abort = %v, want ErrTxnDone", err)
	}

	// 行锁已释放：新事务可以立即更新两行。
	fresh := db.Begin()
	if err := fresh.Put("row", "v3"); err != nil {
		t.Fatalf("fresh Put row: %v", err)
	}
	if err := fresh.Put("other", "o3"); err != nil {
		t.Fatalf("fresh Put other: %v", err)
	}
	if err := fresh.Commit(); err != nil {
		t.Fatalf("fresh commit: %v", err)
	}
}

// 持有者回滚且无冲突时，等待者应继续更新并成功提交。
func TestHolderRollsBackWaiterSucceeds(t *testing.T) {
	db := seededDB(map[string]any{"row": "v0"})

	holder := db.Begin()
	if err := holder.Put("row", "v1"); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	waiter := db.Begin()
	putDone := make(chan error, 1)
	go func() { putDone <- waiter.Put("row", "v2") }()

	time.Sleep(200 * time.Millisecond)
	if err := holder.Rollback(); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}

	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("waiter Put = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter Put did not return after holder rollback")
	}
	if err := waiter.Commit(); err != nil {
		t.Fatalf("waiter commit: %v", err)
	}

	check := db.Begin()
	if got := mustGet(t, check, "row"); got != "v2" {
		t.Fatalf("row = %v, want v2", got)
	}
}

// 目标行在本事务开始后被提交过更新，即使后来改回原值，也仍算冲突。
func TestConflictEvenIfValueRestored(t *testing.T) {
	db := seededDB(map[string]any{"row": "orig"})

	old := db.Begin()

	w1 := db.Begin()
	if err := w1.Put("row", "changed"); err != nil {
		t.Fatalf("w1 Put: %v", err)
	}
	if err := w1.Commit(); err != nil {
		t.Fatalf("w1 commit: %v", err)
	}

	w2 := db.Begin()
	if err := w2.Put("row", "orig"); err != nil {
		t.Fatalf("w2 Put: %v", err)
	}
	if err := w2.Commit(); err != nil {
		t.Fatalf("w2 commit: %v", err)
	}

	// 值已改回 orig，但行在 old 开始后被提交过，必须冲突并整笔回滚。
	if err := old.Put("row", "mine"); err != ErrConflict {
		t.Fatalf("old Put = %v, want ErrConflict", err)
	}
	if err := old.Commit(); err != ErrTxnDone {
		t.Fatalf("old commit after conflict = %v, want ErrTxnDone", err)
	}
	check := db.Begin()
	if got := mustGet(t, check, "row"); got != "orig" {
		t.Fatalf("row = %v, want orig", got)
	}
}

// 读取被占用的行不受等待影响，返回本事务视图中的值。
func TestReadLockedRowUnblocked(t *testing.T) {
	db := seededDB(map[string]any{"row": "v0"})

	holder := db.Begin()
	if err := holder.Put("row", "v1"); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	reader := db.Begin()
	start := time.Now()
	if got := mustGet(t, reader, "row"); got != "v0" {
		t.Fatalf("reader Get = %v, want v0", got)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("read of locked row took %v, want < 500ms", elapsed)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}
}
