package irontable

import "testing"

func mustGet(t *testing.T, tx *Txn, row string) any {
	t.Helper()
	v, ok, err := tx.Get(row)
	if err != nil {
		t.Fatalf("Get(%s) err: %v", row, err)
	}
	if !ok {
		t.Fatalf("Get(%s) not found", row)
	}
	return v
}

func seededDB(rows map[string]any) *DB {
	db := NewDB()
	db.Seed(rows)
	return db
}

// 旧事务在整个提交前后都看到自己开始时的完整视图；新事务看到提交后的完整视图。
func TestSnapshotIsolationAcrossCommit(t *testing.T) {
	db := seededDB(map[string]any{"a": 1, "b": 2, "c": 3})

	oldTx := db.Begin()
	writer := db.Begin()
	for _, row := range []string{"a", "b", "c"} {
		if err := writer.Put(row, 100); err != nil {
			t.Fatalf("writer Put(%s): %v", row, err)
		}
	}

	// 提交前：旧事务与另一个新事务都看到旧值。
	preCommit := db.Begin()
	for _, tx := range []*Txn{oldTx, preCommit} {
		for row, want := range map[string]any{"a": 1, "b": 2, "c": 3} {
			if got := mustGet(t, tx, row); got != want {
				t.Fatalf("pre-commit Get(%s) = %v, want %v", row, got, want)
			}
		}
	}
	if err := preCommit.Rollback(); err != nil {
		t.Fatalf("preCommit rollback: %v", err)
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("writer commit: %v", err)
	}

	// 提交后：旧事务仍逐行看到旧值。
	for row, want := range map[string]any{"a": 1, "b": 2, "c": 3} {
		if got := mustGet(t, oldTx, row); got != want {
			t.Fatalf("old tx Get(%s) = %v, want %v", row, got, want)
		}
	}
	// 新事务逐行看到全部新值，无新旧混杂。
	newTx := db.Begin()
	for _, row := range []string{"a", "b", "c"} {
		if got := mustGet(t, newTx, row); got != 100 {
			t.Fatalf("new tx Get(%s) = %v, want 100", row, got)
		}
	}
}

// 事务读取自己改过的行必须得到自己的最新值，其他事务看不到未提交值。
func TestReadYourOwnWrites(t *testing.T) {
	db := seededDB(map[string]any{"a": 1})
	tx := db.Begin()
	if err := tx.Put("a", 42); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Put("a", 43); err != nil {
		t.Fatalf("Put again: %v", err)
	}
	if got := mustGet(t, tx, "a"); got != 43 {
		t.Fatalf("own write Get = %v, want 43", got)
	}
	other := db.Begin()
	if got := mustGet(t, other, "a"); got != 1 {
		t.Fatalf("other tx Get = %v, want 1", got)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

// 回滚后更新全部撤销，已提交视图不变。
func TestRollbackDiscardsWrites(t *testing.T) {
	db := seededDB(map[string]any{"a": 1, "b": 2})
	tx := db.Begin()
	if err := tx.Put("a", 10); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := tx.Put("b", 20); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	check := db.Begin()
	for row, want := range map[string]any{"a": 1, "b": 2} {
		if got := mustGet(t, check, row); got != want {
			t.Fatalf("Get(%s) = %v, want %v", row, got, want)
		}
	}
}

// 已结束的事务不能再产生有效更新。
func TestFinishedTxnRejectsOps(t *testing.T) {
	db := seededDB(map[string]any{"a": 1})
	tx := db.Begin()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tx.Put("a", 9); err != ErrTxnDone {
		t.Fatalf("Put after commit = %v, want ErrTxnDone", err)
	}
	if _, _, err := tx.Get("a"); err != ErrTxnDone {
		t.Fatalf("Get after commit = %v, want ErrTxnDone", err)
	}
	if err := tx.Commit(); err != ErrTxnDone {
		t.Fatalf("second Commit = %v, want ErrTxnDone", err)
	}

	aborted := db.Begin()
	if err := aborted.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := aborted.Put("a", 9); err != ErrTxnDone {
		t.Fatalf("Put after rollback = %v, want ErrTxnDone", err)
	}
	if err := aborted.Commit(); err != ErrTxnDone {
		t.Fatalf("Commit after rollback = %v, want ErrTxnDone", err)
	}
}
