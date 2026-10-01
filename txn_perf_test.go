package irontable

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// 一个写入持续等待期间，其他事务对不相关行的100次读写应在1秒内完成；
// 读取被占用的行也应正常返回所属事务视图中的值。
func TestUnrelatedOpsFastDuringWait(t *testing.T) {
	db := seededDB(map[string]any{"hot": "v0"})

	holder := db.Begin()
	if err := holder.Put("hot", "v1"); err != nil {
		t.Fatalf("holder Put: %v", err)
	}
	waiter := db.Begin()
	putDone := make(chan error, 1)
	go func() { putDone <- waiter.Put("hot", "v2") }()
	time.Sleep(200 * time.Millisecond) // 确保 waiter 进入等待

	worker := db.Begin()
	start := time.Now()
	for i := 0; i < 100; i++ {
		row := fmt.Sprintf("cold-%d", i)
		if err := worker.Put(row, i); err != nil {
			t.Fatalf("worker Put(%s): %v", row, err)
		}
		if got := mustGet(t, worker, row); got != i {
			t.Fatalf("worker Get(%s) = %v, want %d", row, got, i)
		}
	}
	// 读取被占用的行：返回本事务视图中的旧值，不被等待影响。
	if got := mustGet(t, worker, "hot"); got != "v0" {
		t.Fatalf("worker Get(hot) = %v, want v0", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("100 unrelated ops + locked read took %v, want < 1s", elapsed)
	}
	if err := worker.Commit(); err != nil {
		t.Fatalf("worker commit: %v", err)
	}

	// 清理：撤销等待者与持有者。
	if err := waiter.Rollback(); err != nil {
		t.Fatalf("waiter rollback: %v", err)
	}
	select {
	case err := <-putDone:
		if err != ErrAborted {
			t.Fatalf("waiter Put = %v, want ErrAborted", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waiting Put did not finish within 500ms after rollback")
	}
	if err := holder.Rollback(); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}
}

// 规模与并发：10万行初始数据，64个并发事务各覆盖100行后提交。
func TestScaleAndConcurrency(t *testing.T) {
	const (
		rowCount   = 100000
		txnCount   = 64
		writesEach = 100
	)
	seed := make(map[string]any, rowCount)
	for i := 0; i < rowCount; i++ {
		seed[fmt.Sprintf("row-%d", i)] = i
	}
	db := seededDB(seed)

	var wg sync.WaitGroup
	errs := make(chan error, txnCount)
	for txIdx := 0; txIdx < txnCount; txIdx++ {
		wg.Add(1)
		go func(txIdx int) {
			defer wg.Done()
			tx := db.Begin()
			for w := 0; w < writesEach; w++ {
				row := fmt.Sprintf("row-%d", txIdx*writesEach+w)
				if err := tx.Put(row, -1-txIdx); err != nil {
					errs <- fmt.Errorf("tx %d Put(%s): %w", txIdx, row, err)
					return
				}
			}
			if err := tx.Commit(); err != nil {
				errs <- fmt.Errorf("tx %d commit: %w", txIdx, err)
			}
		}(txIdx)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	check := db.Begin()
	for txIdx := 0; txIdx < txnCount; txIdx++ {
		for w := 0; w < writesEach; w++ {
			row := fmt.Sprintf("row-%d", txIdx*writesEach+w)
			if got := mustGet(t, check, row); got != -1-txIdx {
				t.Fatalf("Get(%s) = %v, want %d", row, got, -1-txIdx)
			}
		}
	}
	// 未触及的行保持初始值。
	if got := mustGet(t, check, fmt.Sprintf("row-%d", rowCount-1)); got != rowCount-1 {
		t.Fatalf("untouched row = %v, want %d", got, rowCount-1)
	}
}
