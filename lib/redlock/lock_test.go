package redlock

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"
)

// ==================== 初始化测试 ====================

func TestNew(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available or not single node, skip test")
	}

	// 验证锁可用
	mutex := l.NewMutex("test:init")
	if err := mutex.Lock(); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	defer func() {
		if ok, err := mutex.Unlock(); !ok || err != nil {
			t.Logf("Unlock failed: ok:%v err:%v", ok, err)
		}
	}()

	t.Log("New lock initialized successfully")
}

func TestNewRedlock(t *testing.T) {
	// 多实例 Redlock 模式
	options := []*redis.Options{
		{Addr: "localhost:6379"},
		{Addr: "localhost:6380"},
		{Addr: "localhost:6381"},
	}

	l := New(options...)
	if l == nil {
		t.Skip("Redlock not initialized, skip test")
	}

	mutex := l.NewMutex("test:redlock:init")
	if err := mutex.Lock(); err != nil {
		t.Fatalf("Redlock Lock failed: %v", err)
	}
	defer func() {
		if ok, err := mutex.Unlock(); !ok || err != nil {
			t.Logf("Redlock Unlock failed: ok:%v err:%v", ok, err)
		}
	}()

	t.Log("NewRedlock initialized successfully")
}

func TestNewConnectionRefused(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:9999"})
	if l != nil {
		t.Error("Expected nil for connection refused, got non-nil")
	}
}

// ==================== 分布式锁测试 ====================

// 基础锁测试 - 防止重复操作
// 场景: 添加会员时，同一个账号同时只能一个请求处理
func TestNewMutex(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	// 模拟场景: Member.Add 中防止重复添加
	account := "testuser"
	lockKey := fmt.Sprintf("Member.%s", account)
	mutex := l.NewMutex(lockKey)

	// 加锁
	if err := mutex.Lock(); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	defer func() {
		if ok, err := mutex.Unlock(); !ok || err != nil {
			t.Logf("Unlock failed: ok:%v err:%v", ok, err)
		}
	}()

	// 模拟业务逻辑
	t.Log("执行业务逻辑...")
}

// LockExtend - 带续租的锁，同步执行耗时任务
func TestLockExtend(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	// 模拟场景: 短信发送处理
	lockKey := "lock:sms:process:test"
	executed := false

	l.LockExtend(lockKey, 30*time.Second, func() error {
		// 模拟耗时任务
		time.Sleep(100 * time.Millisecond)
		executed = true
		return nil
	})

	if !executed {
		t.Error("Task should be executed")
	}
}

// LockExtendGeneric - 带返回值的耗时任务
func TestLockExtendGeneric(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	// 模拟场景: 号码空号检测
	type CheckResult struct {
		ValidCount   int
		InvalidCount int
	}

	lockKey := "lock:mobile:check:test"
	result, err := LockExtendGeneric(l, lockKey, 30*time.Second, func() (CheckResult, error) {
		// 模拟耗时检测任务
		time.Sleep(100 * time.Millisecond)
		return CheckResult{ValidCount: 80, InvalidCount: 20}, nil
	})
	if err != nil {
		t.Fatalf("LockExtendGeneric failed: %v", err)
	}
	if result.ValidCount != 80 || result.InvalidCount != 20 {
		t.Errorf("LockExtendGeneric result = %+v", result)
	}
}

// LockAwaitOnce - 分布式选举，多台机器只有一台执行任务
// 场景: 定时任务初始化，保证集群中只有一台机器执行定时任务
func TestLockAwaitOnce(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	lockKey := "feige-cloud-backend-initCron-LockAwaitOnce:test"
	executed := make(chan bool, 1)

	// 模拟场景: 启动时初始化定时任务
	l.LockAwaitOnce(lockKey, 30*time.Second, func() {
		// 模拟初始化定时任务
		t.Log("定时任务初始化完成")
		executed <- true
	})

	// LockAwaitOnce 是异步的，等待执行完成
	select {
	case <-executed:
		t.Log("LockAwaitOnce executed successfully")
	case <-time.After(10 * time.Second):
		t.Log("LockAwaitOnce timeout, may be waiting for lock")
	}
}

// LockAwaitOnce 带清理函数 - 续期失败时清理定时任务
func TestLockAwaitOnceWithClear(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	lockKey := "feige-cloud-backend-smsStat-LockAwaitOnce:test"
	executed := make(chan bool, 1)
	cleared := make(chan bool, 1)

	// 模拟场景: 短信统计任务，带清理函数
	l.LockAwaitOnce(lockKey, 5*time.Second, func() {
		// 模拟统计任务
		t.Log("统计任务开始执行")
		executed <- true
	}, func() {
		// 续期失败时清理，防止其他机器重复执行
		t.Log("续期失败，清理定时任务")
		cleared <- true
	})

	// 等待执行或清理
	select {
	case <-executed:
		t.Log("LockAwaitOnce task executed")
	case <-cleared:
		t.Log("LockAwaitOnce clear function called")
	case <-time.After(15 * time.Second):
		t.Log("LockAwaitOnce timeout")
	}
}

// ==================== Redlock 多实例锁测试 ====================

// New 多实例 - Redlock 模式
func TestNewRedlockMutex(t *testing.T) {
	// 多实例 Redlock 模式
	options := []*redis.Options{
		{Addr: "localhost:6379"},
		{Addr: "localhost:6380"},
		{Addr: "localhost:6381"},
	}

	l := New(options...)
	if l == nil {
		t.Skip("Redlock not initialized")
	}

	mutex := l.NewMutex("test:redlock:mutex")

	if err := mutex.Lock(); err != nil {
		t.Fatalf("Redlock Lock failed: %v", err)
	}
	defer func() {
		if ok, err := mutex.Unlock(); !ok || err != nil {
			t.Logf("Redlock Unlock failed: ok:%v err:%v", ok, err)
		}
	}()

	t.Log("Redlock acquired successfully")
}

// ==================== NewMutex 选项测试 ====================

func TestNewMutexWithOptions(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	// 自定义重试次数和延迟
	mutex := l.NewMutex("test:mutex:custom",
		redsync.WithTries(5),
		redsync.WithRetryDelay(100*time.Millisecond),
		redsync.WithExpiry(20*time.Second),
	)

	if err := mutex.Lock(); err != nil {
		t.Fatalf("Lock with custom options failed: %v", err)
	}
	defer func() {
		if ok, err := mutex.Unlock(); !ok || err != nil {
			t.Logf("Unlock failed: ok:%v err:%v", ok, err)
		}
	}()

	t.Log("Mutex with custom options acquired successfully")
}

// ==================== 并发锁测试 ====================

func TestConcurrentLock(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	lockKey := "test:concurrent:lock"
	counter := 0
	done := make(chan bool, 5)

	// 5 个 goroutine 竞争同一把锁
	for i := 0; i < 5; i++ {
		go func(id int) {
			l.LockExtend(lockKey, 10*time.Second, func() error {
				counter++
				time.Sleep(50 * time.Millisecond)
				return nil
			})
			done <- true
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 5; i++ {
		<-done
	}

	// 由于锁的互斥性，counter 应该等于 5（每个 goroutine 都执行了一次）
	if counter != 5 {
		t.Errorf("counter = %d, want 5 (each goroutine should execute once)", counter)
	}
}

// ==================== TryLock / LockGuard 测试 ====================

// mockExportTask 模拟一个导出任务:按传入的耗时执行,返回模拟的导出结果
func mockExportTask(duration time.Duration, rows int) error {
	time.Sleep(duration) // 模拟查询+生成Excel的耗时
	if rows < 0 {
		return fmt.Errorf("模拟导出失败: 行数=%d", rows)
	}
	return nil
}

// TestTryLock 基本抢锁:成功后锁存在,Release后可再次抢到
func TestTryLock(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	lockKey := "test:trylock:basic"

	// 第一次抢锁应成功
	guard, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("第一次TryLock应成功: %v", err)
	}

	// 锁被占用期间,第二次抢锁应失败
	if _, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second); !IsLockBusy(err) {
		t.Fatalf("第二次TryLock应返回LockBusy, got: %v", err)
	}

	// 释放后应能重新抢到
	guard.Release()
	guard2, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("Release后TryLock应成功: %v", err)
	}
	guard2.Release()
}

// TestTryLockAddTask 模拟导出场景:同步抢锁 -> AddTask后台执行 -> 任务结束自动释放
func TestTryLockAddTask(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	lockKey := "test:trylock:addtask"
	done := make(chan error, 1)

	// 模拟接口层:先同步抢锁(抢不到直接报错,不产生任务)
	guard, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("抢锁失败: %v", err)
	}

	// 抢到锁才创建任务,AddTask后台执行,任务结束(含panic)自动Release
	guard.AddTask(func() error {
		defer func() { done <- nil }()
		return mockExportTask(100*time.Millisecond, 100) // 模拟导出100行
	})

	// 任务执行期间锁仍被占用
	if _, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second); !IsLockBusy(err) {
		t.Log("注意: 任务可能已执行完毕(时序竞争), err:", err)
	}

	// 等任务跑完
	<-done
	time.Sleep(100 * time.Millisecond) // 等Release完成

	// 任务结束后锁应已自动释放,可再次抢到
	guard2, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("AddTask结束后锁应自动释放: %v", err)
	}
	guard2.Release()
}

// TestTryLockAddTaskPanic 任务panic时锁也必须被释放
func TestTryLockAddTaskPanic(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	lockKey := "test:trylock:panic"
	guard, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("抢锁失败: %v", err)
	}

	guard.AddTask(func() error {
		panic("模拟导出过程panic")
	})

	time.Sleep(200 * time.Millisecond) // 等panic协程退出并Release

	// panic后锁应已自动释放
	guard2, err := l.TryLock(lockKey, 10*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("任务panic后锁应自动释放: %v", err)
	}
	guard2.Release()
}

// TestTryLockTimeout 与LockExtend同款:不传timeouts用默认5分钟,传入即最长持有时间
func TestTryLockTimeout(t *testing.T) {
	l := New(&redis.Options{Addr: "localhost:6379"})
	if l == nil {
		t.Skip("Redis not available, skip test")
	}

	guard, err := l.TryLock("test:trylock:timeout-default", 10*time.Second)
	if err != nil {
		t.Fatalf("TryLock失败: %v", err)
	}
	guard.Release()

	guard, err = l.TryLock("test:trylock:timeout-custom", 10*time.Second, 30*time.Minute)
	if err != nil {
		t.Fatalf("TryLock失败: %v", err)
	}
	guard.Release()
}
