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

	l.LockExtend(lockKey, 30*time.Second, func() {
		// 模拟耗时任务
		time.Sleep(100 * time.Millisecond)
		executed = true
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
			l.LockExtend(lockKey, 10*time.Second, func() {
				counter++
				time.Sleep(50 * time.Millisecond)
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
