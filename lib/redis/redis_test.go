package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	cache "github.com/mgtv-tech/jetcache-go"
	"github.com/redis/go-redis/v9"
)

// ==================== 初始化测试 ====================

func TestNew(t *testing.T) {
	options := &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	}

	client, err := New(options)
	if err != nil {
		t.Skipf("Redis not available, skip test: %v", err)
	}
	defer client.Rdb().Close()

	if client.Rdb() == nil {
		t.Error("Rdb should not be nil")
	}
	if client.GetCache() != nil {
		t.Error("Cache should be nil before WithCache()")
	}
	if client.GetRs() != nil {
		t.Error("Rs should be nil before WithLock()")
	}
}

func TestNewFailover(t *testing.T) {
	options := &redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{":26379", ":26380", ":26381"},
	}

	client, err := NewFailover(options)
	if err != nil {
		t.Skipf("Redis Sentinel not available, skip test: %v", err)
	}
	defer client.Rdb().Close()

	if client.Rdb() == nil {
		t.Error("Rdb should not be nil")
	}
}

func TestNewFailoverCluster(t *testing.T) {
	options := &redis.FailoverOptions{
		MasterName:     "mymaster",
		SentinelAddrs:  []string{":26379", ":26380", ":26381"},
		RouteByLatency: true,
	}

	client, err := NewFailoverCluster(options)
	if err != nil {
		t.Skipf("Redis Sentinel not available, skip test: %v", err)
	}
	defer client.Rdb().Close()

	if client.Rdb() == nil {
		t.Error("Rdb should not be nil")
	}
}

func TestNewCluster(t *testing.T) {
	options := &redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002"},
	}

	client, err := NewCluster(options)
	if err != nil {
		t.Skipf("Redis Cluster not available, skip test: %v", err)
	}
	defer client.Rdb().Close()

	if client.Rdb() == nil {
		t.Error("Rdb should not be nil")
	}
}

func TestNewRing(t *testing.T) {
	options := &redis.RingOptions{
		Addrs: map[string]string{
			"shard1": ":7000",
			"shard2": ":7001",
			"shard3": ":7002",
		},
	}

	client, err := NewRing(options)
	if err != nil {
		t.Skipf("Redis Ring not available, skip test: %v", err)
	}
	defer client.Rdb().Close()

	if client.Rdb() == nil {
		t.Error("Rdb should not be nil")
	}
}

func TestNewUniversal(t *testing.T) {
	options := &redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	}

	client, err := NewUniversal(options)
	if err != nil {
		t.Skipf("Redis not available, skip test: %v", err)
	}
	defer client.Rdb().Close()

	if client.Rdb() == nil {
		t.Error("Rdb should not be nil")
	}
}

func TestNewWithOptions(t *testing.T) {
	options := &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	}

	client, err := New(options, WithCache(), WithLock())
	if err != nil {
		t.Skipf("Redis not available, skip test: %v", err)
	}
	defer client.Rdb().Close()

	if client.GetCache() == nil {
		t.Error("Cache should not be nil when WithCache option is used")
	}
	if client.GetRs() == nil {
		t.Error("Rs should not be nil when WithLock option is used")
	}
}

// ==================== 常用命令测试 ====================

func TestStringCommands(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// Set
	err = client.Rdb().Set(ctx, "test:key", "hello", 10*time.Second).Err()
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get
	val, err := client.Rdb().Get(ctx, "test:key").Result()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "hello" {
		t.Errorf("Get = %q, want %q", val, "hello")
	}

	// SetNX (不存在才设置)
	ok, err := client.Rdb().SetNX(ctx, "test:key", "world", 10*time.Second).Result()
	if err != nil {
		t.Fatalf("SetNX failed: %v", err)
	}
	if ok {
		t.Error("SetNX should return false for existing key")
	}

	// MSet / MGet
	err = client.Rdb().MSet(ctx, "test:m1", "a", "test:m2", "b").Err()
	if err != nil {
		t.Fatalf("MSet failed: %v", err)
	}
	vals, err := client.Rdb().MGet(ctx, "test:m1", "test:m2").Result()
	if err != nil {
		t.Fatalf("MGet failed: %v", err)
	}
	if vals[0] != "a" || vals[1] != "b" {
		t.Errorf("MGet = %v, want [a b]", vals)
	}

	// Incr / Decr
	client.Rdb().Set(ctx, "test:counter", "10", 0)
	newVal, err := client.Rdb().Incr(ctx, "test:counter").Result()
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if newVal != 11 {
		t.Errorf("Incr = %d, want 11", newVal)
	}

	// 清理
	client.Rdb().Del(ctx, "test:key", "test:m1", "test:m2", "test:counter")
}

func TestHashCommands(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// HSet
	err = client.Rdb().HSet(ctx, "test:hash", "field1", "value1", "field2", "value2").Err()
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}

	// HGet
	val, err := client.Rdb().HGet(ctx, "test:hash", "field1").Result()
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("HGet = %q, want %q", val, "value1")
	}

	// HGetAll
	all, err := client.Rdb().HGetAll(ctx, "test:hash").Result()
	if err != nil {
		t.Fatalf("HGetAll failed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("HGetAll returned %d fields, want 2", len(all))
	}

	// HMGet
	vals, err := client.Rdb().HMGet(ctx, "test:hash", "field1", "field2").Result()
	if err != nil {
		t.Fatalf("HMGet failed: %v", err)
	}
	if vals[0] != "value1" || vals[1] != "value2" {
		t.Errorf("HMGet = %v, want [value1 value2]", vals)
	}

	// HIncrBy
	client.Rdb().HSet(ctx, "test:hash", "counter", "10")
	newVal, err := client.Rdb().HIncrBy(ctx, "test:hash", "counter", 5).Result()
	if err != nil {
		t.Fatalf("HIncrBy failed: %v", err)
	}
	if newVal != 15 {
		t.Errorf("HIncrBy = %d, want 15", newVal)
	}

	// 清理
	client.Rdb().Del(ctx, "test:hash")
}

// ==================== List 测试 ====================

func TestListCommands(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// RPush (右边插入)
	err = client.Rdb().RPush(ctx, "test:list", "a", "b", "c").Err()
	if err != nil {
		t.Fatalf("RPush failed: %v", err)
	}

	// LPush (左边插入)
	err = client.Rdb().LPush(ctx, "test:list", "x").Err()
	if err != nil {
		t.Fatalf("LPush failed: %v", err)
	}

	// LLen
	length, err := client.Rdb().LLen(ctx, "test:list").Result()
	if err != nil {
		t.Fatalf("LLen failed: %v", err)
	}
	if length != 4 {
		t.Errorf("LLen = %d, want 4", length)
	}

	// LRange
	vals, err := client.Rdb().LRange(ctx, "test:list", 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange failed: %v", err)
	}
	if len(vals) != 4 || vals[0] != "x" || vals[1] != "a" {
		t.Errorf("LRange = %v, want [x a b c]", vals)
	}

	// LPop
	val, err := client.Rdb().LPop(ctx, "test:list").Result()
	if err != nil {
		t.Fatalf("LPop failed: %v", err)
	}
	if val != "x" {
		t.Errorf("LPop = %q, want %q", val, "x")
	}

	// RPop
	val, err = client.Rdb().RPop(ctx, "test:list").Result()
	if err != nil {
		t.Fatalf("RPop failed: %v", err)
	}
	if val != "c" {
		t.Errorf("RPop = %q, want %q", val, "c")
	}

	// 清理
	client.Rdb().Del(ctx, "test:list")
}

// ==================== Set 测试 ====================

func TestSetCommands(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// SAdd
	err = client.Rdb().SAdd(ctx, "test:set", "a", "b", "c").Err()
	if err != nil {
		t.Fatalf("SAdd failed: %v", err)
	}

	// SIsMember
	ok, err := client.Rdb().SIsMember(ctx, "test:set", "a").Result()
	if err != nil {
		t.Fatalf("SIsMember failed: %v", err)
	}
	if !ok {
		t.Error("SIsMember should return true for existing member")
	}

	// SMembers
	members, err := client.Rdb().SMembers(ctx, "test:set").Result()
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}
	if len(members) != 3 {
		t.Errorf("SMembers returned %d members, want 3", len(members))
	}

	// SCard
	count, err := client.Rdb().SCard(ctx, "test:set").Result()
	if err != nil {
		t.Fatalf("SCard failed: %v", err)
	}
	if count != 3 {
		t.Errorf("SCard = %d, want 3", count)
	}

	// SRem
	err = client.Rdb().SRem(ctx, "test:set", "a").Err()
	if err != nil {
		t.Fatalf("SRem failed: %v", err)
	}
	ok, _ = client.Rdb().SIsMember(ctx, "test:set", "a").Result()
	if ok {
		t.Error("SIsMember should return false after SRem")
	}

	// 清理
	client.Rdb().Del(ctx, "test:set")
}

// ==================== ZSet 测试 ====================

func TestZSetCommands(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// ZAdd
	err = client.Rdb().ZAdd(ctx, "test:zset", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()
	if err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}

	// ZScore
	score, err := client.Rdb().ZScore(ctx, "test:zset", "b").Result()
	if err != nil {
		t.Fatalf("ZScore failed: %v", err)
	}
	if score != 2 {
		t.Errorf("ZScore = %f, want 2", score)
	}

	// ZRange
	members, err := client.Rdb().ZRange(ctx, "test:zset", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRange failed: %v", err)
	}
	if len(members) != 3 {
		t.Errorf("ZRange returned %d members, want 3", len(members))
	}

	// ZRangeWithScores
	zRangeVals, err := client.Rdb().ZRangeWithScores(ctx, "test:zset", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRangeWithScores failed: %v", err)
	}
	if len(zRangeVals) != 3 {
		t.Errorf("ZRangeWithScores returned %d members, want 3", len(zRangeVals))
	}

	// ZRank
	rank, err := client.Rdb().ZRank(ctx, "test:zset", "c").Result()
	if err != nil {
		t.Fatalf("ZRank failed: %v", err)
	}
	if rank != 2 {
		t.Errorf("ZRank = %d, want 2", rank)
	}

	// 清理
	client.Rdb().Del(ctx, "test:zset")
}

// ==================== Key 操作测试 ====================

func TestKeyCommands(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// Set
	client.Rdb().Set(ctx, "test:ttl", "value", 10*time.Second)

	// Exists
	exists, err := client.Rdb().Exists(ctx, "test:ttl").Result()
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists != 1 {
		t.Errorf("Exists = %d, want 1", exists)
	}

	// TTL
	ttl, err := client.Rdb().TTL(ctx, "test:ttl").Result()
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	if ttl <= 0 || ttl > 10*time.Second {
		t.Errorf("TTL = %v, want between 0 and 10s", ttl)
	}

	// Type
	keyType, err := client.Rdb().Type(ctx, "test:ttl").Result()
	if err != nil {
		t.Fatalf("Type failed: %v", err)
	}
	if keyType != "string" {
		t.Errorf("Type = %q, want %q", keyType, "string")
	}

	// Rename
	client.Rdb().Set(ctx, "test:old", "value", 0)
	err = client.Rdb().Rename(ctx, "test:old", "test:new").Err()
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	val, _ := client.Rdb().Get(ctx, "test:new").Result()
	if val != "value" {
		t.Errorf("Get after Rename = %q, want %q", val, "value")
	}

	// Del
	client.Rdb().Del(ctx, "test:ttl", "test:new")
	exists, _ = client.Rdb().Exists(ctx, "test:ttl").Result()
	if exists != 0 {
		t.Error("Key should not exist after Del")
	}
}

// ==================== 发布订阅测试 ====================

func TestPubSub(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// 订阅
	pubsub := client.Rdb().Subscribe(ctx, "test:channel")
	defer pubsub.Close()

	// 等待订阅生效
	_, err = pubsub.Receive(ctx)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// 发布消息
	err = client.Rdb().Publish(ctx, "test:channel", "hello").Err()
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// 接收消息 (带超时)
	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage failed: %v", err)
	}
	if msg.Payload != "hello" {
		t.Errorf("Payload = %q, want %q", msg.Payload, "hello")
	}
	if msg.Channel != "test:channel" {
		t.Errorf("Channel = %q, want %q", msg.Channel, "test:channel")
	}
}

func TestPubSubPattern(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// 模式订阅
	pubsub := client.Rdb().PSubscribe(ctx, "test:*")
	defer pubsub.Close()

	_, err = pubsub.Receive(ctx)
	if err != nil {
		t.Fatalf("PSubscribe failed: %v", err)
	}

	// 发布到不同频道
	client.Rdb().Publish(ctx, "test:abc", "msg1")

	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage failed: %v", err)
	}
	if msg.Payload != "msg1" {
		t.Errorf("Payload = %q, want %q", msg.Payload, "msg1")
	}
}

// ==================== Pipeline 测试 ====================

func TestPipeline(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// Pipeline 批量执行
	pipe := client.Rdb().Pipeline()
	pipe.Set(ctx, "test:p1", "a", 0)
	pipe.Set(ctx, "test:p2", "b", 0)
	pipe.Set(ctx, "test:p3", "c", 0)
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Pipeline Exec failed: %v", err)
	}
	if len(cmds) != 3 {
		t.Errorf("Pipeline returned %d commands, want 3", len(cmds))
	}

	// 验证
	val, _ := client.Rdb().Get(ctx, "test:p1").Result()
	if val != "a" {
		t.Errorf("Get p1 = %q, want %q", val, "a")
	}

	// 清理
	client.Rdb().Del(ctx, "test:p1", "test:p2", "test:p3")
}

// ==================== 分布式锁测试 ====================

// 基础锁测试 - 防止重复操作
// 场景: 添加会员时，同一个账号同时只能一个请求处理
func TestNewMutex(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithLock())
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()

	// 模拟场景: Member.Add 中防止重复添加
	account := "testuser"
	lockKey := fmt.Sprintf("Member.%s", account)
	mutex := client.NewMutex(lockKey)

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
	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithLock())
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()

	// 模拟场景: 短信发送处理
	lockKey := "lock:sms:process"
	executed := false

	client.LockExtend(lockKey, 30*time.Second, func() {
		// 模拟耗时任务
		time.Sleep(100 * time.Second)
		executed = true
	})

	if !executed {
		t.Error("Task should be executed")
	}
}

// LockExtendGeneric - 带返回值的耗时任务
func TestLockExtendGeneric(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithLock())
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()

	// 模拟场景: 号码空号检测
	type CheckResult struct {
		ValidCount   int
		InvalidCount int
	}

	lockKey := "lock:mobile:check"
	result, err := LockExtendGeneric(client, lockKey, 30*time.Second, func() (CheckResult, error) {
		// 模拟耗时检测任务
		time.Sleep(100 * time.Second)
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
	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithLock())
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()

	lockKey := "feige-cloud-backend-initCron-LockAwaitOnce"

	// 模拟场景: 启动时初始化定时任务
	client.LockAwaitOnce(lockKey, 30*time.Second, func() {
		// 模拟初始化定时任务
		// gcron.AddSingleton("*/10 * * * * *", func() { ... })
		// gcron.AddSingleton("0 */30 * * * *", func() { ... })
		t.Log("定时任务初始化完成")
	})

	// LockAwaitOnce 是异步的，让程序等待一段时间模拟运行
	time.Sleep(5 * time.Second)
}

// LockAwaitOnce 带清理函数 - 续期失败时清理定时任务
func TestLockAwaitOnceWithClear(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithLock())
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()

	lockKey := "feige-cloud-backend-smsStat-LockAwaitOnce"

	// 模拟场景: 短信统计任务，带清理函数
	client.LockAwaitOnce(lockKey, 5*time.Second, func() {
		// 模拟统计任务
		// gcron.AddSingleton("0 0 * * * *", func() { ... })
		t.Log("统计任务开始执行")
	}, func() {
		// 续期失败时清理，防止其他机器重复执行
		// gcron.Remove("SmsStatTask")
		t.Log("续期失败，清理定时任务")
	})

	// 让程序等待一段时间模拟运行
	time.Sleep(10 * time.Second)
}

// ==================== Redlock 多实例锁测试 ====================

// WithLock 多实例 - Redlock 模式
func TestWithLockRedlock(t *testing.T) {
	// 多实例 Redlock 模式
	redlockOptions := []*redis.Options{
		{Addr: "localhost:6379"},
		{Addr: "localhost:6380"},
		{Addr: "localhost:6381"},
	}

	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithLock(redlockOptions...))
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()

	if client.GetRs() == nil {
		t.Skip("Redlock not initialized")
	}

	mutex := client.NewMutex("test:redlock")

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

// ==================== 缓存测试 ====================

func TestCacheOnce(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithCache())
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// 模拟实际使用: Cache.Once 带 TTL 和 Do 回调
	type UserInfo struct {
		Name string
		Age  int
	}
	var user UserInfo
	callCount := 0
	key := "test:cache:user:1001"

	err = client.GetCache().Once(ctx, key,
		cache.Value(&user),
		cache.TTL(10*time.Minute),
		cache.Refresh(false),
		cache.Do(func(ctx context.Context) (any, error) {
			callCount++
			// 模拟从数据库查询
			return UserInfo{Name: "张三", Age: 25}, nil
		}),
	)
	if err != nil {
		t.Fatalf("Cache Once failed: %v", err)
	}
	if user.Name != "张三" || user.Age != 25 {
		t.Errorf("Cache Once result = %+v, want {Name:张三 Age:25}", user)
	}
	if callCount != 1 {
		t.Errorf("Function called %d times, want 1", callCount)
	}

	// 第二次调用应该使用缓存，不会执行 Do 函数
	var user2 UserInfo
	err = client.GetCache().Once(ctx, key,
		cache.Value(&user2),
		cache.TTL(10*time.Minute),
		cache.Do(func(ctx context.Context) (any, error) {
			callCount++
			return UserInfo{Name: "李四", Age: 30}, nil
		}),
	)
	if err != nil {
		t.Fatalf("Cache Once second call failed: %v", err)
	}
	if user2.Name != "张三" {
		t.Errorf("Cache hit should return cached value, got %+v", user2)
	}
	if callCount != 1 {
		t.Errorf("Do function should not be called again, callCount = %d", callCount)
	}

	// 清理缓存
	client.GetCache().Delete(ctx, key)
}

func TestCacheDelete(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"}, WithCache())
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// 先设置缓存
	key := "test:cache:delete"
	var val string
	err = client.GetCache().Once(ctx, key,
		cache.Value(&val),
		cache.TTL(10*time.Minute),
		cache.Do(func(ctx context.Context) (any, error) {
			return "to-be-deleted", nil
		}),
	)
	if err != nil {
		t.Fatalf("Cache Once failed: %v", err)
	}

	// 删除缓存
	err = client.GetCache().Delete(ctx, key)
	if err != nil {
		t.Fatalf("Cache Delete failed: %v", err)
	}

	// 删除后再次调用应该重新执行 Do 函数
	callCount := 0
	err = client.GetCache().Once(ctx, key,
		cache.Value(&val),
		cache.TTL(10*time.Minute),
		cache.Do(func(ctx context.Context) (any, error) {
			callCount++
			return "new-value", nil
		}),
	)
	if err != nil {
		t.Fatalf("Cache Once after delete failed: %v", err)
	}
	if callCount != 1 {
		t.Errorf("Do function should be called after delete, callCount = %d", callCount)
	}
	if val != "new-value" {
		t.Errorf("After delete, should get new value, got %q", val)
	}
}

func TestCacheJoin(t *testing.T) {
	// 模拟实际使用: 用 Join 拼接缓存 key
	key := Join("user:info", 1001)
	if key != "user:info:1001" {
		t.Errorf("Join = %q, want %q", key, "user:info:1001")
	}
}

// ==================== Join 测试 ====================

func TestJoin(t *testing.T) {
	tests := []struct {
		key  string
		args []any
		want string
	}{
		{"", nil, ""},
		{"key", nil, "key"},
		{"", []any{"a", "b"}, "a:b"},
		{"key", []any{"a", "b"}, "key:a:b"},
		{"key", []any{"", "b"}, "key:b"},
		{"key", []any{1, 2}, "key:1:2"},
	}

	for _, tt := range tests {
		got := Join(tt.key, tt.args...)
		if got != tt.want {
			t.Errorf("Join(%q, %v) = %q, want %q", tt.key, tt.args, got, tt.want)
		}
	}
}
