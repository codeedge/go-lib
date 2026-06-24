package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

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

// Pipeline 批量执行，减少网络往返，但不保证原子性
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

// TxPipeline 事务管道（MULTI/EXEC）
// 保证：命令按顺序执行，不会被其他客户端的命令插入
// 不保证：其中一个命令失败时，不会回滚已执行的命令
// 与 Lua 脚本的区别：MULTI/EXEC 在 EXEC 之前就确定了所有命令，命令之间不能有逻辑判断；
// Lua 脚本在服务端执行，可以在脚本里根据读取的值决定下一步操作
func TestTxPipeline(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// 初始化
	client.Rdb().Set(ctx, "test:tx:a", "10", 0)
	client.Rdb().Set(ctx, "test:tx:b", "20", 0)

	// TxPipeline 事务管道：原子执行（MULTI/EXEC）
	pipe := client.Rdb().TxPipeline()
	pipe.Incr(ctx, "test:tx:a")
	pipe.IncrBy(ctx, "test:tx:b", 5)
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("TxPipeline Exec failed: %v", err)
	}
	if len(cmds) != 2 {
		t.Errorf("TxPipeline returned %d commands, want 2", len(cmds))
	}

	// 验证
	valA, _ := client.Rdb().Get(ctx, "test:tx:a").Result()
	if valA != "11" {
		t.Errorf("tx:a = %q, want %q", valA, "11")
	}
	valB, _ := client.Rdb().Get(ctx, "test:tx:b").Result()
	if valB != "25" {
		t.Errorf("tx:b = %q, want %q", valB, "25")
	}

	// 清理
	client.Rdb().Del(ctx, "test:tx:a", "test:tx:b")
}

// ==================== Lua 脚本测试 ====================

// Lua 脚本原子执行：脚本执行期间不会有其他命令插入（Redis 单线程）
// 注意：这里的原子性指隔离性（不被插入），脚本内某个命令失败不会回滚已执行的命令
func TestLuaScript(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// 设置初始值
	client.Rdb().Set(ctx, "test:lua:counter", "0", 0)

	// Lua 原子递增脚本
	script := redis.NewScript(`
		local val = redis.call("INCR", KEYS[1])
		return val
	`)

	// 执行 Lua 脚本
	result, err := script.Run(ctx, client.Rdb(), []string{"test:lua:counter"}).Int()
	if err != nil {
		t.Fatalf("Lua script failed: %v", err)
	}
	if result != 1 {
		t.Errorf("Lua INCR = %d, want 1", result)
	}

	// 再次执行
	result, err = script.Run(ctx, client.Rdb(), []string{"test:lua:counter"}).Int()
	if err != nil {
		t.Fatalf("Lua script second run failed: %v", err)
	}
	if result != 2 {
		t.Errorf("Lua INCR = %d, want 2", result)
	}

	// Lua 带参数的脚本
	setScript := redis.NewScript(`
		redis.call("SET", KEYS[1], ARGV[1])
		return redis.call("GET", KEYS[1])
	`)

	val, err := setScript.Run(ctx, client.Rdb(), []string{"test:lua:set"}, "hello").Result()
	if err != nil {
		t.Fatalf("Lua set script failed: %v", err)
	}
	if val != "hello" {
		t.Errorf("Lua SET = %q, want %q", val, "hello")
	}

	// 清理
	client.Rdb().Del(ctx, "test:lua:counter", "test:lua:set")
}

// ==================== Join 测试 ====================

func TestJoin(t *testing.T) {
	if Join("key", "a", "b") != "key:a:b" {
		t.Errorf("Join basic failed")
	}
	if Join("key", 1, 2) != "key:1:2" {
		t.Errorf("Join int failed")
	}
	if Join("key", "", "b") != "key:b" {
		t.Errorf("Join empty filter failed")
	}
	if Join("", nil) != "" {
		t.Errorf("Join empty failed")
	}
}

// ==================== Rdb 测试 ====================

func TestRdb(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()

	rdb := client.Rdb()
	if rdb == nil {
		t.Error("Rdb() should not return nil")
	}

	// 验证 Rdb 可以正常执行命令
	ctx := context.Background()
	err = rdb.Set(ctx, "test:rdb", "ok", 10*time.Second).Err()
	if err != nil {
		t.Fatalf("Rdb Set failed: %v", err)
	}
	val, err := rdb.Get(ctx, "test:rdb").Result()
	if err != nil {
		t.Fatalf("Rdb Get failed: %v", err)
	}
	if val != "ok" {
		t.Errorf("Rdb Get = %q, want %q", val, "ok")
	}

	rdb.Del(ctx, "test:rdb")
}

// ==================== 错误处理测试 ====================

func TestNewConnectionRefused(t *testing.T) {
	options := &redis.Options{
		Addr: "localhost:9999", // 不存在的端口
	}

	_, err := New(options)
	if err == nil {
		t.Error("Expected error for connection refused, got nil")
	}
}

func TestNewClusterConnectionRefused(t *testing.T) {
	options := &redis.ClusterOptions{
		Addrs: []string{":9999"},
	}

	_, err := NewCluster(options)
	if err == nil {
		t.Error("Expected error for cluster connection refused, got nil")
	}
}

func TestNewRingConnectionRefused(t *testing.T) {
	options := &redis.RingOptions{
		Addrs: map[string]string{
			"shard1": ":9999",
		},
	}

	_, err := NewRing(options)
	if err == nil {
		t.Error("Expected error for ring connection refused, got nil")
	}
}

// ==================== 并发测试 ====================

func TestConcurrentAccess(t *testing.T) {
	client, err := New(&redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Rdb().Close()
	ctx := context.Background()

	// 并发写入
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			key := fmt.Sprintf("test:concurrent:%d", n)
			client.Rdb().Set(ctx, key, fmt.Sprintf("val%d", n), 10*time.Second)
			done <- true
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证所有值
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("test:concurrent:%d", i)
		val, err := client.Rdb().Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("Get %s failed: %v", key, err)
		}
		expected := fmt.Sprintf("val%d", i)
		if val != expected {
			t.Errorf("Get %s = %q, want %q", key, val, expected)
		}
	}

	// 清理
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("test:concurrent:%d", i)
	}
	client.Rdb().Del(ctx, keys...)
}
