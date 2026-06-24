package jetcache

import (
	"context"
	"fmt"
	"testing"
	"time"

	cache "github.com/mgtv-tech/jetcache-go"
	"github.com/mgtv-tech/jetcache-go/local"
	"github.com/redis/go-redis/v9"
)

// ==================== 初始化测试 ====================

func TestNewSimple(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available, skip test: %v", err)
	}
	defer rdb.Close()

	c := NewSimple(rdb)
	if c == nil {
		t.Fatal("NewSimple should not return nil")
	}
	if c.Cache() == nil {
		t.Error("Cache should not be nil")
	}
	if c.SourceID() == "" {
		t.Error("SourceID should not be empty")
	}
	if c.ChannelName() != "syncLocalChannel" {
		t.Errorf("ChannelName = %q, want %q", c.ChannelName(), "syncLocalChannel")
	}
}

func TestNew(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available, skip test: %v", err)
	}
	defer rdb.Close()

	c := New(rdb, "my:channel")
	if c == nil {
		t.Fatal("New should not return nil")
	}
	if c.Cache() == nil {
		t.Error("Cache should not be nil")
	}
	if c.SourceID() == "" {
		t.Error("SourceID should not be empty")
	}
	if c.ChannelName() != "my:channel" {
		t.Errorf("ChannelName = %q, want %q", c.ChannelName(), "my:channel")
	}
}

func TestNewWithCustomOptions(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available, skip test: %v", err)
	}
	defer rdb.Close()

	// 自定义本地缓存容量和过期时间
	c := New(rdb, "test:channel",
		cache.WithName("test"),
		cache.WithLocal(local.NewTinyLFU(5000, 2*time.Minute)),
		cache.WithStatsDisabled(true),
	)
	if c == nil {
		t.Fatal("New with custom options should not return nil")
	}
	if c.Cache() == nil {
		t.Error("Cache should not be nil")
	}
}

// ==================== 缓存操作测试 ====================

// 模拟数据库查询
type UserInfo struct {
	Name string
	Age  int
}

// fakeDB 模拟数据库，1=正常返回 2=数据库出错 3=查无数据
func fakeDB(n int) (any, error) {
	switch n {
	case 1:
		return UserInfo{Name: "张三", Age: 25}, nil
	case 2:
		return nil, fmt.Errorf("database connection error")
	case 3:
		return nil, ErrRecordNotFound
	default:
		return UserInfo{Name: "张三", Age: 25}, nil
	}
}

// 正常查询，缓存命中
func TestCacheOnce(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer rdb.Close()

	c := NewSimple(rdb)
	ctx := context.Background()

	var user UserInfo
	callCount := 0
	key := "test:cache:user:1001"

	// 第一次调用，执行 Do 从 "数据库" 查询
	err := c.Cache().Once(ctx, key,
		cache.Value(&user),
		cache.TTL(10*time.Minute),
		cache.Refresh(false),
		cache.Do(func(ctx context.Context) (any, error) {
			callCount++
			return fakeDB(1)
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
	err = c.Cache().Once(ctx, key,
		cache.Value(&user2),
		cache.TTL(10*time.Minute),
		cache.Do(func(ctx context.Context) (any, error) {
			callCount++
			return fakeDB(1)
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
	c.Cache().Delete(ctx, key)
}

// 数据库查询出错，返回 ErrRecordNotFound，不缓存错误，下次调用会重新查询
func TestCacheOnceDBError(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer rdb.Close()

	c := NewSimple(rdb)
	ctx := context.Background()

	var user UserInfo
	key := "test:cache:user:dberror"

	// 数据库出错，返回 ErrRecordNotFound
	err := c.Cache().Once(ctx, key,
		cache.Value(&user),
		cache.TTL(10*time.Minute),
		cache.Do(func(ctx context.Context) (any, error) {
			return fakeDB(2)
		}),
	)
	// Do 返回普通错误时，Once 会直接返回该错误
	if err == nil {
		t.Error("Expected error from database, got nil")
	}

	// 清理
	c.Cache().Delete(ctx, key)
}

// 数据库查无数据，返回 ErrRecordNotFound，缓存空值防止缓存穿透
func TestCacheOnceNoData(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer rdb.Close()

	c := NewSimple(rdb)
	ctx := context.Background()

	var user UserInfo
	callCount := 0
	key := "test:cache:user:nodata"

	// 查无数据，返回 ErrRecordNotFound
	err := c.Cache().Once(ctx, key,
		cache.Value(&user),
		cache.TTL(10*time.Minute),
		cache.Do(func(ctx context.Context) (any, error) {
			callCount++
			return fakeDB(3)
		}),
	)
	// ErrRecordNotFound 会被 jetcache-go 缓存为空值占位符，Once 返回 nil
	if err != nil {
		t.Fatalf("Cache Once with not found should return nil, got: %v", err)
	}

	// 再次调用，由于空值已被缓存，Do 不会执行
	var user2 UserInfo
	err = c.Cache().Once(ctx, key,
		cache.Value(&user2),
		cache.TTL(10*time.Minute),
		cache.Do(func(ctx context.Context) (any, error) {
			callCount++
			return fakeDB(1)
		}),
	)
	if err != nil {
		t.Fatalf("Cache Once second call failed: %v", err)
	}
	if callCount != 1 {
		t.Errorf("Do function should only be called once (cached not-found), callCount = %d", callCount)
	}

	// 清理缓存
	c.Cache().Delete(ctx, key)
}

func TestCacheDelete(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer rdb.Close()

	c := NewSimple(rdb)
	ctx := context.Background()

	// 先设置缓存
	key := "test:cache:delete"
	var val string
	err := c.Cache().Once(ctx, key,
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
	err = c.Cache().Delete(ctx, key)
	if err != nil {
		t.Fatalf("Cache Delete failed: %v", err)
	}

	// 删除后再次调用应该重新执行 Do 函数
	callCount := 0
	err = c.Cache().Once(ctx, key,
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

// ==================== Getter 测试 ====================

func TestCacheGetter(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer rdb.Close()

	c := New(rdb, "test:channel")

	if c.Cache() == nil {
		t.Error("Cache() should not return nil")
	}
	if c.SourceID() == "" {
		t.Error("SourceID() should not return empty string")
	}
	if c.ChannelName() != "test:channel" {
		t.Errorf("ChannelName() = %q, want %q", c.ChannelName(), "test:channel")
	}
}

// ==================== ErrRecordNotFound 测试 ====================

func TestErrRecordNotFound(t *testing.T) {
	if ErrRecordNotFound == nil {
		t.Error("ErrRecordNotFound should not be nil")
	}
	if ErrRecordNotFound.Error() != "data not found" {
		t.Errorf("ErrRecordNotFound.Error() = %q, want %q", ErrRecordNotFound.Error(), "data not found")
	}
}
