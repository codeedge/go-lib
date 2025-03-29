package gredis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/codeedge/go-lib/lib/goRedis"
	"github.com/codeedge/go-lib/lib/goRedis/redisCache"
	"githu
	"sync"
	"testing"
	"time"
)

type User struct {
	ID    int
	Name  string
	Email string
}

var ctx = context.Background()

func Test_cache(t *testing.T) {
	fmt.Println("=== 基本操作 ===")
	basicOperations()

	fmt.Println("\n=== 防缓存击穿 ===")
	cachePenetration()

	fmt.Println("\n=== 本地缓存验证 ===")
	localCacheDemo()

	fmt.Println("\n=== 自定义序列化 ===")
	customMarshaler()
}

// 基本操作示例 ==============================================
func basicOperations() {
	user := &User{
		ID:    1,
		Name:  "张三",
		Email: "zhangsan@example.com",
	}

	// 设置缓存
	if err := redisCache.Cache.Set(&cache.Item{
		Ctx:   ctx,
		Key:   userKey(user.ID),
		Value: user,
		TTL:   time.Hour,
	}); err != nil {
		panic(err)
	}

	// 获取缓存
	var savedUser User
	if err := redisCache.Cache.Get(ctx, userKey(user.ID), &savedUser); err == nil {
		fmt.Printf("获取用户成功: %+v\n", savedUser)
	}

	// 删除缓存
	if err := redisCache.Cache.Delete(ctx, userKey(user.ID)); err == nil {
		fmt.Println("删除用户缓存成功")
	}
}

// 防缓存击穿示例 ============================================
func getFromDB(userID int) (*User, error) {
	fmt.Printf("查询数据库用户 %d\n", userID)
	time.Sleep(100 * time.Millisecond)
	return &User{
		ID:    userID,
		Name:  fmt.Sprintf("用户%d", userID),
		Email: fmt.Sprintf("user%d@example.com", userID),
	}, nil
}

func getWithOnce(userID int) (*User, error) {
	var user User
	key := fmt.Sprintf("user_once:%d", userID)

	// 使用防击穿模式获取
	err := redisCache.Cache.Once(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: &user,
		TTL:   time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			return getFromDB(userID)
		},
	})

	return &user, err
}

func cachePenetration() {
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if user, err := getWithOnce(100); err == nil {
				fmt.Printf("获取用户: %+v\n", user)
			}
		}()
	}

	wg.Wait()
}

// 本地缓存示例 ==============================================
func localCacheDemo() {
	key := "local_cache_test"
	value := "缓存值"

	// 设置缓存（会同时写入Redis和本地缓存）
	if err := redisCache.Cache.Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: value,
		TTL:   time.Minute,
	}); err != nil {
		panic(err)
	}

	// 第一次获取（填充本地缓存）
	var result string
	if err := redisCache.Cache.Get(ctx, key, &result); err == nil {
		fmt.Println("首次获取:", result)
	}

	// 删除Redis数据验证本地缓存
	if goRedis.Rdb.Del(ctx, key).Err() == nil {
		fmt.Println("已删除Redis中的数据")
	}

	// 再次获取（应该从本地缓存获取）
	if err := redisCache.Cache.Get(ctx, key, &result); err == nil {
		fmt.Println("本地缓存生效:", result)
	}

	// 完全删除缓存
	if err := redisCache.Cache.Delete(ctx, key); err == nil {
		fmt.Println("完全删除缓存成功")
	}
}

// 自定义序列化示例 ============================================
type customUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func (u *customUser) MarshalBinary() ([]byte, error) {
	return json.Marshal(u)
}

func (u *customUser) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, u)
}

func customMarshaler() {
	user := &customUser{ID: 2, Name: "李四"}

	if err := redisCache.Cache.Set(&cache.Item{
		Ctx:   ctx,
		Key:   "custom_user",
		Value: user,
		TTL:   time.Hour,
	}); err != nil {
		panic(err)
	}

	var savedUser customUser
	if err := redisCache.Cache.Get(ctx, "custom_user", &savedUser); err == nil {
		fmt.Printf("自定义序列化用户: %+v\n", savedUser)
	}
}

// 辅助函数 ==================================================
func userKey(id int) string {
	return fmt.Sprintf("user:%d", id)
}
