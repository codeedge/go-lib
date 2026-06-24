package jetcache

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	cache "github.com/mgtv-tech/jetcache-go"
	json2 "github.com/mgtv-tech/jetcache-go/encoding/json"
	"github.com/mgtv-tech/jetcache-go/local"
	"github.com/mgtv-tech/jetcache-go/remote"
)

// ErrRecordNotFound 缓存未命中时返回的错误
var ErrRecordNotFound = errors.New("data not found")

// Cache 二级缓存（本地 TinyLFU + Redis）
type Cache struct {
	cache       cache.Cache // 底层 jetcache-go 缓存实例
	rdb         redis.UniversalClient
	sourceID    string
	channelName string
	pubSub      *redis.PubSub
}

// NewSimple 初始化二级缓存，使用默认配置
// 默认：频道名 "syncLocalChannel"，本地缓存容量10000，过期1分钟，刷新间隔1分钟，停止刷新时间1小时
func NewSimple(rdb redis.UniversalClient) *Cache {
	sourceID := uuid.New().String()
	return New(rdb, "syncLocalChannel",
		cache.WithName("any"),
		cache.WithRemote(remote.NewGoRedisV9Adapter(rdb)),          // Redis 作为远程缓存
		cache.WithLocal(local.NewTinyLFU(10000, time.Minute)),      // 设置本地缓存，容量10000，过期时间1分钟
		cache.WithCodec(json2.Name),                                // 序列化方式，默认是msgpack.Name
		cache.WithErrNotFound(ErrRecordNotFound),                   // 设置缓存未命中时返回的错误
		cache.WithRefreshDuration(time.Minute),                     // 设置异步刷新时间间隔
		cache.WithStopRefreshAfterLastAccess(time.Hour),            // 设置缓存 key 没有访问后的刷新任务取消时间
		cache.WithSourceId(sourceID),                               // 设置当前实例的唯一标识
		cache.WithSyncLocal(true),                                  // 启用本地缓存同步
		cache.WithStatsDisabled(true),                              // 禁用统计
		cache.WithEventHandler(func(event *cache.Event) {
			// Broadcast local cache invalidation for the received keys
			// 广播本地缓存失效事件
			bs, _ := json.Marshal(event)
			rdb.Publish(context.Background(), "syncLocalChannel", string(bs))
		}),
	)
}

// New 初始化二级缓存（本地缓存 + Redis 缓存），支持自定义频道名和 jetcache-go 配置
// 使用 jetcache-go 库实现，支持：
// 1. 本地缓存（TinyLFU）：减少 Redis 访问，提高热点数据读取速度
// 2. Redis 缓存：分布式缓存，多节点共享
// 3. 缓存同步：通过 Redis Pub/Sub 实现多节点本地缓存失效通知
// 4. 异步刷新：支持缓存过期后异步刷新，避免缓存击穿
// opts 直接透传给 jetcache-go，不设任何默认值
func New(rdb redis.UniversalClient, channelName string, opts ...cache.Option) *Cache {
	sourceID := uuid.New().String()
	pubSub := rdb.Subscribe(context.Background(), channelName)

	c := &Cache{
		cache:       cache.New(opts...),
		rdb:         rdb,
		sourceID:    sourceID,
		channelName: channelName,
		pubSub:      pubSub,
	}

	// 启动 goroutine 监听缓存同步事件
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println("setJetCache panic:", r)
			}
		}()
		for {
			msg := <-pubSub.Channel()
			var event *cache.Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				log.Println("Error unmarshalling message:", err)
				continue
			}

			// Invalidate local cache for received keys (except own events)
			// 收到其他实例的缓存失效事件时，删除本地缓存中对应的 key
			if event.SourceID != sourceID {
				for _, key := range event.Keys {
					c.cache.DeleteFromLocalCache(key)
				}
			}
		}
	}()

	return c
}

// Cache 获取底层 jetcache-go 缓存实例
func (c *Cache) Cache() cache.Cache {
	return c.cache
}

// SourceID 获取缓存实例唯一标识
func (c *Cache) SourceID() string {
	return c.sourceID
}

// ChannelName 获取同步频道名
func (c *Cache) ChannelName() string {
	return c.channelName
}
