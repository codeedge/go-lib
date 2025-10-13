package jetcache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	json2 "github.com/mgtv-tech/jetcache-go/encoding/json"
	"time"

	"github.com/mgtv-tech/jetcache-go"
	"github.com/mgtv-tech/jetcache-go/local"
	"github.com/mgtv-tech/jetcache-go/remote"
	"github.com/redis/go-redis/v9"
)

// https://juejin.cn/post/7278246015194447887
// https://juejin.cn/post/7403315125007368242
// https://github.com/mgtv-tech/jetcache-go/blob/main/docs/CN/GettingStarted.md

var errRecordNotFound = errors.New("mock gorm.errRecordNotFound")

type object struct {
	Str string
	Num int
}

func Example_basicUsage() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"localhost": ":6379",
		},
		Password: "123456",
	})
	mycache := cache.New(cache.WithName("any"),
		cache.WithRemote(remote.NewGoRedisV9Adapter(ring)),
		// 两种本地缓存：高频小对象用TinyLFU，低频大对象用FreeCache

		// 实践方案示例
		// 场景：电商平台商品详情缓存
		// 热销商品元数据（平均2KB）：
		// local.NewTinyLFU(50000) // 可缓存5万商品≈100MB
		// 商品详情HTML片段（平均50KB）：
		// local.NewFreeCache(512*local.MB) // 支持约1万条目

		// 设置本地缓存过期时间统一为1分钟
		// FreeCache 优点：Zero GC高性能 缺点：缓存Key/Value大小限制，需要预申请虚拟内存 最多256M内存本地缓存，
		// 启动就先分配 采用分段环形存储结构（3个segment），当总数据量超256MB时：1. 触发近似LRU淘汰，覆盖最旧数据 2. 写入速度不受影响（无GC停顿）
		// 条目大小限制：单个Key+Value不得超过总容量的1/1024（256MB场景下单条≤256KB）
		// 适合存储序列化后的JSON/Protobuf等大块数据（需确保单条不超限）
		// time.Minute代表每个缓存条目在本地缓存中的最大存活时间是1分钟。任何存储在本地缓存的数据将在 1分钟(60秒)后自动过期，到期后条目会被自动清理，下次访问时会触发重新加载，这是强制的最大生存期，
		// 即使数据被频繁访问也会到期
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)),
		// TinyLFU 优点：缓存命中率高 缺点：如果缓存条目过多，GC负担会比较严重 最多10000条本地缓存，
		// 当条目数超过10000时，触发W-TinyLFU分层淘汰机制 1. 使用Count-Min Sketch统计访问频率 2. 优先淘汰低频访问的旧数据（结合LRU窗口+SLRU主区）
		// cache.WithLocal(local.NewTinyLFU(10000, time.Minute)),
		cache.WithCodec(json2.Name),              // 序列化方式 默认是msgpack.Name
		cache.WithErrNotFound(errRecordNotFound), // 设置缓存未命中时返回的错误
	)

	ctx := context.TODO()
	key := "mykey:1"
	obj, _ := mockDBGetObject(context.TODO(), 1)
	if err := mycache.Set(ctx, key, cache.Value(obj), cache.TTL(time.Hour)); err != nil {
		panic(err)
	}

	var wanted object
	if err := mycache.Get(ctx, key, &wanted); err == nil {
		fmt.Println(wanted)
	}
	// Output: {mystring 1}

	mycache.Close()
}

// singleflight单飞模式
func Example_advancedUsage() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"localhost": ":6379",
		},
		Password: "123456",
	})

	// 自动刷新缓存
	// jetcache-go提供了自动刷新缓存的能力，目的是为了防止缓存失效时造成的雪崩效应打爆数据库。对一些key比较少，实时性要求不高，加载开销非常大的缓存场景，适合使用自动刷新。
	// 下面的代码指定每分钟刷新一次，1小时如果没有访问就停止刷新。
	// 如果缓存是redis或者多级缓存最后一级是redis，缓存加载行为是全局唯一的，也就是说不管有多少台服务器，同时只有一个服务器在刷新，目的是为了降低后端的加载负担。

	mycache := cache.New(cache.WithName("any"),
		cache.WithRemote(remote.NewGoRedisV9Adapter(ring)),
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)),
		cache.WithErrNotFound(errRecordNotFound),
		cache.WithRefreshDuration(time.Minute),          // 设置异步刷新时间间隔
		cache.WithStopRefreshAfterLastAccess(time.Hour), // 设置缓存 key 没有访问后的刷新任务取消时间
	)

	ctx := context.TODO()
	key := "mykey:1"
	obj := new(object)
	// `Once` 接口通过 `cache.Refresh(true)` 开启自动刷新
	if err := mycache.Once(ctx, key, cache.Value(obj), cache.TTL(time.Hour), cache.Refresh(true),
		cache.Do(func(ctx context.Context) (any, error) {
			return mockDBGetObject(ctx, 1)
		})); err != nil {
		panic(err)
	}
	fmt.Println(obj)
	// Output: &{mystring 42}

	mycache.Close()
}

// MGet批量查询 也使用了单飞模式
func Example_mGetUsage() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"localhost": ":6379",
		},
		Password: "123456",
	})

	mycache := cache.New(cache.WithName("any"),
		cache.WithRemote(remote.NewGoRedisV9Adapter(ring)),
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)),
		cache.WithErrNotFound(errRecordNotFound),
		cache.WithRemoteExpiry(time.Minute),
	)
	cacheT := cache.NewT[int, *object](mycache)

	ctx := context.TODO()
	key := "mykey1"
	ids := []int{1, 2, 3}

	ret := cacheT.MGet(ctx, key, ids, func(ctx context.Context, ids []int) (map[int]*object, error) {
		return mockDBMGetObject(ctx, ids) // key不存在才执行这个，并且会将返回值同步到redis
	})

	var b bytes.Buffer
	for _, id := range ids {
		b.WriteString(fmt.Sprintf("%v", ret[id]))
	}
	fmt.Println(b.String())
	// Output: &{mystring 1}&{mystring 2}&{mystring 3}

	cacheT.Close()
}

func Example_syncLocalUsage() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"localhost": ":6379",
		},
		Password: "123456",
	})

	sourceID := "12345678" // Unique identifier for this cache instance  可以使用uuid或者机器id
	channelName := "syncLocalChannel"
	pubSub := ring.Subscribe(context.Background(), channelName)

	mycache := cache.New(cache.WithName("any"),
		cache.WithRemote(remote.NewGoRedisV9Adapter(ring)),
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)),
		cache.WithErrNotFound(errRecordNotFound),
		cache.WithRemoteExpiry(time.Minute),
		cache.WithSourceId(sourceID),
		cache.WithSyncLocal(true),
		cache.WithEventHandler(func(event *cache.Event) {
			// Broadcast local cache invalidation for the received keys
			bs, _ := json.Marshal(event)
			ring.Publish(context.Background(), channelName, string(bs))
		}),
	)
	obj, _ := mockDBGetObject(context.TODO(), 1)
	if err := mycache.Set(context.TODO(), "mykey", cache.Value(obj), cache.TTL(time.Hour)); err != nil {
		panic(err)
	}

	go func() {
		for {
			msg := <-pubSub.Channel()
			var event *cache.Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				panic(err)
			}
			fmt.Println(event.Keys)

			// Invalidate local cache for received keys (except own events)
			if event.SourceID != sourceID {
				for _, key := range event.Keys {
					mycache.DeleteFromLocalCache(key)
				}
			}
		}
	}()

	// Output: [mykey]
	mycache.Close()
	time.Sleep(time.Second)
}

// 模拟从数据库获取数据
func mockDBGetObject(ctx context.Context, id int) (*object, error) {
	if id > 100 {
		return nil, errRecordNotFound // 如果不使用errRecordNotFound，也可以定义个默认值代表空，判断是不是默认值来判断是不是有数据
	}
	return &object{Str: "mystring", Num: id}, nil
}

// 模拟从数据库批量获取数据
func mockDBMGetObject(ctx context.Context, ids []int) (map[int]*object, error) {
	ret := make(map[int]*object)
	for _, id := range ids {
		ret[id] = &object{Str: "mystring", Num: id}
	}
	return ret, nil
}
