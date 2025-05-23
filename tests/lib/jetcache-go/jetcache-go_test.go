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

//func Test_1(t *testing.T) {
//
//}

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
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)),
		cache.WithCodec(json2.Name),
		cache.WithErrNotFound(errRecordNotFound))

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

	mycache := cache.New(cache.WithName("any"),
		cache.WithRemote(remote.NewGoRedisV9Adapter(ring)),
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)),
		cache.WithErrNotFound(errRecordNotFound),
		cache.WithRefreshDuration(time.Minute))

	ctx := context.TODO()
	key := "mykey:1"
	obj := new(object)
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

	sourceID := "12345678" // Unique identifier for this cache instance
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
		return nil, errRecordNotFound
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
