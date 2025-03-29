package singleflight

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/singleflight"
	"sync"
	"testing"
	"time"
)

// 并发执行相同key的函数，只会执行1次
func Test_SingleFlight(t *testing.T) {
	NewDelayReturn := func(dur time.Duration, n int) func() (interface{}, error) {
		return func() (interface{}, error) {
			time.Sleep(dur)
			return n, nil
		}
	}
	g := singleflight.Group{}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		ret, err := g.Do("key", NewDelayReturn(time.Second*1, 1))
		if err != nil {
			panic(err)
		}
		fmt.Printf("key-1 get %v\n", ret)
		wg.Done()
	}()
	go func() {
		time.Sleep(100 * time.Millisecond) // make sure this is call is later
		ret, err := g.Do("key", NewDelayReturn(time.Second*2, 2))
		if err != nil {
			panic(err)
		}
		fmt.Printf("key-2 get %v\n", ret)
		wg.Done()
	}()
	wg.Wait()
}

// 对于缓存的更新，可以这样实现
func Test_SingleFlight_Cache(t *testing.T) {
	cacheMiss := true
	if cacheMiss {
		cacheKey := "key"
		g := singleflight.Group{}
		fn := func() (interface{}, error) {
			// 缓存更新逻辑
			return 1, nil
		}
		data, err := g.Do(cacheKey, fn)
		fmt.Println(data, err)
	}
	fmt.Println("OK")
}
