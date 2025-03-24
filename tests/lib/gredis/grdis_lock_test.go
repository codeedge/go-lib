package gredis

import (
	"fmt"
	"github.com/go-redsync/redsync"
	"github.com/gomodule/redigo/redis"
	"sync"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	var err error
	_redis := newPool()
	redisKey := "test-lock-incr"
	_, _ = _redis.Get().Do("set", redisKey, 1)
	//if err != nil {
	//	fmt.Println(err.Error())
	//}

	//_, err = _redis.Get().Do("incr", redisKey)
	//if err != nil {
	//	fmt.Println(err.Error())
	//}

	wg := sync.WaitGroup{}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go incr(_redis, redisKey, wg)
	}
	wg.Wait()

	if err != nil {
		fmt.Println(err.Error())
	}

	//time.Sleep(3 * time.Second)

	fmt.Print("====>>>>")
	result, _ := _redis.Get().Do("get", redisKey)
	fmt.Println(result)
}

func incr(_redis *redis.Pool, redisKey string, wg sync.WaitGroup) {
	defer wg.Done()

	var err error

	pools := []redsync.Pool{}
	_redis2 := newPool()
	pools = append(pools, _redis2)
	mutex := redsync.New(pools).NewMutex("mutex-lock")
	err = mutex.Lock()

	_, err = _redis.Get().Do("incr", redisKey)

	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = mutex.Unlock()

}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: time.Duration(24) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(
				"tcp",
				"192.168.2.110:6379",
				redis.DialPassword("yw123456!@#"),
				redis.DialDatabase(0))
			if err != nil {
				panic(err.Error())
				//s.Log.Errorf("redis", "load redis redisServer err, %s", err.Error())
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				//s.Log.Errorf("redis", "ping redis redisServer err, %s", err.Error())
				return err
			}
			return err
		},
	}
}
