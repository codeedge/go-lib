package goredis

import (
	"fmt"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	goredislib "github.com/redis/go-redis/v9"
	"testing"
	"time"
)

// redis需要初始化 仅作演示用
func Test_LockRedsync(t *testing.T) {
	// Create a pool with go-redis (or redigo) which is the pool redisync will
	// use while communicating with Redis. This can also be any pool that
	// implements the `redis.Pool` interface.
	client := goredislib.NewClient(&goredislib.Options{
		Addr: "localhost:6379",
	})
	pool := goredis.NewPool(client) // or, pool := redigo.NewPool(...)

	// Create an instance of redisync to be used to obtain a mutual exclusion
	// lock.
	rs := redsync.New(pool)

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	mutexname := "my-global-mutex"
	mutex := rs.NewMutex(mutexname)

	// Obtain a lock for our given mutex. After this is successful, no one else
	// can obtain the same lock (the same mutex name) until we unlock it.
	if err := mutex.Lock(); err != nil {
		panic(err)
	}

	// Do your work that requires the lock.

	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := mutex.Unlock(); !ok || err != nil {
		panic("unlock failed")
	}
}

// redis需要初始化 仅作演示用 自动续租
// 【Redis】【Go】分布式锁和续租：Redis分布式锁与Redsync源码解读 https://juejin.cn/post/7233284282964770871
// 这段代码首先创建一个go-redis客户端和连接池，并使用它们创建一个redsync实例。然后，创建一个带有10秒过期时间的互斥锁并尝试获取它。
// 在获取锁后，开启一个goroutine来周期性地续租锁。当需要锁的工作完成后，关闭done channel以通知续租goroutine停止，并释放锁。
// 如何是在定时任务等场景使用，不需要释放锁，需要一直续期，直到程序退出或者崩溃才会释放锁，这样保证集群下只有一台机器获得了定时任务的锁。
func Test_ExtendContext(t *testing.T) {
	// Create a pool with go-redis (or redigo) which is the pool redisync will
	// use while communicating with Redis. This can also be any pool that
	// implements the `redis.Pool` interface.
	client := goredislib.NewClient(&goredislib.Options{
		Addr: "localhost:6379",
	})
	pool := goredis.NewPool(client) // or, pool := redigo.NewPool(...)

	// Create an instance of redisync to be used to obtain a mutual exclusion
	// lock.
	rs := redsync.New(pool)

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	mutexname := "my-global-mutex"
	mutex := rs.NewMutex(mutexname, redsync.WithExpiry(10*time.Second)) // 创建一个带有10秒过期时间的互斥锁

	// Obtain a lock for our given mutex. After this is successful, no one else
	// can obtain the same lock (the same mutex name) until we unlock it.
	if err := mutex.Lock(); err != nil {
		panic(err)
	}

	// Do your work that requires the lock.

	//创建一个channel，用来通知续租goroutine任务已经完成
	done := make(chan bool)

	// 开启一个goroutine，周期性地续租锁
	go func() {
		ticker := time.NewTicker(5 * time.Second) // 按照需求调整 每隔5秒续租一次，此处默认的续租时间是10秒
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ok, err := mutex.Extend()
				if err != nil {
					fmt.Println("Failed to extend lock:", err)
				} else if !ok {
					fmt.Println("Failed to extend lock: not successes")
				}
			case <-done:
				return
			}
		}
	}()

	// 执行需要锁的工作
	time.Sleep(30 * time.Second)
	//通知goRoutine停止续租
	close(done)

	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := mutex.Unlock(); !ok || err != nil {
		panic("unlock failed")
	}
}
