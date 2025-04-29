package rds

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"log"
	"strings"
	"time"
)

var Rdb *redis.Client
var Rs *redsync.Redsync

func Init(options *redis.Options) {
	Rdb = redis.NewClient(options)
	pong, err := Rdb.Ping(context.Background()).Result()
	if errors.Is(err, redis.Nil) {
		fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		fmt.Errorf("redis失败:%v", err)
	} else {
		log.Printf("redis-init:%v\n", pong)
		setRedsync()
	}
}

// InitFailover 哨兵模式
func InitFailover(options *redis.FailoverOptions) {
	//&redis.FailoverOptions{
	//	MasterName:    "master-name",
	//	SentinelAddrs: []string{":9126", ":9127", ":9128"},
	//}
	Rdb := redis.NewFailoverClient(options)
	pong, err := Rdb.Ping(context.Background()).Result()
	if errors.Is(err, redis.Nil) {
		fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		fmt.Errorf("redis失败:%v", err)
	} else {
		log.Printf("redis-init:%v\n", pong)
	}
}

func setRedsync() {
	pool := goredis.NewPool(Rdb)
	Rs = redsync.New(pool)
}

// LockExtend 创建带续租的分布式锁并执行任务
// 适合场景：分布式系统并发时只允许一个进程执行一些耗时操作，无法保证锁在释放前执行完，需要给锁续租，直到程序执行完后释放锁，并停止续租
// lockKey 锁的key
// expiry 锁的过期时间
// task 执行的任务
func LockExtend(lockKey string, expiry time.Duration, task func()) {
	if expiry < 1 {
		expiry = 10 * time.Second
	}
	mutex := Rs.NewMutex(lockKey, redsync.WithExpiry(expiry)) // 创建一个带有过期时间的互斥锁
	if err := mutex.Lock(); err != nil {
		log.Println(err)
		return //  锁获取失败直接退出
	}

	done := make(chan bool)

	// 开启一个goroutine，周期性地续租锁
	go func() {
		ticker := time.NewTicker(expiry / 2) // 按照需求调整 每隔过期时间的一半续租一次
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ok, err := mutex.Extend()
				if !ok || err != nil {
					log.Printf("Failed to extend lock: ok:%v err:%v", ok, err)
					return
				}
			case <-done:
				return
			}
		}
	}()

	// 执行需要锁的工作
	task()

	// 通知goRoutine停止续租
	close(done)
	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := mutex.Unlock(); !ok || err != nil {
		log.Println("unlock failed")
	}
}

// LockAwaitOnce 创建带续租的分布式锁并执行任务，等待执行，一直循环获取锁直到获得成功
// 适合场景：定时任务集群模式只需要一台机器执行任务，其他机器需要一直尝试获取锁，防止获取锁的机器重启或者宕机导致没有机器执行任务
// 适合启动只会执行1次的初始化程序防止并发启动，比如初始化定时任务,在task中添加定时任务
// 如果想均衡利用各个机器的资源执行任务需要在每个定时任务中使用Lock UnLock，这样也会使redis的压力变大
// lockKey 锁的key
// expiry 锁的过期时间
// task 执行的任务
// clear 在续期失败时清理执行的任务，比如清理定时任务，防止续期失败后其他机器和本机器多次执行了定时任务
func LockAwaitOnce(lockKey string, expiry time.Duration, task func(), clear func()) {
	if expiry < 1 {
		expiry = 10 * time.Second
	}
	mutex := Rs.NewMutex(lockKey, redsync.WithExpiry(expiry)) // 创建一个带有过期时间的互斥锁
	// 一直循环尝试获取锁，获取成功则执行任务
	for {
		if err := mutex.Lock(); err != nil {
			// 获取失败睡眠一半的时间再重试
			time.Sleep(expiry / 2)
			continue
		}

		// 开启一个goroutine，周期性地续租锁
		go func() {
			ticker := time.NewTicker(expiry / 2) // 按照需求调整 每隔过期时间的一半续租一次
			defer ticker.Stop()

			for range ticker.C {
				ok, err := mutex.Extend()
				if !ok || err != nil {
					log.Printf("Failed to extend lock: ok:%v err:%v", ok, err)
					clear()
					return
				}
			}
		}()

		// 执行需要锁的工作，使用匿名函数来限制 recover 的作用域
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Task panic recovered: %v, key: %s", r, lockKey)
				}
			}()
			task()
		}()

		// 获取成功后退出循环
		return
	}

}

// Join 拼接cacheKey
func Join(key string, args ...interface{}) string {
	// 步骤1：转换所有参数为字符串并过滤空值
	segments := make([]string, 0, len(args)+1)
	if key != "" {
		segments = append(segments, key)
	}

	// 步骤2：处理变长参数
	for _, arg := range args {
		s := fmt.Sprint(arg)
		if s != "" { // 过滤空字符串参数
			segments = append(segments, s)
		}
	}

	// 步骤3：拼接最终Key并处理边界情况
	if len(segments) == 0 {
		return ""
	}
	return strings.Join(segments, ":")
}
