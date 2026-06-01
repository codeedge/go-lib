package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// initLock 初始化分布式锁
// 不传参数：使用当前连接（必须是单节点模式，不支持哨兵/集群/Ring）
// 传参数：使用指定的独立 Redis 实例（Redlock 模式，推荐 5 个独立单节点）
func (c *Client) initLock(options ...*redis.Options) {
	pools := make([]redsyncredis.Pool, 0)

	if len(options) == 0 {
		// 不传参数时，使用默认的 rdb，必须是单节点实例
		client, ok := c.rdb.(*redis.Client)
		if !ok {
			log.Println("Lock only supports single node redis, use WithLock(options...) for Redlock")
			return
		}
		pools = append(pools, goredis.NewPool(client))
	} else {
		// 传参数时，使用指定的独立 Redis 实例（Redlock 模式）
		for i, opt := range options {
			client := redis.NewClient(opt)
			pong, err := client.Ping(context.Background()).Result()
			if err != nil {
				log.Printf("Lock redis[%d] ping failed: %v\n", i, err)
				continue
			}
			log.Printf("Lock redis[%d] init: %v\n", i, pong)
			pools = append(pools, goredis.NewPool(client))
		}
	}

	if len(pools) == 0 {
		log.Println("Lock: no available redis instance")
		return
	}

	c.rs = redsync.New(pools...)
}

// NewMutex 设置分布式锁 配置重试3次，重试延迟50毫秒
func (c *Client) NewMutex(name string, options ...redsync.Option) *redsync.Mutex {
	opts := []redsync.Option{
		redsync.WithTries(3), redsync.WithRetryDelay(time.Millisecond * 50),
	}
	if len(options) > 0 {
		opts = append(opts, options...)
	}
	return c.rs.NewMutex(name, opts...)
}

// LockExtend 创建带续租的分布式锁并执行任务
func (c *Client) LockExtend(lockKey string, expiry time.Duration, task func(), timeouts ...time.Duration) {
	if expiry < 1 {
		expiry = 10 * time.Second
	}

	var timeout = time.Minute * 5
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}

	mutex := c.rs.NewMutex(lockKey, redsync.WithExpiry(expiry))
	if err := mutex.Lock(); err != nil {
		log.Printf("LockExtend lock lockKey:%s err: %v\n", lockKey, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	go func() {
		ticker := time.NewTicker(expiry / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ok, err := mutex.Extend()
				if !ok || err != nil {
					log.Printf("LockExtend extend lock failed, exiting watchdog: ok:%v err:%v\n", ok, err)
					return
				}
			case <-ctx.Done():
				log.Printf("LockExtend watchdog received done signal, exiting.\n")
				return
			}
		}
	}()

	defer func() {
		cancel()
		if ok, err := mutex.Unlock(); !ok || err != nil {
			log.Printf("LockExtend unlock failed ok:%v,err:%v\n", ok, err)
		}
		if p := recover(); p != nil {
			log.Printf("LockExtend: task panicked: %v\n", p)
			panic(p) // 重新抛出 panic，避免静默吞噬导致调用者无法感知任务崩溃
		}
	}()

	task()
}

// LockExtendGeneric 创建带续租的分布式锁并执行任务并返回任务的返回值泛型函数
func LockExtendGeneric[T any](c *Client, lockKey string, expiry time.Duration, task func() (T, error), timeouts ...time.Duration) (res T, err error) {
	if expiry < 1 {
		expiry = 10 * time.Second
	}

	var timeout = time.Minute * 5
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}

	mutex := c.rs.NewMutex(lockKey, redsync.WithExpiry(expiry))
	if err = mutex.Lock(); err != nil {
		log.Printf("LockExtend lock lockKey:%s err: %v\n", lockKey, err)
		return res, fmt.Errorf("failed to acquire lock: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	go func() {
		ticker := time.NewTicker(expiry / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ok, err := mutex.Extend()
				if !ok || err != nil {
					log.Printf("LockExtend extend lock failed, exiting watchdog: ok:%v err:%v\n", ok, err)
					return
				}
			case <-ctx.Done():
				log.Printf("LockExtend watchdog received done signal, exiting.\n")
				return
			}
		}
	}()

	defer func() {
		cancel()
		if ok, err := mutex.Unlock(); !ok || err != nil {
			log.Printf("LockExtend unlock failed ok:%v,err:%v\n", ok, err)
		}
		if p := recover(); p != nil {
			log.Printf("LockExtendGeneric: task panicked: %v\n", p)
			panic(p) // 重新抛出 panic，避免静默吞噬
		}
	}()

	res, err = task()
	return res, err
}

// LockAwaitOnce 创建带续租的分布式锁并执行任务，等待执行，一直循环获取锁直到获得成功
func (c *Client) LockAwaitOnce(lockKey string, expiry time.Duration, task func(), clear ...func()) {
	go func() {
		if expiry < 1 {
			expiry = 10 * time.Second
		}
		mutex := c.rs.NewMutex(lockKey, redsync.WithExpiry(expiry), redsync.WithTries(3), redsync.WithRetryDelay(time.Millisecond*50))
		for {
			if err := mutex.Lock(); err != nil {
				time.Sleep(expiry / 2)
				continue
			}

			log.Printf("LockAwaitOnce mutex.Lock() 得到锁 time:%v", time.Now().Format(time.RFC3339Nano))
			renewalFailed := make(chan struct{})

			go func() {
				select {
				case <-renewalFailed:
					if len(clear) > 0 {
						clear[0]()
					}
					if unlockOk, unlockErr := mutex.Unlock(); !unlockOk || unlockErr != nil {
						log.Printf("LockAwaitOnce Failed to unlock after extend failure: ok:%v err:%v time:%v", unlockOk, unlockErr, time.Now().Format(time.RFC3339Nano))
					}
					return
				}
			}()

			go func() {
				ticker := time.NewTicker(expiry / 2)
				defer ticker.Stop()

				for range ticker.C {
					ok, err := mutex.Extend()
					if !ok || err != nil {
						log.Printf("LockAwaitOnce Failed to extend lock: ok:%v err:%v time:%v", ok, err, time.Now().Format(time.RFC3339Nano))
						close(renewalFailed)
						return
					}
				}
			}()

			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("LockAwaitOnce Task panic recovered: %v, key: %s time:%v", r, lockKey, time.Now().Format(time.RFC3339Nano))
						close(renewalFailed)
						panic(r) // 重新抛出 panic，避免静默吞噬
					}
				}()
				task()
			}()
		}
	}()
}
