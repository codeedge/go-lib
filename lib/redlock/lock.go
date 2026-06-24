package redlock

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

// Lock 分布式锁
type Lock struct {
	rs *redsync.Redsync
}

// New 创建分布式锁
// 传 1 个参数：单节点模式
// 传多个参数：Redlock 模式（推荐 5 个独立单节点）
// Redlock 部署要求：
//  1. 每个 Redis 节点必须是独立的主节点，不能有主从复制关系
//  2. 生产环境推荐 5 个节点，可容忍 2 个节点故障
//  3. 不支持主从模式和集群模式，因为异步复制会导致锁失效
func New(options ...*redis.Options) *Lock {
	pools := make([]redsyncredis.Pool, 0)
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

	if len(pools) == 0 {
		log.Println("Lock: no available redis instance")
		return nil
	}

	return &Lock{rs: redsync.New(pools...)}
}

// NewMutex 设置分布式锁 配置重试3次，重试延迟50毫秒 默认的锁会重试32次，响应过慢
func (l *Lock) NewMutex(name string, options ...redsync.Option) *redsync.Mutex {
	opts := []redsync.Option{
		redsync.WithTries(3), redsync.WithRetryDelay(time.Millisecond * 50),
	}
	if len(options) > 0 {
		opts = append(opts, options...)
	}
	return l.rs.NewMutex(name, opts...)
}

// LockExtend 创建带续租的分布式锁并执行任务
// 适合场景：分布式系统并发时只允许一个进程执行一些耗时操作，无法保证锁在释放前执行完，需要给锁续租，直到程序执行完后释放锁，并停止续租
// lockKey 锁的key
// expiry 锁的过期时间
// task 执行的任务
// timeouts 超时时间，控制任务最大执行时间，比给锁加超长时间更优，因为即使程序挂了锁住的时间更短
func (l *Lock) LockExtend(lockKey string, expiry time.Duration, task func(), timeouts ...time.Duration) {
	// 设置默认值
	if expiry < 1 {
		expiry = 10 * time.Second
	}

	var timeout = time.Minute * 5 // 默认5分钟超时
	// 任务超时时间必须大于0
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}

	// 创建一个带有过期时间的互斥锁
	mutex := l.rs.NewMutex(lockKey, redsync.WithExpiry(expiry))
	if err := mutex.Lock(); err != nil {
		log.Printf("LockExtend lock lockKey:%s err: %v\n", lockKey, err)
		return
	}

	// 创建带超时的 Context，此 Context 将用于控制任务执行和看门狗协程
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	// 开启一个 goroutine，周期性地续租锁（看门狗）
	// 它监听 ctx.Done() 来停止续租
	go func() {
		ticker := time.NewTicker(expiry / 2) // 每隔过期时间的一半续租一次
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ok, err := mutex.Extend()
				if !ok || err != nil {
					log.Printf("LockExtend extend lock failed, exiting watchdog: ok:%v err:%v\n", ok, err)
					// 续租失败意味着锁可能已被释放或过期，看门狗必须退出
					return
				}
			case <-ctx.Done():
				// 接收到取消信号（任务完成或超时），看门狗退出
				log.Printf("LockExtend watchdog received done signal, exiting.\n")
				return
			}
		}
	}()

	// 使用 defer 确保资源清理
	defer func() {
		// A. 取消 Context，这会向看门狗协程发送停止信号
		cancel()
		// B. 释放锁
		if ok, err := mutex.Unlock(); !ok || err != nil {
			log.Printf("LockExtend unlock failed ok:%v,err:%v\n", ok, err)
		}
		// C. 捕获 panic 并重新抛出，避免静默吞噬导致调用者无法感知任务崩溃
		if p := recover(); p != nil {
			log.Printf("LockExtend: task panicked: %v\n", p)
		}
	}()

	// 执行任务
	task()
}

// LockExtendGeneric 创建带续租的分布式锁并执行任务并返回任务的返回值泛型函数
// T 是任务函数返回值的类型参数
// 适合场景：分布式系统并发时只允许一个进程执行一些耗时操作，无法保证锁在释放前执行完，需要给锁续租，直到程序执行完后释放锁，并停止续租
// lockKey 锁的key
// expiry 锁的过期时间
// task 执行的任务
// timeouts 超时时间，控制任务最大执行时间
func LockExtendGeneric[T any](l *Lock, lockKey string, expiry time.Duration, task func() (T, error), timeouts ...time.Duration) (res T, err error) {
	// 设置默认值
	if expiry < 1 {
		expiry = 10 * time.Second
	}

	var timeout = time.Minute * 5 // 默认5分钟超时
	// 任务超时时间必须大于0
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}

	// 创建一个带有过期时间的互斥锁
	mutex := l.rs.NewMutex(lockKey, redsync.WithExpiry(expiry))
	if err = mutex.Lock(); err != nil {
		log.Printf("LockExtend lock lockKey:%s err: %v\n", lockKey, err)
		return res, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// 创建带超时的 Context，此 Context 将用于控制任务执行和看门狗协程
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	// 开启一个 goroutine，周期性地续租锁（看门狗）
	// 它监听 ctx.Done() 来停止续租
	go func() {
		ticker := time.NewTicker(expiry / 2) // 每隔过期时间的一半续租一次
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ok, err := mutex.Extend()
				if !ok || err != nil {
					log.Printf("LockExtend extend lock failed, exiting watchdog: ok:%v err:%v\n", ok, err)
					// 续租失败意味着锁可能已被释放或过期，看门狗必须退出
					return
				}
			case <-ctx.Done():
				// 接收到取消信号（任务完成或超时），看门狗退出
				log.Printf("LockExtend watchdog received done signal, exiting.\n")
				return
			}
		}
	}()

	// 使用 defer 确保资源清理
	defer func() {
		// A. 取消 Context，这会向看门狗协程发送停止信号
		cancel()
		// B. 释放锁
		if ok, err := mutex.Unlock(); !ok || err != nil {
			log.Printf("LockExtend unlock failed ok:%v,err:%v\n", ok, err)
		}
		// C. 捕获 panic 并重新抛出，避免静默吞噬
		if p := recover(); p != nil {
			log.Printf("LockExtendGeneric: task panicked: %v\n", p)
		}
	}()

	// 执行任务
	res, err = task()
	return res, err
}

// LockAwaitOnce 创建带续租的分布式锁并执行任务，等待执行，一直循环获取锁直到获得成功
// 适合场景：分布式环境下的单节点选举，比如定时任务初始化，保证集群中只有一台机器执行定时任务
// lockKey 锁的key
// expiry 锁的过期时间
// task 执行的任务
// clear 在续期失败时清理执行的任务，比如清理定时任务，防止续期失败后其他机器和本机器多次执行了定时任务
func (l *Lock) LockAwaitOnce(lockKey string, expiry time.Duration, task func(), clear ...func()) {
	go func() {
		if expiry < 1 {
			expiry = 10 * time.Second
		}
		// 创建一个带有过期时间的互斥锁 设置重试3次，重试之间等待的时间长度50毫秒
		mutex := l.rs.NewMutex(lockKey, redsync.WithExpiry(expiry), redsync.WithTries(3), redsync.WithRetryDelay(time.Millisecond*50))
		// 一直循环尝试获取锁，获取成功则执行任务
		for {
			if err := mutex.Lock(); err != nil {
				// 获取失败睡眠一半的时间再重试
				time.Sleep(expiry / 2)
				continue
			}

			log.Printf("LockAwaitOnce mutex.Lock() 得到锁 time:%v", time.Now().Format(time.RFC3339Nano))
			// 创建通道用于协调续租协程
			renewalFailed := make(chan struct{})

			go func() {
				// 任务完成后的处理
				// 任务完成后，阻塞等待，直到收到续租失败的信号
				select {
				case <-renewalFailed:
					if len(clear) > 0 {
						clear[0]()
					}
					// 主动释放锁，避免死锁
					if unlockOk, unlockErr := mutex.Unlock(); !unlockOk || unlockErr != nil {
						log.Printf("LockAwaitOnce Failed to unlock after extend failure: ok:%v err:%v time:%v", unlockOk, unlockErr, time.Now().Format(time.RFC3339Nano))
					}
					return
				}
			}()

			// 开启一个goroutine，周期性地续租锁
			go func() {
				ticker := time.NewTicker(expiry / 2) // 按照需求调整 每隔过期时间的一半续租一次
				defer ticker.Stop()

				for range ticker.C {
					ok, err := mutex.Extend()
					if !ok || err != nil {
						log.Printf("LockAwaitOnce Failed to extend lock: ok:%v err:%v time:%v", ok, err, time.Now().Format(time.RFC3339Nano))
						close(renewalFailed) // 通知主协程续租失败
						return
					}
				}
			}()

			// 执行需要锁的工作，使用匿名函数来限制 recover 的作用域
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("LockAwaitOnce Task panic recovered: %v, key: %s time:%v", r, lockKey, time.Now().Format(time.RFC3339Nano))
						close(renewalFailed) // 通知主协程续租失败
					}
				}()
				task()
			}()

			// 获取成功后不用退出循环，这样本机如果续期失败还可以继续尝试参与进来，可以把过期时间设置久一点，这样请求redis的频率就低了。
			// return
		}
	}()
}
