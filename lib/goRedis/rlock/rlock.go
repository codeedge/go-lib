package rlock

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/gogf/guuid"
	"github.com/kdcer/go-lib/lib/goRedis"
)

type RdsLock struct {
	key        string // redis的键
	value      string // 随机value 防止其他协程删除本协程的锁
	timeoutSec int64  // 超时时间(秒)
	done       uint32 // 标记
}

// New 创建锁
func New(lockKey string, timeoutSec int64) (*RdsLock, error) {
	if lockKey == "" || timeoutSec <= 0 {
		return nil, errors.New("key or timeoutSec is not valid")
	}
	return &RdsLock{
		key:        lockKey,
		value:      guuid.New().String(),
		timeoutSec: timeoutSec,
	}, nil
}

// Lock 加锁
// 适合启动会执行多次的一般程序防止并发，比如定时任务的每一个定时任务内部间隔执行的场景，即使程序异常退出key没有释放，其他程序会一直轮询执行总会获得锁的
func (lock *RdsLock) Lock() bool {
	b := goRedis.Rdb.SetNX(context.Background(), lock.key, lock.value, time.Second*time.Duration(lock.timeoutSec)).Val()
	return b
}

// Unlock 解锁
func (lock *RdsLock) Unlock() (err error) {
	_, err = goRedis.CheckAndDel(lock.key, lock.value)
	return
}

// SetTimeout 设置新的超时时间
func (lock *RdsLock) SetTimeout(exTime ...int64) error {
	var expireTime int64
	if len(exTime) == 0 {
		expireTime = lock.timeoutSec
	} else {
		expireTime = exTime[0]
	}
	err := goRedis.Rdb.Expire(context.Background(), lock.key, time.Second*time.Duration(expireTime)).Err()
	return err
}

// LockAwaitOnce 等待执行 一直循环获取锁直到获得成功
// 每隔一段时间获取1次锁 获取成功则执行任务
// 如果是3台集群 A获得了锁然后挂了，B和C会都有可能获得锁，不会发生没有机器跑任务的情况
// 适合启动只会执行1次的初始化程序防止并发启动，比如初始化定时任务，防止程序异常退出没有执行删除命令，此时锁还没有释放，其他并发程序初始化无法进入执行，通过等待来实现进入
// 注意等待可能会影响其他程序的启动，如果会阻塞主程序启动需要把 LockAwait 所在的程序在后台以goroutine的形式启动
// 如果想均衡利用各个机器的资源执行任务需要在每个定时任务中使用Lock UnLock，这样也会使redis的压力变大
func (lock *RdsLock) LockAwaitOnce(task func()) {
	fn := func() {
		if !atomic.CompareAndSwapUint32(&lock.done, 1, 1) {
			// 获取锁成功则设置down为1,设置过期时间为2倍防止其他机器获得锁,然后运行任务
			if lock.Lock() {
				atomic.StoreUint32(&lock.done, 1)
				lock.SetTimeout(lock.timeoutSec * 2)
				task()
			}
		} else { // 运行了之后会设置过期时间为2倍 防止其他机器获得锁
			lock.SetTimeout(lock.timeoutSec * 2)
		}
	}
	// 先执行一遍再定时执行,利于需要立即执行的任务执行
	go fn()
	ticker := time.NewTicker(time.Second * time.Duration(lock.timeoutSec))
	for range ticker.C {
		// 如果没有运行过则尝试获取锁
		// 如果任务执行很久,在下一次循环开始还没有执行完会阻塞else续期的执行,所以使用goroutine,lock.done也需要原子操作
		go fn()
	}
}
