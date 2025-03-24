package gredis

import (
	"fmt"
	"testing"
	"time"

	"github.com/kdcer/go-lib/lib/goRedis/rlock"
)

// redis需要初始化 仅作演示用
func Test_Lock(t *testing.T) {
	lockKey := "key"
	rdsLock, err := rlock.New(lockKey, 10)
	if err != nil {
		return
	}
	for {
		func() {
			time.Sleep(time.Second * 5)
			if rdsLock.Lock() {
				fmt.Println(666)
			}
			defer rdsLock.Unlock()
		}()
	}
}

// redis需要初始化 仅作演示用
func Test_LockAwaitOnce(t *testing.T) {
	go func() {
		lockKey := "key"
		rdsLock, err := rlock.New(lockKey, 10)
		if err != nil {
			return
		}
		rdsLock.LockAwaitOnce(func() {
			fmt.Println(666)
		})
	}()
}

func Test_1(t *testing.T) {
	lock, err := rlock.New("lockKey", 10)
	if err != nil {
		panic(err)
	}
	lock.Lock()
	lock.Lock()
	lock.SetTimeout(20)
	lock.Unlock()
	lock.Lock()
	lock.LockAwaitOnce(func() {
		fmt.Println(111)
	})
}
