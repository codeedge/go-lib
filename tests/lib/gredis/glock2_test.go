package gredis

import (
	"fmt"
	"github.com/kdcer/go-lib/lib/gredis/glock"
	"testing"
	"time"
)

// redis需要初始化 仅作演示用
func Test_Lock(t *testing.T) {
	lockKey := "key"
	rdsLock, err := glock.New(lockKey, 10)
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
		rdsLock, err := glock.New(lockKey, 10)
		if err != nil {
			return
		}
		rdsLock.LockAwaitOnce(func() {
			fmt.Println(666)
		})
	}()
}
