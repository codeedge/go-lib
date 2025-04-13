package gpool

import (
	"fmt"
	"github.com/gogf/gf/v2/os/gmlock"
)

var defaultPoolLimitSize uint64 = 100
var defaultPoolHandlerName = "defaultPoolHandlerName"

// 协程池
var poolMap = make(map[string]*Pool, 0)

func GetPool(handlerName string, poolLimitSize ...uint64) *Pool {
	var limit uint64
	if len(poolLimitSize) > 0 {
		limit = poolLimitSize[0]
	} else {
		limit = defaultPoolLimitSize
	}
	gmlock.Lock(handlerName)
	defer gmlock.Unlock(handlerName)
	if p, ok := poolMap[handlerName]; ok {
		return p
	} else {
		pool, err := NewPool(limit)
		if err != nil {
			fmt.Println("初始化gpool.Pool失败 poolLimitSize=", limit)
			panic(err)
		}
		poolMap[handlerName] = pool
		return pool
	}
}

func GetDefaultPool(poolLimitSize ...uint64) *Pool {
	return GetPool(defaultPoolHandlerName, poolLimitSize...)
}

// AddTask 添加后台任务
func AddTask(task *Task) {
	GetDefaultPool().Put(task)
}

// AddTaskByHandlerName 根据函数名添加后台任务
func AddTaskByHandlerName(handlerName string, task *Task) {
	GetPool(handlerName).Put(task)
}

// AddTaskByFunc 添加后台任务
func AddTaskByFunc(handler func(args ...interface{}), args ...interface{}) {
	GetDefaultPool().Put(&Task{
		Handler: handler,
		Params:  args,
	})
}

// AddFuncByHandlerName 根据函数名添加后台任务
func AddFuncByHandlerName(handlerName string, handler func(args ...interface{}), args ...interface{}) {
	GetPool(handlerName).Put(&Task{
		Handler: handler,
		Params:  args,
	})
}

// ClosePool 关闭协程池
func ClosePool(handlerName ...string) {
	var _handlerName string
	if len(handlerName) == 0 {
		_handlerName = defaultPoolHandlerName
	} else {
		_handlerName = handlerName[0]
	}
	// lockKey不可以和GetPool的一样
	lockKey := "close" + _handlerName
	gmlock.Lock(lockKey)
	defer gmlock.Unlock(lockKey)
	GetPool(_handlerName).Close()
	delete(poolMap, _handlerName)
}
