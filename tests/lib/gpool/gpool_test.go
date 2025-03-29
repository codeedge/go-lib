package gpool

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/codeedge/go-lib/lib/gpool"
)

func Test_gpool_01(t *testing.T) {
	pool, err := gpool.NewPool(10)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 20; i++ {
		pool.Put(&gpool.Task{
			Handler: func(v ...interface{}) {
				fmt.Println(v)
			},
			Params: []interface{}{i},
		})

	}

	time.Sleep(1e9)
}

func Test_gpool_02(t *testing.T) {
	for i := 0; i < 20; i++ {
		gpool.AddTask(&gpool.Task{
			Handler: func(v ...interface{}) {
				fmt.Println(v)
			},
			Params: []interface{}{i},
		})
	}
	time.Sleep(1e9)
}

type TestPool struct {
	index int
}

func Test_gpool_03(t *testing.T) {
	testPools := make([]*TestPool, 20)
	for i := 0; i < 20; i++ {
		testPools[i] = &TestPool{}
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		gpool.AddTaskByFunc(func(v ...interface{}) {
			testPool := v[0].(*TestPool)
			testPool.index = v[1].(int)
			fmt.Println(testPool)
			defer wg.Done()
		}, testPools[i], i)
	}

	wg.Wait()

	for i := 0; i < 20; i++ {
		fmt.Println(testPools[i])
	}

}

func Test_gpool_04(t *testing.T) {
	gpool.AddTask(&gpool.Task{
		Handler: func(v ...interface{}) {
			fmt.Println(111)
		},
	})
	gpool.AddTaskByHandlerName("test", &gpool.Task{
		Handler: func(v ...interface{}) {
			fmt.Println(111)
		},
	})
	gpool.AddTaskByFunc(func(args ...interface{}) {
		fmt.Println(111)
	})
	gpool.AddFuncByHandlerName("test", func(args ...interface{}) {
		fmt.Println(111)
	})
	// 指定name和limit初始化test2 pool
	p := gpool.GetPool("test2", 100)
	// 直接使用
	p.Put(&gpool.Task{
		Handler: func(v ...interface{}) {
			fmt.Println(v[0].(int))
		},
		Params: []interface{}{1},
	})
	// 使用已经初始化的test2 pool
	gpool.AddFuncByHandlerName("test2", func(args ...interface{}) {
		fmt.Println(1)
	})

	gpool.ClosePool()
	gpool.ClosePool("test")
	gpool.ClosePool("test2")
}
