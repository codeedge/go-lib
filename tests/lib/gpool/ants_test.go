package gpool

import (
	"fmt"
	"github.com/panjf2000/ants/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var sum int32

// 只能接受一个 interface{}参数，需要可以放入任何类型
func myFunc(i interface{}) {
	n := i.(int32)
	atomic.AddInt32(&sum, n)
	fmt.Printf("run with %d\n", n)
}

func demoFunc() {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Hello World!")
}

func Test_Ants(t *testing.T) {
	defer ants.Release()

	runTimes := 1000

	//第一种 使用公共池  池的容量是 DefaultAntsPoolSize = math.MaxInt32 , DefaultAntsPoolSize是默认goroutine池的默认容量。
	// Use the common pool.
	var wg sync.WaitGroup
	syncCalculateSum := func() {
		demoFunc()
		wg.Done()
	}
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		//go syncCalculateSum()//等价于在此goroutine
		_ = ants.Submit(syncCalculateSum)
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", ants.Running())
	fmt.Printf("finish all tasks.\n")
}

func Test_Ants2(t *testing.T) {
	runTimes := 1000
	var wg sync.WaitGroup

	//第二种 使用方法池
	// Use the pool with a method,
	// set 10 to the capacity of goroutine pool and 1 second for expired duration.
	p, _ := ants.NewPoolWithFunc(10, func(i interface{}) {
		myFunc(i) // 注意：这里的变量必须传递进来，不能像go func(){}一样直接引用外部的，否则会无法进入函数
		wg.Done()
	})
	defer p.Release()
	// Submit tasks one by one.
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = p.Invoke(int32(i)) // Invoke向池提交一个任务。
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", p.Running()) // Running返回当前运行的goroutine的数量。
	fmt.Printf("finish all tasks, result is %d\n", sum)
	if sum != 499500 {
		panic("the final result is wrong!!!")
	}
}
