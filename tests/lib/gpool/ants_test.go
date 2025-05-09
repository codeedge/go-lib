package gpool

import (
	"fmt"
	"github.com/panjf2000/ants/v2"
	"runtime/debug"
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

func Test_Ants3(t *testing.T) {
	// 第三种 使用自定义池
	_ = Pool().Submit(func() {
		fmt.Println("Hello World!")
	})

	// 预先分配 goroutine 队列内存
	// ants 支持预先为 pool 分配容量的内存， 这个功能可以在某些特定的场景下提高 goroutine 池的性能。比如， 有一个场景需要一个超大容量的池，而且每个 goroutine 里面的任务都是耗时任务，
	// 这种情况下，预先分配 goroutine 队列内存将会减少不必要的内存重新分配。
	// 提前分配的 pool 容量的内存空间
	pool, _ := ants.NewPool(100000, ants.WithPreAlloc(true))

	// 动态调整 goroutine 池容量
	// 需要动态调整 pool 容量可以通过调用 ants.Tune：	该方法是线程安全的。
	pool.Tune(1000)   // Tune its capacity to 1000
	pool.Tune(100000) // Tune its capacity to 100000

	//释放 Pool
	pool.Release()
	//或者
	pool.ReleaseTimeout(time.Second * 3)

	// 重启 Pool
	// 只要调用 Reboot() 方法，就可以重新激活一个之前已经被销毁掉的池，并且投入使用。
	pool.Reboot()
}

const (
	MaxSessions = 102400
)

var (
	pool    *ants.Pool
	onePool sync.Once
)

func Pool() *ants.Pool {
	onePool.Do(func() {
		var options = ants.Options{
			// ExpiryDuration 是清理过期 worker 的时间周期。清理协程（scavenger）会每隔 `ExpiryDuration` 扫描一次所有 worker，并清理那些超过 `ExpiryDuration` 未被使用的 worker。
			ExpiryDuration: time.Minute,
			// Nonblocking 为 true 时，Pool.Submit 永远不会阻塞。如果无法立即提交任务，会直接返回 ErrPoolOverload 错误。当 Nonblocking 为 true 时，MaxBlockingTasks 配置无效。
			Nonblocking: false,
			// MaxBlockingTasks 是 pool.Submit 方法的最大阻塞任务数。默认值 0 表示无限制。当阻塞的任务数达到此值时，提交新任务会返回错误。
			MaxBlockingTasks: 0,
			// PreAlloc 表示在初始化 Pool 时是否预分配内存。如果为 true，会提前分配内存以优化性能。
			PreAlloc: false,
			// PanicHandler 用于处理 worker 协程中的 panic。如果为 nil，panic 会从 worker 协程中直接抛出（可能导致进程崩溃）。
			PanicHandler: func(e interface{}) {
				fmt.Printf("%v\n%s", e, debug.Stack())
			},
			// Logger 是自定义的日志记录器，用于输出日志。如果未设置，默认使用标准库的 `log` 包。
			Logger: nil,
			// DisablePurge 为 true 时，worker 不会被清理，而是常驻内存。禁用清理功能后，ExpiryDuration 配置将无效。
			DisablePurge: false,
		}
		var err error
		pool, err = ants.NewPool(MaxSessions, ants.WithOptions(options))
		if err != nil {
			fmt.Printf("new pool error: %v", err)
		}
	})
	return pool
}
