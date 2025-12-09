package gopool

import (
	"github.com/codeedge/go-lib/lib/exit"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

var (
	_poolSafe    *ants.Pool
	onePoolSafe  sync.Once
	poolSafeWG   sync.WaitGroup // 跟踪所有通过Submit提交的任务
	shuttingDown atomic.Bool    // 优雅关闭状态标志
)

func poolSafe(sizes ...int) *ants.Pool {
	size := maxPoolSize
	if len(sizes) > 0 {
		size = sizes[0]
	}
	onePoolSafe.Do(func() {
		var options = ants.Options{
			ExpiryDuration:   time.Minute,
			Nonblocking:      false,
			MaxBlockingTasks: 0,
			PreAlloc:         false,
			PanicHandler: func(e interface{}) {
				log.Printf("panic recovered: %v\n 调用栈: %s", e, debug.Stack())
			},
		}
		var err error
		_poolSafe, err = ants.NewPool(size, ants.WithOptions(options))
		if err != nil {
			log.Printf("New Pool Error: %v", err)
		}

		// 注册全局优雅退出处理
		exit.WG.Add(1)
		go gracefulShutdown()
	})
	return _poolSafe
}

// SubmitSafe 提交任务到协程池（修复版本）
func SubmitSafe(task func()) error {
	// 如果正在关闭，拒绝新任务
	if shuttingDown.Load() {
		return ants.ErrPoolClosed
	}

	// 增加等待组计数
	poolSafeWG.Add(1)

	// 包装任务函数，确保资源清理
	wrappedTask := func() {
		defer poolSafeWG.Done()
		task()
	}

	// 提交到ants池
	return poolSafe().Submit(wrappedTask)
}

// gracefulShutdown 优雅关闭处理
func gracefulShutdown() {
	defer exit.WG.Done()      // 步骤1：通知全局组，本协调员任务已结束
	<-exit.StopContext.Done() // 阻塞，直到收到停止信号

	log.Println("协程池开始优雅关闭...")

	shuttingDown.Store(true) // 步骤2：阻止新任务提交

	// 步骤3：等待所有已提交任务完成 (依赖内部组 poolSafeWG)
	done := make(chan struct{})
	go func() {
		poolSafeWG.Wait()
		close(done)
	}()

	// 步骤4：带超时等待
	select {
	case <-done:
		log.Println("所有协程池任务已完成")
	case <-time.After(30 * time.Second):
		log.Println("协程池任务等待超时，强制退出")
	}

	// 步骤5：释放ants池资源
	poolSafe().Release()
	log.Println("协程池已关闭")
}

// IsShuttingDown 可选：提供检查函数
func IsShuttingDown() bool {
	return shuttingDown.Load()
}

// SubmitBatch 可选：批量提交任务
func SubmitBatch(tasks []func()) error {
	if shuttingDown.Load() {
		return ants.ErrPoolClosed
	}

	for _, task := range tasks {
		if err := SubmitSafe(task); err != nil {
			return err
		}
	}
	return nil
}
