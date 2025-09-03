package exit

import (
	"context"
	"github.com/codeedge/go-lib/lib/gopool"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	StopContext context.Context
	stop        context.CancelFunc
	Stopping    atomic.Bool
	WG          sync.WaitGroup // 跟踪所有 safeExit 协程
)

// ListenExit 监听退出信号 需要使用select{}等方式阻塞main函数的退出
func ListenExit(callback func(), delay time.Duration) {
	StopContext, stop = signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	_ = gopool.Pool().Submit(func() {
		<-StopContext.Done()
		Stopping.Store(true)

		// 第一步：关闭HTTP服务器（停止接收新请求）
		if httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// shutdown后http服务的的s.ListenAndServe()会退出导致整个程序退出，需要在s.ListenAndServe()后加select {}阻塞主线程
			if err := httpServer.Shutdown(ctx); err != nil {
				log.Printf("HTTP服务器关闭错误: %v", err)
			}
		}

		// 第二步：执行自定义回调
		log.Printf("收到退出指令，程序将在%v秒后退出...", delay.Seconds())
		if callback != nil {
			callback()
		}

		// 第三步：等待剩余任务
		WG.Wait() // 等待所有 safeExit 协程完成
		time.Sleep(delay)

		log.Printf("程序已退出！")

		os.Exit(0)
	})
}

func StopExit() {
	stop()
}

var (
	httpServer *http.Server // 需要保存服务器实例
)

func RegisterHTTPServer(srv *http.Server) {
	httpServer = srv
}
