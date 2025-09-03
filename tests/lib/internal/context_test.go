package internal

import (
	"context"
	"errors"
	"fmt"
	"github.com/codeedge/go-lib/lib/exit"
	"github.com/codeedge/go-lib/lib/logs"
	"net/http"
	"testing"
	"time"
)

/*
在 Go 语言中，使用 context 和直接 close(channel) 都是控制 goroutine 退出的有效手段，但它们的设计哲学、适用场景和实现机制有显著区别。
下面这个表格汇总了它们的核心差异，方便你快速了解：

特性对比	Context										close(channel)
设计目的	传递取消信号、超时、截止时间和请求范围的值 			协程间数据传递与同步
通知机制	通过关闭 Done() 返回的 channel 进行广播			通过关闭 channel 进行广播
层级传递	支持，父 Context 取消会导致所有子 Context 取消 	需手动实现，无内置层级关系
超时控制	原生支持 (WithTimeout, WithDeadline) 		需结合 time.After 等自行实现
数据传递	仅适合传递请求范围的元数据 (如 request ID) 		可传递任意类型的业务数据
复杂度与推荐场景	适用于复杂调用树和网络请求等生命周期管理 	适用于内部协程间的简单同步或通知

*/

/*
	下面是两种方法的具体代码示例和关键说明。

🔀 使用 Context 退出 Select
context 包专为管理 goroutine 的生命周期设计，尤其在需要传递取消信号、超时或截止时间的场景中非常有用。
核心优点：
广播机制：一旦父 Context 被取消，所有通过该 Context 或由其衍生的子 Context 监听 ctx.Done() 的 goroutine 都会收到信号并安全退出。这是 层级式的广播。
内置超时控制：可以很方便地使用 context.WithTimeout 或 context.WithDeadline 来设置超时。
传递附加值：可以通过 Context 在调用链中传递一些请求范围的元数据（如请求 ID、认证令牌）
*/
func Test_00(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go worker00(ctx)

	time.Sleep(2 * time.Second)
	cancel() // 发送取消信号

	time.Sleep(100 * time.Millisecond) // 给worker一点时间退出
}

func worker00(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // 监听取消信号
			fmt.Printf("Worker退出: %v\n", ctx.Err())
			return
		case <-time.After(500 * time.Millisecond):
			fmt.Println("Working...")
		}
	}
}

/*
使用 Close Channel 退出 Select
直接关闭 channel 来实现退出，是一种更轻量、更直接的广播机制，常用于 goroutine 内部的协调。
✅ 核心优点：
简单直接的广播：关闭 channel 会使所有阻塞在接收该 channel 的 goroutine 立即被唤醒并接收到零值，从而继续执行退出逻辑。这是一种高效的广播通知机制。
零内存开销：使用空结构体 chan struct{} 作为信号 channel，除了 channel 本身的管理开销外，几乎不占用额外内存

⚠️ 注意事项
1.
不要重复关闭 Channel：重复关闭一个已关闭的 channel 会引起 panic。确保关闭操作只执行一次，通常可使用 sync.Once 来保护。
2.
Context 仅用于取消和超时：应避免使用 Context 的 Value 功能来传递函数可选参数或控制流程，它主要用于传递请求范围的元数据（如请求ID、认证令牌等）。
3.
谁创建，谁关闭：channel 的关闭职责最好由它的创建者或发送者来承担，接收者通常不应关闭 channel。
🎯 如何选择
选择哪种方式，取决于你的具体需求：
选择 context 的场景：
需要超时或截止时间控制（例如，HTTP请求、数据库查询）。
操作存在于多层函数调用或goroutine调用树中，需要将取消信号层层传递下去。
需要传递一些请求范围的元数据（如链路追踪ID）。

选择 close(channel) 的场景：
内部协调：在单个组件或一组紧密相关的 goroutine 内部进行简单的生命周期同步或退出协调。
广播通知：需要同时通知多个等待的 goroutine 退出或停止工作。
希望实现方式尽可能简单和轻量。
有时，两者也可以结合使用。例如，在一个大型应用中，使用 Context 来管理整个请求的超时和取消，同时在某个独立的模块内部使用 channel 来同步其内部 goroutine 的状态。
*/

func Test_01(t *testing.T) {
	stopChan := make(chan struct{})
	go worker01(stopChan)

	time.Sleep(2 * time.Second)
	close(stopChan) // 关闭channel以广播退出信号

	time.Sleep(100 * time.Millisecond)
}

func worker01(stopChan chan struct{}) {
	for {
		select {
		case <-stopChan: // 感知channel关闭
			fmt.Println("Worker退出")
			return
		case <-time.After(500 * time.Millisecond):
			fmt.Println("Working...")
		}
	}
}

/*
你的案例很好 能不能在此基础上给我context更多类型的使用方法 比如信号量、超时、取消、截止时间、子context、广播、数据传送这几种场景的使用方法
好的，我会在之前的基础上，为你详细解释 Go Context 更多类型的使用方法。通过一个结构化的表格和详细的代码示例，帮助你全面掌握 Context 的各种应用场景。

下面这个表格汇总了 Context 的主要使用场景和核心方法，方便你快速了解：

场景					核心方法/机制					关键特点												推荐使用场景
取消信号				WithCancel			手动控制取消，通过调用 cancel() 函数发出信号 						需要主动终止一系列操作的场景，如用户中断任务、清理资源等
超时控制				WithTimeout			基于持续时间自动触发取消 										网络请求、数据库查询等需要在固定时间内完成的操作
截止时间				WithDeadline		基于具体时间点自动触发取消 										任务必须在某个特定时间点之前完成，例如定时任务调度
数据传递				WithValue			在上下文链中传递请求范围的元数据（如 trace ID、用户认证信息）			传递链路追踪ID、用户身份等需要在多个函数或goroutine间共享的数据
层级传播（子Context）	所有 WithXXX 函数	取消信号自动从父Context向所有子Context广播 						构建具有层级关系的任务树，实现级联取消
广播机制				ctx.Done() Channel	多个goroutine 监听同一个 ctx.Done() 通道，实现一对多的协同停止 	需要同时通知多个goroutine退出的场景
下面是针对这些场景的具体代码示例和说明。

*/

/*

🔀 1. 取消信号 (WithCancel)
context.WithCancel 用于创建一个可手动取消的上下文。它返回一个上下文对象和一个取消函数。调用该取消函数会关闭上下文对象的 Done 通道，
从而通知所有监听该上下文的操作停止执行 。
*/

func Test_1(t *testing.T) {
	// 创建一个可取消的 Context
	ctx, cancel := context.WithCancel(context.Background())

	// 启动多个 worker goroutine
	for i := 1; i <= 3; i++ {
		go worker1(ctx, i)
	}

	// 主程序等待一段时间
	time.Sleep(3 * time.Second)
	fmt.Println("主程序: 发出取消信号")
	cancel() // 调用 cancel 函数，取消所有操作

	// 给 goroutine 一些时间来响应取消信号
	time.Sleep(1 * time.Second)
	fmt.Println("主程序: 退出")
}

func worker1(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done(): // 监听取消信号
			fmt.Printf("Worker %d: 收到取消信号，退出\n", id)
			return
		default:
			fmt.Printf("Worker %d: 工作中...\n", id)
			time.Sleep(1 * time.Second)
		}
	}
}

/*
⏰ 2. 超时控制 (WithTimeout)
context.WithTimeout 用于创建一个具有超时机制的上下文。它接受一个父上下文和一个超时时间（Duration）。如果超过指定时间操作仍未完成，该上下文会自动取消 。
*/

func Test_2(t *testing.T) {
	// 创建一个超时时间为 2 秒的 Context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel() // 良好的实践是使用 defer 来确保资源释放

	// 模拟一个可能耗时的 HTTP 请求
	req, err := http.NewRequestWithContext(ctx, "GET", "https://httpbin.org/delay/3", nil)
	if err != nil {
		fmt.Println("创建请求失败:", err)
		return
	}

	client := &http.Client{}
	fmt.Println("发起请求...")
	resp, err := client.Do(req)

	if err != nil {
		// 检查错误是否是因为超时
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			fmt.Println("请求超时:", err)
		} else {
			fmt.Println("其他错误:", err)
		}
		return
	}
	defer resp.Body.Close()

	fmt.Println("请求成功! 状态码:", resp.StatusCode)
}

/*
📅 3. 截止时间 (WithDeadline)
context.WithDeadline 与 WithTimeout 类似，但它接受一个具体的截止时间点（Time），而不是一个持续时间。到达这个时间点后，上下文会自动取消 。
*/
func Test_3(t *testing.T) {
	// 设置一个具体的截止时间（当前时间后 5 秒）
	deadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	select {
	case <-time.After(6 * time.Second): // 模拟一个耗时 6 秒的操作
		fmt.Println("操作完成")
	case <-ctx.Done():
		fmt.Println("操作已截止:", ctx.Err()) // 输出: context deadline exceeded
	}
}

/*
📦 4. 数据传递 (WithValue)
context.WithValue 允许你在上下文链中存储和传递键值对数据。这常用于传递请求范围（request-scoped）的数据，如跟踪ID、用户身份信息等
*/
func Test_4(t *testing.T) {
	// 创建基础 Context
	ctx := context.Background()

	// 使用 WithValue 向 Context 链中存储多个值
	ctx = context.WithValue(ctx, requestIDKey, "req-12345")
	ctx = context.WithValue(ctx, userTokenKey, "abcde-secure-token")

	processRequest(ctx)
}

// 定义自定义类型作为 key，避免与其他包可能存入的 key 冲突
type contextKey string

const (
	requestIDKey contextKey = "requestID"
	userTokenKey contextKey = "userToken"
)

func processRequest(ctx context.Context) {
	// 从 Context 中提取值
	if reqID, ok := ctx.Value(requestIDKey).(string); ok {
		fmt.Println("请求ID:", reqID)
	} else {
		fmt.Println("请求ID 不存在")
	}

	if token, ok := ctx.Value(userTokenKey).(string); ok {
		fmt.Println("用户Token:", token)
	} else {
		fmt.Println("用户Token 不存在")
	}
}

/*
🌲 5. 子Context与广播机制
Context 的一个重要特性是层级关系和广播机制。当你从一个父 Context 派生出子 Context（使用 WithCancel, WithTimeout, WithDeadline, WithValue）时，
父 Context 的取消会导致所有派生出的子 Context 也被取消。这使得取消信号可以广播到整个操作树 。
*/
func Test_5(t *testing.T) {
	// 创建根 Context
	rootCtx, rootCancel := context.WithCancel(context.Background())

	// 从 rootCtx 派生两个子 Context
	childCtx1, _ := context.WithCancel(rootCtx) // 注意：这里也返回了 cancel 函数，但未使用
	childCtx2, _ := context.WithCancel(rootCtx)

	// 启动多个监控协程
	go watch(childCtx1, "子协程1")
	go watch(childCtx2, "子协程2")

	// 主程序等待一段时间
	time.Sleep(3 * time.Second)
	fmt.Println("主程序: 取消根 Context")
	rootCancel() // 取消根 Context，信号会广播到所有子 Context

	// 给 goroutine 一些时间来响应
	time.Sleep(1 * time.Second)
}
func watch(ctx context.Context, name string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("%s 收到取消信号: %v\n", name, ctx.Err())
			return
		default:
			fmt.Printf("%s 监控中...\n", name)
			time.Sleep(1 * time.Second)
		}
	}
}

/*
🔄 6. 与信号量模式结合实现优雅退出
*/
func Test_6(t *testing.T) {
	// 先初始化
	exit.ListenExit(func() {
		// 自定义退出逻辑
		logs.Info("执行自定义清理操作...")
	}, 1*time.Second)

	// 具体任务使用 先add 然后在协程处理逻辑
	exit.WG.Add(1)
	go func() {
		defer exit.WG.Done() // 通知全局退出组
		<-exit.StopContext.Done()
		// 具体任务处理逻辑
		logs.Info("具体任务处理逻辑...")
	}()
}

/*
💡 关键总结与最佳实践
1.Context 是树形结构：取消信号从父节点广播到所有子节点，这是其最强大的特性之一 。
2.传递规则：Context 应作为函数的第一个参数显式传递，不要将其存储在结构体中 。
3.超时与截止时间：WithTimeout 和 WithDeadline 返回的 Context，即使自动超时，也应调用 cancel 函数以释放相关资源（通常用 defer cancel()）。
4.数据传递：WithValue 应仅用于传递请求范围的进程（如请求ID、用户认证令牌），而不应传递函数可选参数或程序的关键参数。Key 最好使用自定义类型以避免冲突 。
5.监听取消：在可能长时间运行或循环的操作中，应定期检查 ctx.Done()，以确保能及时响应取消信号。
6.错误处理：操作被取消后，可通过 ctx.Err() 获取错误原因，通常是 context.Canceled 或 context.DeadlineExceeded 。
希望这些更详细的场景和代码示例能帮助你更好地理解和使用 Go Context。
*/
