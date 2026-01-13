package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/codeedge/go-lib/lib/exit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig 用于测试的配置
var TestConfig = Config{
	URL:               "amqp://guest:guest@localhost:5672/", // ⚠️ 请确保您的RabbitMQ运行在此地址
	MaxRetries:        3,
	RetryBaseInterval: 1,
	PublisherPoolSize: 2,
}

// Global setup: 在所有测试运行前初始化客户端
func TestMain(m *testing.M) {
	// 1. 初始化 RabbitMQ 客户端
	err := Init(TestConfig, exit.Instance)
	if err != nil {
		fmt.Printf("FATAL: Failed to initialize RabbitMQ client. Is RabbitMQ running? Error: %v\n", err)
		os.Exit(1)
	}

	// 2. 运行所有测试
	code := m.Run()

	// 3. 清理资源
	MQ.Close()

	os.Exit(code)
}

// -----------------------------------------------------------------------------
// 测试辅助函数
// -----------------------------------------------------------------------------

// setupQueueAndExchange 在测试前声明交换机、队列和绑定
func setupQueueAndExchange(t *testing.T, exchangeName, queueName, kind, routingKey string) {
	t.Helper()

	// 1. 声明交换机
	err := MQ.DeclareExchange(ExchangeOption{
		Name:       exchangeName,
		Kind:       kind,
		Durable:    true,
		AutoDelete: false,
	})
	require.NoError(t, err, "DeclareExchange failed")

	// 2. 声明队列
	err = MQ.DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
	})
	require.NoError(t, err, "DeclareQueue failed")

	// 3. 绑定队列
	err = MQ.BindQueue(queueName, routingKey, exchangeName)
	require.NoError(t, err, "BindQueue failed")
}

// -----------------------------------------------------------------------------
// 测试发布方法
// -----------------------------------------------------------------------------

func TestPublishToQueue(t *testing.T) {
	queueName := "test_queue_simple"
	data := map[string]string{"message": "hello simple queue"}

	// 声明队列 (PublishToQueue内部会调用DeclareQueue, 这里先手动确保存在)
	err := MQ.DeclareQueue(&QueueOption{
		Name: queueName, Durable: true,
	})
	require.NoError(t, err, "Setup DeclareQueue failed")

	// 执行发布
	err = MQ.PublishToQueue(context.Background(), queueName, data)
	assert.NoError(t, err, "PublishToQueue failed")

	// 验证消息是否到达 (通过消费验证，后续TestConsumeFromQueue会更详细)
	// 这里的简单验证是：只要发布不报错，就认为成功。
}

func TestPublishToFanout(t *testing.T) {
	exchangeName := "test_exchange_fanout"
	data := map[string]string{"message": "hello fanout"}

	// 设置 Fanout 交换机
	err := MQ.DeclareExchange(ExchangeOption{
		Name: exchangeName, Kind: "fanout", Durable: true,
	})
	require.NoError(t, err, "Setup DeclareExchange failed")

	// 执行发布
	err = MQ.PublishToFanout(context.Background(), exchangeName, data)
	assert.NoError(t, err, "PublishToFanout failed")
}

func TestPublishToRoutingKey(t *testing.T) {
	exchangeName := "test_exchange_direct"
	routingKey := "test.key.direct"
	data := map[string]string{"message": "hello direct"}

	// 设置 Direct 交换机
	err := MQ.DeclareExchange(ExchangeOption{
		Name: exchangeName, Kind: "direct", Durable: true,
	})
	require.NoError(t, err, "Setup DeclareExchange failed")

	// 执行发布
	err = MQ.PublishToDirect(context.Background(), exchangeName, routingKey, data)
	assert.NoError(t, err, "PublishToRoutingKey failed")
}

// -----------------------------------------------------------------------------
// 测试消费方法 (包含重试逻辑测试)
// -----------------------------------------------------------------------------

// MockData 用于测试消息体
type MockData struct {
	Content string `json:"content"`
}

func TestConsumeFromQueue_SuccessAndPermanentFailure(t *testing.T) {
	queueName := "test_queue_consume_ack"
	successData := MockData{Content: "should_ack"}
	permanentFailData := MockData{Content: "should_nack_no_requeue"}

	// 1. 设置队列
	err := MQ.DeclareQueue(&QueueOption{Name: queueName, Durable: true})
	require.NoError(t, err, "Setup DeclareQueue failed")

	// 2. 定义状态追踪
	receivedMsgs := make(chan string, 10)

	// 3. 消费处理函数
	handler := func(data []byte) error {
		var msg MockData
		err := json.Unmarshal(data, &msg)
		if err != nil {
			return fmt.Errorf("unmarshal error: %w", err)
		}

		receivedMsgs <- msg.Content

		switch msg.Content {
		case successData.Content:
			// 成功处理，应 Ack
			return nil
		case permanentFailData.Content:
			// 永久性失败，应 Nack(false, false)
			return NewPermanentError("permanent test failure")
		default:
			// 其他情况，如瞬时错误，不应在测试中发生
			return errors.New("unexpected message")
		}
	}

	// 4. 启动消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = MQ.ConsumeFromQueue(ctx, queueName, false, handler) // 手动确认
	require.NoError(t, err, "ConsumeFromQueue failed to start")
	time.Sleep(100 * time.Millisecond) // 确保消费者启动

	// 5. 发布消息
	// 确保 PermanentFailure 消息只被消费一次
	err = MQ.PublishToQueue(ctx, queueName, permanentFailData)
	require.NoError(t, err, "Publish permanent fail message failed")

	// 确保 Success 消息被消费
	err = MQ.PublishToQueue(ctx, queueName, successData)
	require.NoError(t, err, "Publish success message failed")

	// 6. 验证结果
	// 成功消息应该在短时间内收到
	assert.Equal(t, successData.Content, <-receivedMsgs, "Success message not received")

	// 永久失败消息应该只收到一次
	assert.Equal(t, permanentFailData.Content, <-receivedMsgs, "Permanent failure message not received")

	// 7. 验证不再重试
	// 等待一段时间（例如1秒），确保 PermanentFailure 消息没有被 Requeue 再次消费
	select {
	case msg := <-receivedMsgs:
		t.Fatalf("Permanent failure message was unexpectedly requeued: %s", msg)
	case <-time.After(1 * time.Second):
		t.Log("Permanent failure message was NOT requeued, test passed.")
	}
}

func TestConsumeFromFanout(t *testing.T) {
	exchangeName := "test_fanout_consume_ex"
	// 临时队列名为空，将自动生成队列名，并绑定到 exchangeName
	queueName := ""

	// 1. 启动两个消费者
	var wg sync.WaitGroup
	received := make(chan string, 2)

	// 消费者 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		handler := func(data []byte) error {
			received <- "Consumer1:" + string(data)
			return nil
		}

		// 注意：第一个消费者启动时会声明 Exchange/Queue/Bind
		err := MQ.ConsumeFromFanout(ctx, exchangeName, queueName, true, handler) // AutoAck=true
		assert.NoError(t, err, "Consumer 1 failed to start")
		<-ctx.Done() // 等待取消
	}()

	// 消费者 2 (使用另一个临时队列，但绑定到同一个交换机)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		handler := func(data []byte) error {
			received <- "Consumer2:" + string(data)
			return nil
		}

		err := MQ.ConsumeFromFanout(ctx, exchangeName, queueName, true, handler)
		assert.NoError(t, err, "Consumer 2 failed to start")
		<-ctx.Done()
	}()

	time.Sleep(500 * time.Millisecond) // 等待消费者启动

	// 2. 发布消息
	testMessage := "Broadcast Test"
	err := MQ.PublishToFanout(context.Background(), exchangeName, testMessage)
	require.NoError(t, err, "Publish to Fanout failed")

	// 3. 验证两个消费者都收到了消息
	var receivedList []string
	timeout := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-received:
			receivedList = append(receivedList, msg)
		case <-timeout:
			t.Fatal("Timeout waiting for both consumers to receive message")
		}
	}

	assert.Len(t, receivedList, 2, "Expected 2 messages, got %d", len(receivedList))
	assert.Contains(t, receivedList, "Consumer1:\"Broadcast Test\"", "Consumer 1 did not receive message")
	assert.Contains(t, receivedList, "Consumer2:\"Broadcast Test\"", "Consumer 2 did not receive message")

	// ⚠️ Note: 由于使用了临时队列，当测试结束，消费者关闭后，队列会被自动删除。
}

func TestConsumeWorkQueue_Qos(t *testing.T) {
	queueName := "test_work_queue_qos"

	// 1. 设置队列
	err := MQ.DeclareQueue(&QueueOption{Name: queueName, Durable: true})
	require.NoError(t, err, "Setup DeclareQueue failed")

	// 2. 启动两个工作消费者，QoS=1
	// 目标：验证消息公平分配 (虽然这里只测试功能，没有严格测试QoS)

	receivedCount := make(chan string, 10)

	// 消费者 1
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	handler1 := func(data []byte) error {
		// 模拟处理时间，以允许第二个消费者启动并获取下一条消息
		time.Sleep(50 * time.Millisecond)
		receivedCount <- "Consumer1"
		return nil
	}
	err = MQ.ConsumeWorkQueue(ctx1, queueName, "worker1", 1, handler1) // QoS=1
	require.NoError(t, err, "Worker 1 failed to start")

	// 消费者 2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	handler2 := func(data []byte) error {
		time.Sleep(50 * time.Millisecond)
		receivedCount <- "Consumer2"
		return nil
	}
	err = MQ.ConsumeWorkQueue(ctx2, queueName, "worker2", 1, handler2) // QoS=1
	require.NoError(t, err, "Worker 2 failed to start")

	time.Sleep(200 * time.Millisecond) // 确保消费者启动

	// 3. 发布消息 (4条消息)
	for i := 0; i < 4; i++ {
		err = MQ.PublishToQueue(ctx1, queueName, fmt.Sprintf("Task %d", i))
		require.NoError(t, err, "Publish task failed")
	}

	// 4. 验证分配是否大致公平（应该收到4条消息，每个消费者2条）
	counts := make(map[string]int)
	timeout := time.After(2 * time.Second)

	for i := 0; i < 4; i++ {
		select {
		case worker := <-receivedCount:
			counts[worker]++
		case <-timeout:
			t.Fatalf("Timeout waiting for 4 messages. Only received %d.", i)
		}
	}

	assert.Equal(t, 2, counts["Consumer1"], "Consumer 1 did not receive 2 tasks (QoS test)")
	assert.Equal(t, 2, counts["Consumer2"], "Consumer 2 did not receive 2 tasks (QoS test)")
}

func TestDLX(t *testing.T) {
	ctx := context.Background()
	orderData := []byte("OrderID:123456")
	// 1 用户下单时 (发送 10 分钟延迟)：
	// 延迟 600,000 毫秒 (10分钟)
	MQ.PublishDelay(ctx, "order.cancel", orderData, 600000)

	// 2 后台取消服务 (启动监听)：
	MQ.ConsumeDelay(ctx, "order.cancel", func(data []byte) error {
		// 1. 解析订单ID
		// 2. 查数据库：如果还是待支付，则执行取消
		// 3. 返回 nil (Ack)
		return nil
	})
}

func TestHash(t *testing.T) {
	ctx := context.Background()
	smsRecord := []byte("Id:123456")
	// 生产者代码示例：
	// 每次发消息，都把 ID 传进去
	smsID := "123456789"
	MQ.PublishToHashExchange(ctx, "sms_hash_ex", smsID, smsRecord)

	// 消费者代码示例：
	// 为了保证批处理不乱序，建议每个节点对应一个独立的队列（例如队列名加节点序号）。如果多个节点共用一个队列，RabbitMQ 默认的循环分发（Round-Robin）依然可能导致消息在进入 smsCache 之前就被分给了不同节点。
	// 推荐做法：
	// 节点 1
	MQ.ConsumeFromHashExchange(ctx, "sms_hash_ex", "sms_q_1", "10", func(data []byte) error {
		return nil
	})

	// 节点 2
	MQ.ConsumeFromHashExchange(ctx, "sms_hash_ex", "sms_q_2", "10", func(data []byte) error {
		return nil
	})
}

// -----------------------------------------------------------------------------
// 测试逻辑分片 Hash 路由 (Manual Slot Hash + Single Active Consumer)
// -----------------------------------------------------------------------------
func TestLogicHashFullFlow(t *testing.T) {
	ctx := context.Background()
	exchange := "sms_logic_ex"
	prefix := "sms_task"
	count := 10 // 物理队列总数
	smsID := int64(20240501001)

	// 1. 模拟【初始化/消费者】机器：创建队列环境
	// 只需要调用一次，保证所有队列带上 x-single-active-consumer 属性
	err := MQ.SetupLogicHashQueues(exchange, prefix, count)
	require.NoError(t, err)

	// 2. 模拟【机器 A】启动监听
	go func() {
		// 监听 0 号分片
		MQ.ConsumeFromLogicHash(ctx, prefix, 0, 100, func(data []byte) error {
			fmt.Println("机器 A 处理了消息:", string(data))
			return nil
		})
	}()

	// 3. 模拟【机器 B】启动监听（同样监听 0 号分片，模拟竞争）
	go func() {
		MQ.ConsumeFromLogicHash(ctx, prefix, 0, 100, func(data []byte) error {
			fmt.Println("机器 B 处理了消息:", string(data))
			return nil
		})
	}()

	time.Sleep(time.Second) // 等待监听就绪

	// 4. 模拟【发布者】发送消息
	// 无论调用多少次，只要 smsID 不变，它永远进入 sms_task_0 (假设 hash 结果是 0)
	for i := 1; i <= 3; i++ {
		msg := fmt.Sprintf("短信内容 %d", i)
		err := MQ.PublishToLogicHash(ctx, exchange, prefix, smsID, count, msg)
		assert.NoError(t, err)
	}

	time.Sleep(time.Second)
	// 运行结果你会发现：只有 A 或者只有 B 在打印，绝不会两个都打印
}

/*
部署与运维说明（给开发同学的 Tips）
1. 关于 queueCount (50)： 这个数字必须在发布端和消费端的代码里保持绝对一致。如果发布端计算按 50 模，消费端只监听了 20 个，剩下的消息就会在队列里堆积。

2. 机器扩容时的表现：
当你只有 1 台 机器时，它会独自扛起这 50 个队列的活跃消费权（Active）。
当你增加到 2 台 机器时，RabbitMQ 会自动把其中 25 个队列的活跃权转交给新机器。
切换是无损的：旧机器会先收到取消信号，处理完手头消息，新机器才正式开始。

3. 内存聚合的最佳实践： 由于同一个 smsID 保证会进入同一个 qIdx，你可以在 handler 内部使用 sync.Pool 或者按 qIdx 分组的缓存。这样即使在高并发下，同一条短信的状态更新也能在同一台机器的同一个 Batch 里完成合并入库。

4.优雅退出： 因为 ConsumeFromLogicHash 内部使用了你封装的 safeExit.StopContext，当你按下 Ctrl+C 或者 K8s 重启 Pod 时，所有的 50 个协程会安全停止并 Ack 掉已处理的消息。
*/
// 逻辑分片消费者使用方法
// 它会自动完成：队列初始化 -> 50个分片并发监听 -> 优雅退出处理
func StartLogicHashConsumerDemo(ctx context.Context) error {
	// --- 1. 核心参数配置 ---
	exchangeName := "sms_logic_ex"
	routingPrefix := "sms_task"
	queueCount := 50     // 逻辑分片数，一旦定下不要轻易改动
	prefetchCount := 200 // 每条分片队列的预取数

	// --- 2. 预初始化：确保 50 个队列属性全部正确 (带上 SAC 特性) ---
	// 无论是哪台机器先启动，都会执行这个幂等操作
	log.Printf("正在初始化 %d 个逻辑分片队列...", queueCount)
	err := MQ.SetupLogicHashQueues(exchangeName, routingPrefix, queueCount)
	if err != nil {
		return fmt.Errorf("初始化分片队列失败: %w", err)
	}

	// --- 3. 循环启动 50 个监听协程 ---
	// 提示：每台机器都启动 50 个，RabbitMQ 会自动在多台机器间平分这些队列
	log.Printf("开始订阅分片，当前机器将尝试接管部分分片的消费权...")

	for i := 0; i < queueCount; i++ {
		// 必须在闭包外捕获索引，避免协程竞争同一个 i
		idx := i

		go func(qIdx int) {
			// 调用你封装好的逻辑分片消费方法
			// handler 内部就是你的业务入库逻辑（例如 smsCache 聚合）
			err := MQ.ConsumeFromLogicHash(ctx, routingPrefix, qIdx, prefetchCount, func(data []byte) error {
				// -------------------------------------------------------
				// 这里就是你的核心业务逻辑：
				// 1. json.Unmarshal(data) -> 得到短信记录
				// 2. 放入该分片对应的内存缓存或全局缓存
				// 3. 满足批量条件时调用 s.RequestUpdateFirst.Create()
				// -------------------------------------------------------
				log.Printf("[分片 %d] 收到消息，正在处理...", qIdx)
				return nil
			})

			if err != nil {
				log.Printf("[致命错误] 分片 %d 监听器崩溃: %v", qIdx, err)
			}
		}(idx)
	}

	log.Println("所有分片监听协程已就绪。")
	return nil
}

// -----------------------------------------------------------------------------
// 逻辑分片使用方法
// -----------------------------------------------------------------------------

func TestProductionLogicHashDemo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. 启动消费端 (模拟真实业务启动)
	err := StartLogicHashConsumerDemo(ctx)
	require.NoError(t, err)

	// 2. 模拟发送端发送不同 ID 的消息
	exchange := "sms_logic_ex"
	prefix := "sms_task"

	// 发送 100 条不同 ID 的消息，观察它们如何分布到 50 个队列
	for i := 0; i < 100; i++ {
		testID := int64(1000 + i)
		msg := fmt.Sprintf("ID_%d_Data", testID)
		err := MQ.PublishToLogicHash(ctx, exchange, prefix, testID, 50, msg)
		assert.NoError(t, err)
	}

	// 等待一会儿看日志输出
	time.Sleep(2 * time.Second)
}
