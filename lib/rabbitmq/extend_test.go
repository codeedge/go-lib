package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// 测试辅助函数
// -----------------------------------------------------------------------------

// setupQueueAndExchange 在测试前声明交换机、队列和绑定
func setupQueueAndExchange(t *testing.T, exchangeName, queueName, kind, routingKey string) {
	t.Helper()

	// 1. 声明交换机
	err := MQ().DeclareExchange(ExchangeOption{
		Name:       exchangeName,
		Kind:       kind,
		Durable:    true,
		AutoDelete: false,
	})
	require.NoError(t, err, "DeclareExchange failed")

	// 2. 声明队列
	err = MQ().DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
	})
	require.NoError(t, err, "DeclareQueue failed")

	// 3. 绑定队列
	err = MQ().BindQueue(queueName, routingKey, exchangeName)
	require.NoError(t, err, "BindQueue failed")
}

// -----------------------------------------------------------------------------
// 测试发布方法
// -----------------------------------------------------------------------------

func TestPublishToQueue(t *testing.T) {
	initMQ()
	queueName := "test_queue_simple"
	data := map[string]string{"message": "hello simple queue"}

	// 声明队列 (PublishToQueue内部会调用DeclareQueue, 这里先手动确保存在)
	err := MQ().DeclareQueue(&QueueOption{
		Name: queueName, Durable: true,
	})
	require.NoError(t, err, "Setup DeclareQueue failed")

	// 执行发布
	err = MQ().PublishToQueue(context.Background(), queueName, data)
	assert.NoError(t, err, "PublishToQueue failed")
}

func TestPublishToFanout(t *testing.T) {
	initMQ()
	exchangeName := "test_exchange_fanout"
	data := map[string]string{"message": "hello fanout"}

	// 设置 Fanout 交换机
	err := MQ().DeclareExchange(ExchangeOption{
		Name: exchangeName, Kind: "fanout", Durable: true,
	})
	require.NoError(t, err, "Setup DeclareExchange failed")

	// 执行发布
	err = MQ().PublishToFanout(context.Background(), exchangeName, data)
	assert.NoError(t, err, "PublishToFanout failed")
}

func TestPublishToRoutingKey(t *testing.T) {
	initMQ()
	exchangeName := "test_exchange_direct"
	routingKey := "test.key.direct"
	data := map[string]string{"message": "hello direct"}

	// 设置 Direct 交换机
	err := MQ().DeclareExchange(ExchangeOption{
		Name: exchangeName, Kind: "direct", Durable: true,
	})
	require.NoError(t, err, "Setup DeclareExchange failed")

	// 执行发布
	err = MQ().PublishToDirect(context.Background(), exchangeName, routingKey, data)
	assert.NoError(t, err, "PublishToRoutingKey failed")
}

// TestPublishToQueue_DurableConflict 测试队列属性冲突场景
// 手动声明非持久化队列后，PublishToQueue 内部会尝试声明持久化队列，触发 406 PRECONDITION_FAILED
// DeclareQueue 应自动处理此错误，不会影响消息发布
func TestPublishToQueue_DurableConflict(t *testing.T) {
	queueName := "test_queue_durable_conflict"

	// 1. 手动声明非持久化队列（Durable: false）
	err := MQ().DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    false,
		AutoDelete: true,
	})
	require.NoError(t, err, "Manual DeclareQueue failed")

	// 2. 发布消息（PublishToQueue 内部 doPublish 会用 Durable: true 再次声明，触发 406）
	// DeclareQueue 应自动忽略 406 错误，消息正常发布
	data := map[string]string{"message": "durable conflict test"}
	err = MQ().PublishToQueue(context.Background(), queueName, data)
	assert.NoError(t, err, "PublishToQueue should handle 406 gracefully")
}

// -----------------------------------------------------------------------------
// 测试消费方法 (包含重试逻辑测试)
// -----------------------------------------------------------------------------

// MockData 用于测试消息体
type MockData struct {
	Content string `json:"content"`
}

func TestConsumeFromQueue_SuccessAndPermanentFailure(t *testing.T) {
	initMQ()
	queueName := "test_queue_consume_ack"
	successData := MockData{Content: "should_ack"}
	permanentFailData := MockData{Content: "should_nack_no_requeue"}

	// 1. 设置队列
	err := MQ().DeclareQueue(&QueueOption{Name: queueName, Durable: true})
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
			return nil
		case permanentFailData.Content:
			return NewPermanentError("permanent test failure")
		default:
			return errors.New("unexpected message")
		}
	}

	// 4. 启动消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = MQ().ConsumeFromQueue(ctx, queueName, false, handler)
	require.NoError(t, err, "ConsumeFromQueue failed to start")
	time.Sleep(100 * time.Millisecond)

	// 5. 发布消息
	err = MQ().PublishToQueue(ctx, queueName, permanentFailData)
	require.NoError(t, err, "Publish permanent fail message failed")

	err = MQ().PublishToQueue(ctx, queueName, successData)
	require.NoError(t, err, "Publish success message failed")

	// 6. 验证结果
	assert.Equal(t, successData.Content, <-receivedMsgs, "Success message not received")
	assert.Equal(t, permanentFailData.Content, <-receivedMsgs, "Permanent failure message not received")

	// 7. 验证不再重试
	select {
	case msg := <-receivedMsgs:
		t.Fatalf("Permanent failure message was unexpectedly requeued: %s", msg)
	case <-time.After(1 * time.Second):
		t.Log("Permanent failure message was NOT requeued, test passed.")
	}
}

func TestConsumeFromFanout(t *testing.T) {
	initMQ()
	exchangeName := "test_fanout_consume_ex"
	queueName := ""

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

		err := MQ().ConsumeFromFanout(ctx, exchangeName, queueName, true, handler)
		assert.NoError(t, err, "Consumer 1 failed to start")
		<-ctx.Done()
	}()

	// 消费者 2
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		handler := func(data []byte) error {
			received <- "Consumer2:" + string(data)
			return nil
		}

		err := MQ().ConsumeFromFanout(ctx, exchangeName, queueName, true, handler)
		assert.NoError(t, err, "Consumer 2 failed to start")
		<-ctx.Done()
	}()

	time.Sleep(500 * time.Millisecond)

	testMessage := "Broadcast Test"
	err := MQ().PublishToFanout(context.Background(), exchangeName, testMessage)
	require.NoError(t, err, "Publish to Fanout failed")

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

	assert.Len(t, receivedList, 2)
	assert.Contains(t, receivedList, "Consumer1:\"Broadcast Test\"")
	assert.Contains(t, receivedList, "Consumer2:\"Broadcast Test\"")
}

func TestConsumeWorkQueue_Qos(t *testing.T) {
	initMQ()
	queueName := "test_work_queue_qos"

	err := MQ().DeclareQueue(&QueueOption{Name: queueName, Durable: true})
	require.NoError(t, err, "Setup DeclareQueue failed")

	receivedCount := make(chan string, 10)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	handler1 := func(data []byte) error {
		time.Sleep(50 * time.Millisecond)
		receivedCount <- "Consumer1"
		return nil
	}
	err = MQ().ConsumeWorkQueue(ctx1, queueName, "worker1", 1, handler1)
	require.NoError(t, err, "Worker 1 failed to start")

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	handler2 := func(data []byte) error {
		time.Sleep(50 * time.Millisecond)
		receivedCount <- "Consumer2"
		return nil
	}
	err = MQ().ConsumeWorkQueue(ctx2, queueName, "worker2", 1, handler2)
	require.NoError(t, err, "Worker 2 failed to start")

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 4; i++ {
		err = MQ().PublishToQueue(ctx1, queueName, fmt.Sprintf("Task %d", i))
		require.NoError(t, err, "Publish task failed")
	}

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

	assert.Equal(t, 2, counts["Consumer1"])
	assert.Equal(t, 2, counts["Consumer2"])
}

func TestDLX(t *testing.T) {
	initMQ()
	ctx := context.Background()
	orderData := []byte("OrderID:123456")
	MQ().PublishDelay(ctx, "order.cancel", orderData, 600000)

	MQ().ConsumeDelay(ctx, "order.cancel", func(data []byte) error {
		return nil
	})
}

func TestHash(t *testing.T) {
	initMQ()
	ctx := context.Background()
	smsRecord := []byte("Id:123456")
	smsID := "123456789"
	MQ().PublishToHashExchange(ctx, "sms_hash_ex", smsID, smsRecord)

	MQ().ConsumeFromHashExchange(ctx, "sms_hash_ex", "sms_q_1", "10", func(data []byte) error {
		return nil
	})

	MQ().ConsumeFromHashExchange(ctx, "sms_hash_ex", "sms_q_2", "10", func(data []byte) error {
		return nil
	})
}

func TestLogicHashFullFlow(t *testing.T) {
	initMQ()
	ctx := context.Background()
	exchange := "sms_logic_ex"
	prefix := "sms_task"
	count := 10
	smsID := int64(20240501001)

	err := MQ().SetupLogicHashQueues(exchange, prefix, count)
	require.NoError(t, err)

	go func() {
		MQ().ConsumeFromLogicHash(ctx, prefix, 0, 100, func(data []byte) error {
			fmt.Println("机器 A 处理了消息:", string(data))
			return nil
		})
	}()

	go func() {
		MQ().ConsumeFromLogicHash(ctx, prefix, 0, 100, func(data []byte) error {
			fmt.Println("机器 B 处理了消息:", string(data))
			return nil
		})
	}()

	time.Sleep(time.Second)

	for i := 1; i <= 3; i++ {
		msg := fmt.Sprintf("短信内容 %d", i)
		err := MQ().PublishToLogicHash(ctx, exchange, prefix, smsID, count, msg)
		assert.NoError(t, err)
	}

	time.Sleep(time.Second)
}

func StartLogicHashConsumerDemo(ctx context.Context) error {
	exchangeName := "sms_logic_ex"
	routingPrefix := "sms_task"
	queueCount := 50
	prefetchCount := 200

	log.Printf("正在初始化 %d 个逻辑分片队列...", queueCount)
	err := MQ().SetupLogicHashQueues(exchangeName, routingPrefix, queueCount)
	if err != nil {
		return fmt.Errorf("初始化分片队列失败: %w", err)
	}

	log.Printf("开始订阅分片，当前机器将尝试接管部分分片的消费权...")

	for i := 0; i < queueCount; i++ {
		idx := i
		go func(qIdx int) {
			err := MQ().ConsumeFromLogicHash(ctx, routingPrefix, qIdx, prefetchCount, func(data []byte) error {
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

func TestProductionLogicHashDemo(t *testing.T) {
	initMQ()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := StartLogicHashConsumerDemo(ctx)
	require.NoError(t, err)

	exchange := "sms_logic_ex"
	prefix := "sms_task"

	for i := 0; i < 100; i++ {
		testID := int64(1000 + i)
		msg := fmt.Sprintf("ID_%d_Data", testID)
		err := MQ().PublishToLogicHash(ctx, exchange, prefix, testID, 50, msg)
		assert.NoError(t, err)
	}

	time.Sleep(2 * time.Second)
}

// -----------------------------------------------------------------------------
// 测试优先级队列
// -----------------------------------------------------------------------------

// TestPublishToPriorityQueue 测试发布优先级消息
func TestPublishToPriorityQueue(t *testing.T) {
	initMQ()
	queueName := "test_priority_queue_pub"

	// 发布不同优先级的消息（不传 maxPriority，默认 10）
	lowData := map[string]string{"level": "low", "msg": "普通消息"}
	highData := map[string]string{"level": "high", "msg": "紧急消息"}

	err := MQ().PublishToPriorityQueue(context.Background(), queueName, 1, lowData)
	assert.NoError(t, err, "Publish low priority message failed")

	err = MQ().PublishToPriorityQueue(context.Background(), queueName, 9, highData)
	assert.NoError(t, err, "Publish high priority message failed")
}

// TestPublishToPriorityQueue_CustomMax 测试自定义 maxPriority
func TestPublishToPriorityQueue_CustomMax(t *testing.T) {
	initMQ()
	queueName := "test_priority_queue_custom_max"

	// 传入自定义 maxPriority
	data := map[string]string{"level": "max", "msg": "最高优先级"}
	err := MQ().PublishToPriorityQueue(context.Background(), queueName, 255, data, 255)
	assert.NoError(t, err, "Publish max priority message failed")
}

// TestConsumeFromPriorityQueue 测试消费优先级队列，验证高优先级消息先被消费
func TestConsumeFromPriorityQueue(t *testing.T) {
	initMQ()
	queueName := "test_priority_queue_consume"

	receivedOrder := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动消费者（不传 maxPriority，默认 10）
	err := MQ().ConsumeFromPriorityQueue(ctx, queueName, func(data []byte) error {
		var msg map[string]string
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		receivedOrder <- msg["level"]
		return nil
	})
	require.NoError(t, err, "ConsumeFromPriorityQueue failed to start")
	time.Sleep(200 * time.Millisecond)

	// 先发低优先级，再发高优先级（验证高优先级先出队）
	err = MQ().PublishToPriorityQueue(ctx, queueName, 1, map[string]string{"level": "low"})
	require.NoError(t, err)

	err = MQ().PublishToPriorityQueue(ctx, queueName, 5, map[string]string{"level": "medium"})
	require.NoError(t, err)

	err = MQ().PublishToPriorityQueue(ctx, queueName, 9, map[string]string{"level": "high"})
	require.NoError(t, err)

	// 验证消费顺序：高优先级先出
	timeout := time.After(3 * time.Second)
	var received []string
	for i := 0; i < 3; i++ {
		select {
		case level := <-receivedOrder:
			received = append(received, level)
		case <-timeout:
			t.Fatalf("Timeout waiting for message %d, received: %v", i, received)
		}
	}

	// 验证顺序：high -> medium -> low
	assert.Equal(t, "high", received[0], "Highest priority should be consumed first")
	assert.Equal(t, "medium", received[1], "Medium priority should be consumed second")
	assert.Equal(t, "low", received[2], "Low priority should be consumed last")
}

// TestPriorityQueueOrder 验证批量消息的优先级排序
func TestPriorityQueueOrder(t *testing.T) {
	initMQ()
	queueName := "test_priority_queue_order"

	receivedPriorities := make(chan int, 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动消费者（传入自定义 maxPriority=50）
	err := MQ().ConsumeFromPriorityQueue(ctx, queueName, func(data []byte) error {
		var msg struct {
			Priority int `json:"priority"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		receivedPriorities <- msg.Priority
		return nil
	}, 50)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// 打乱顺序发送
	priorities := []int{5, 20, 10, 50, 1, 30, 40, 15, 25, 35}
	for _, p := range priorities {
		err = MQ().PublishToPriorityQueue(ctx, queueName, uint8(p), map[string]int{"priority": p}, 50)
		require.NoError(t, err)
	}

	// 验证消费顺序（应该按优先级降序）
	timeout := time.After(5 * time.Second)
	var received []int
	for i := 0; i < len(priorities); i++ {
		select {
		case p := <-receivedPriorities:
			received = append(received, p)
		case <-timeout:
			t.Fatalf("Timeout, received %d/%d messages: %v", i, len(priorities), received)
		}
	}

	// 验证降序排列
	for i := 1; i < len(received); i++ {
		assert.GreaterOrEqual(t, received[i-1], received[i],
			"Message %d (priority=%d) should have higher or equal priority than message %d (priority=%d)",
			i-1, received[i-1], i, received[i])
	}
}

// -----------------------------------------------------------------------------
// 测试 Topic 交换机
// -----------------------------------------------------------------------------

func TestPublishToTopic(t *testing.T) {
	initMQ()
	exchangeName := "test_exchange_topic"
	routingKey := "sms.us-east.1"
	data := map[string]string{"message": "hello topic"}

	// 执行发布
	err := MQ().PublishToTopic(context.Background(), exchangeName, routingKey, data)
	assert.NoError(t, err, "PublishToTopic failed")
}

func TestConsumeFromTopic(t *testing.T) {
	initMQ()
	exchangeName := "test_topic_consume_ex"
	queueName := "test_topic_queue"
	routingKey := "sms.*.1" // 通配符匹配

	received := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动消费者
	err := MQ().ConsumeFromTopic(ctx, exchangeName, queueName, routingKey, false, func(data []byte) error {
		received <- string(data)
		return nil
	})
	require.NoError(t, err, "ConsumeFromTopic failed to start")
	time.Sleep(200 * time.Millisecond)

	// 发布消息
	testData := "Topic Test"
	err = MQ().PublishToTopic(ctx, exchangeName, "sms.us-east.1", testData)
	require.NoError(t, err, "Publish to Topic failed")

	// 验证接收
	select {
	case msg := <-received:
		assert.Equal(t, testData, msg, "Received message mismatch")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

// -----------------------------------------------------------------------------
// 测试 Direct 交换机消费
// -----------------------------------------------------------------------------

func TestConsumeFromDirect(t *testing.T) {
	initMQ()
	exchangeName := "test_direct_consume_ex"
	queueName := "test_direct_consume_queue"
	routingKey := "sms.task"

	received := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动消费者
	err := MQ().ConsumeFromDirect(ctx, exchangeName, queueName, routingKey, false, func(data []byte) error {
		received <- string(data)
		return nil
	})
	require.NoError(t, err, "ConsumeFromDirect failed to start")
	time.Sleep(200 * time.Millisecond)

	// 发布消息
	testData := "Direct Consume Test"
	err = MQ().PublishToDirect(ctx, exchangeName, routingKey, testData)
	require.NoError(t, err, "Publish to Direct failed")

	// 验证接收
	select {
	case msg := <-received:
		assert.Equal(t, testData, msg, "Received message mismatch")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

// -----------------------------------------------------------------------------
// 测试延迟队列 (插件模式)
// -----------------------------------------------------------------------------

func TestPublishDelayPlugin(t *testing.T) {
	initMQ()
	exchangeName := "test_delay_plugin_ex"
	routingKey := "sms.delay.plugin"
	data := map[string]string{"message": "delayed plugin message"}

	// 发布延迟消息 (1秒延迟)
	err := MQ().PublishDelayPlugin(context.Background(), exchangeName, routingKey, data, 1*time.Second)
	assert.NoError(t, err, "PublishDelayPlugin failed")
}

func TestConsumeDelayPlugin(t *testing.T) {
	initMQ()
	exchangeName := "test_delay_plugin_consume_ex"
	queueName := "test_delay_plugin_queue"
	routingKey := "sms.delay.plugin.consume"

	received := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动消费者
	err := MQ().ConsumeDelayPlugin(ctx, exchangeName, queueName, routingKey, func(data []byte) error {
		received <- string(data)
		return nil
	})
	require.NoError(t, err, "ConsumeDelayPlugin failed to start")
	time.Sleep(200 * time.Millisecond)

	// 发布延迟消息 (100ms延迟，方便测试)
	testData := "Delay Plugin Test"
	err = MQ().PublishDelayPlugin(ctx, exchangeName, routingKey, testData, 100*time.Millisecond)
	require.NoError(t, err, "PublishDelayPlugin failed")

	// 验证接收
	select {
	case msg := <-received:
		// 延迟消息会经过 JSON 序列化，这里直接比较字符串
		assert.Contains(t, msg, "Delay Plugin Test", "Received message mismatch")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for delayed message")
	}
}
