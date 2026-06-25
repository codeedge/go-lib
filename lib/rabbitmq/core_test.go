package rabbitmq

import (
	"context"
	"feige-cloud-backend/pkg/exit"
	"feige-cloud-backend/pkg/gopool"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestConfig 用于测试的配置
var TestConfig = Config{
	URL:               "amqp://guest:guest@localhost:5672/", // ⚠️ 请确保您的RabbitMQ运行在此地址
	MaxRetries:        3,
	RetryBaseInterval: 1,
	PublisherPoolSize: 2,
}

var (
	_mq   *Rabbit
	_once sync.Once
)

// Set 设置 MQ 测试实例，只能执行一次（类似 instance.Set）
func Set(mq *Rabbit) {
	_once.Do(func() {
		_mq = mq
	})
}

// MQ 获取 MQ 测试实例（类似 instance.MQ）
func MQ() *Rabbit {
	return _mq
}

// initMQ 初始化 MQ 测试实例（类似 boot.go）
func initMQ() {
	mq, err := New(TestConfig)
	if err != nil {
		panic(fmt.Sprintf("MQ初始化失败: %v", err))
	}

	// 优雅退出
	exit.Instance.WG.Add(1)
	gopool.Submit(context.Background(), func(ctx context.Context) {
		defer exit.Instance.WG.Done()
		mq.GracefulShutdown(exit.Instance.StopContext)
	})

	Set(mq)
}

// 生产者（发布/订阅模式）
func Test1(t *testing.T) {
	initMQ()
	// 声明 Fanout 交换机
	err := MQ().DeclareExchange(ExchangeOption{
		Name:       "logs",
		Kind:       "fanout",
		Durable:    false,
		AutoDelete: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("Log message %d", i)
		err := MQ().Publish(context.Background(), &PublishOption{
			Exchange:    "logs",
			RoutingKey:  "",
			ContentType: "text/plain",
			Persistent:  false,
		}, []byte(msg))

		if err != nil {
			log.Printf("Publish failed: %v", err)
		} else {
			log.Printf("Published: %s", msg)
		}
		time.Sleep(1 * time.Second)
	}
}

func Test2(t *testing.T) {
	initMQ()
	queueName := "test-queue"
	// 声明临时队列
	err := MQ().DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    false,
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 绑定到交换机
	if err := MQ().BindQueue(queueName, "", "logs"); err != nil {
		log.Fatal(err)
	}

	// 开始消费
	err = MQ().Consume(context.Background(), &ConsumeOption{
		Queue:   queueName,
		AutoAck: true,
	}, func(d amqp.Delivery) {
		log.Printf("Received message: %s", d.Body)
	})

	if err != nil {
		log.Fatal(err)
	}

	// 保持运行
	select {}
}

// 重连测试
func TestReConnect(t *testing.T) {
	initMQ()
	queueName := "test-queue"

	// 声明队列
	err := MQ().DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	msg := fmt.Sprintf("Log message %d", 1111)
	err = MQ().Publish(context.Background(), &PublishOption{
		Exchange:    "",
		RoutingKey:  queueName,
		ContentType: "text/plain",
		Persistent:  false,
	}, []byte(msg))

	if err != nil {
		log.Printf("Publish failed: %v", err)
	} else {
		log.Printf("Published: %s", msg)
	}

	// 开始消费
	err = MQ().Consume(context.Background(), &ConsumeOption{
		Queue:   queueName,
		AutoAck: true,
	}, func(d amqp.Delivery) {
		log.Printf("Received message: %s", d.Body)
	})

	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	MQ().Close() // close测试

	msg = fmt.Sprintf("Log message %d", 222)
	err = MQ().Publish(context.Background(), &PublishOption{
		Exchange:    "",
		RoutingKey:  queueName,
		ContentType: "text/plain",
		Persistent:  false,
	}, []byte(msg))

	if err != nil {
		log.Printf("Publish failed: %v", err)
	} else {
		log.Printf("Published: %s", msg)
	}

	// 开始消费
	err = MQ().Consume(context.Background(), &ConsumeOption{
		Queue:   queueName,
		AutoAck: true,
	}, func(d amqp.Delivery) {
		log.Printf("Received message: %s", d.Body)
	})

	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(5 * time.Second)
	fmt.Println(1)
}
