package rabbitmq

import (
	"context"
	"fmt"
	"github.com/codeedge/go-lib/lib/exit"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"testing"
	"time"
)

// 生产者（发布/订阅模式）
func Test1(t *testing.T) {
	cfg := Config{
		URL:               "amqp://xz:xz@127.0.0.1:5672/xz",
		MaxRetries:        5,
		RetryBaseInterval: 1,
		PublisherPoolSize: 3,
	}

	err := Init(cfg, exit.Instance)
	if err != nil {
		log.Fatal(err)
	}

	// 声明 Fanout 交换机
	err = MQ.DeclareExchange(ExchangeOption{
		Name:       "logs",
		Kind:       "fanout",
		Durable:    false,
		AutoDelete: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; ; i++ {
		msg := fmt.Sprintf("Log message %d", i)
		err := MQ.Publish(context.Background(), &PublishOption{
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
		time.Sleep(2 * time.Second)
	}
}

func Test2(t *testing.T) {
	cfg := Config{
		URL:               "amqp://guest:guest@localhost:5672/",
		MaxRetries:        5,
		RetryBaseInterval: 1,
		PublisherPoolSize: 1,
	}

	err := Init(cfg, exit.Instance)
	if err != nil {
		log.Fatal(err)
	}

	queueName := "test-queue"
	// 声明临时队列
	err = MQ.DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    false,
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 绑定到交换机
	if err := MQ.BindQueue(queueName, "", "logs"); err != nil {
		log.Fatal(err)
	}

	// 开始消费
	err = MQ.Consume(context.Background(), &ConsumeOption{
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
	cfg := Config{
		URL:               "amqp://xz:xz@127.0.0.1:5672/xz",
		MaxRetries:        5,
		RetryBaseInterval: 1,
		PublisherPoolSize: 1000,
	}

	err := Init(cfg, exit.Instance)
	if err != nil {
		log.Fatal(err)
	}
	// client.Close()
	queueName := "test-queue"

	// 声明 Fanout 交换机
	err = MQ.DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	msg := fmt.Sprintf("Log message %d", 1111)
	err = MQ.Publish(context.Background(), &PublishOption{
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
	err = MQ.Consume(context.Background(), &ConsumeOption{
		Queue:   queueName,
		AutoAck: true,
	}, func(d amqp.Delivery) {
		log.Printf("Received message: %s", d.Body)
	})

	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	MQ.Close() // close测试

	msg = fmt.Sprintf("Log message %d", 222)
	err = MQ.Publish(context.Background(), &PublishOption{
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
	err = MQ.Consume(context.Background(), &ConsumeOption{
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
