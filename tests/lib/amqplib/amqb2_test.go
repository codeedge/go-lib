package amqplib

import (
	"context"
	"fmt"
	v2 "github.com/kdcer/go-lib/lib/amqplib/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"testing"
	"time"
)

// 生产者（发布/订阅模式）
func Test1(t *testing.T) {
	cfg := v2.Config{
		URL:               "amqp://xz:xz@127.0.0.1:5672/xz",
		MaxRetries:        5,
		RetryBaseInterval: 1,
		PublisherPoolSize: 3,
	}

	client, err := v2.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 声明 Fanout 交换机
	err = client.DeclareExchange(v2.ExchangeOption{
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
		err := client.Publish(context.Background(), v2.PublishOption{
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
	cfg := v2.Config{
		URL:               "amqp://guest:guest@localhost:5672/",
		MaxRetries:        5,
		RetryBaseInterval: 1,
		ConsumerPoolSize:  3,
	}

	client, err := v2.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 声明临时队列
	q, err := client.DeclareQueue(v2.QueueOption{
		Name:       "",
		Durable:    false,
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 绑定到交换机
	if err := client.BindQueue(q.Name, "", "logs"); err != nil {
		log.Fatal(err)
	}

	// 开始消费
	err = client.Consume(v2.ConsumeOption{
		Queue:   q.Name,
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
