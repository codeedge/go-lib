package amqplib

import (
	"context"
	"fmt"
	v2 "github.com/codeedge/go-lib/lib/amqplib/v2"
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
		PublisherPoolSize: 1,
		ConsumerPoolSize:  1,
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

// 重连测试
func TestReConnect(t *testing.T) {
	cfg := v2.Config{
		URL:               "amqp://xz:xz@127.0.0.1:5672/xz",
		MaxRetries:        5,
		RetryBaseInterval: 1,
		PublisherPoolSize: 1000,
		ConsumerPoolSize:  1000,
	}

	err := v2.Init(cfg)
	if err != nil {
		log.Fatal(err)
	}
	//client.Close()

	// 声明 Fanout 交换机
	queueOpt, err := v2.MQClient.DeclareQueue(v2.QueueOption{
		Name:       "test-queue",
		Durable:    true,
		AutoDelete: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	msg := fmt.Sprintf("Log message %d", 1111)
	err = v2.MQClient.Publish(context.Background(), v2.PublishOption{
		Exchange:    "",
		RoutingKey:  queueOpt.Name,
		ContentType: "text/plain",
		Persistent:  false,
	}, []byte(msg))

	if err != nil {
		log.Printf("Publish failed: %v", err)
	} else {
		log.Printf("Published: %s", msg)
	}

	// 开始消费
	err = v2.MQClient.Consume(v2.ConsumeOption{
		Queue:   queueOpt.Name,
		AutoAck: true,
	}, func(d amqp.Delivery) {
		log.Printf("Received message: %s", d.Body)
	})

	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	v2.MQClient.Close() // close测试

	msg = fmt.Sprintf("Log message %d", 222)
	err = v2.MQClient.Publish(context.Background(), v2.PublishOption{
		Exchange:    "",
		RoutingKey:  queueOpt.Name,
		ContentType: "text/plain",
		Persistent:  false,
	}, []byte(msg))

	if err != nil {
		log.Printf("Publish failed: %v", err)
	} else {
		log.Printf("Published: %s", msg)
	}

	// 开始消费
	err = v2.MQClient.Consume(v2.ConsumeOption{
		Queue:   queueOpt.Name,
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
