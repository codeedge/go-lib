package amqplib

import (
	"context"
	"fmt"
	"github.com/codeedge/go-lib/lib/amqplib"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"testing"
	"time"
)

// 生产者（发布/订阅模式）
func Test(t *testing.T) {
	cfg := amqplib.Config{
		URL:               "amqp://xz:xz@127.0.0.1:5672/xz",
		MaxRetries:        5,
		RetryBaseInterval: 1,
	}

	if err := amqplib.Init(cfg); err != nil {
		log.Fatal(err)
	}

	channel, err := amqplib.MQClient.NewChannelContext()
	defer channel.Close()
	if err != nil {
		log.Fatal(err)
		return
	}

	// 声明队列
	queueOpt := amqplib.QueueOption{
		Name:       "test-queue",
		Durable:    true,
		AutoDelete: false,
	}
	_, err = channel.DeclareQueue(queueOpt)
	if err != nil {
		log.Fatal(err)
	}

	// 发送消息
	err = channel.Publish(context.Background(), amqplib.PublishOption{
		Exchange:   "",
		RoutingKey: "test-queue",
		Persistent: true,
	}, []byte("test message"))
	if err != nil {
		log.Fatal(err)
	}

	// 消费消息
	err = channel.Consume(amqplib.ConsumeOption{
		Queue:   "test-queue",
		AutoAck: true,
	}, func(d amqp.Delivery) {
		log.Printf("Received message: %s", d.Body)
	})
	if err != nil {
		log.Fatal(err)
	}

	amqplib.MQClient.Close2()
	channel, err = amqplib.MQClient.NewChannelContext()
	defer channel.Close()
	if err != nil {
		log.Fatal(err)
		return
	}

	//// 声明队列
	//queueOpt = amqplib.QueueOption{
	//	Name:       "test-queue",
	//	Durable:    true,
	//	AutoDelete: false,
	//}
	//_, err = channel.DeclareQueue(queueOpt)
	//if err != nil {
	//	log.Fatal(err)
	//}
	// 发送消息
	err = channel.Publish(context.Background(), amqplib.PublishOption{
		Exchange:   "",
		RoutingKey: "test-queue",
		Persistent: true,
	}, []byte("test message"))
	if err != nil {
		log.Fatal(err)
	}

	// 消费消息
	err = channel.Consume(amqplib.ConsumeOption{
		Queue:   "test-queue",
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
