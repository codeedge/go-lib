package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// ==================== 简化的发布方法 ====================

// PublishToQueue 发布消息到队列（简单队列/工作队列模式）
// queueName: 队列名称
// data: JSON可序列化的数据
func (c *Client) PublishToQueue(ctx context.Context, queueName string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}

	return c.Publish(ctx, &PublishOption{
		Exchange:    "",
		RoutingKey:  queueName,
		ContentType: Json,
		Persistent:  true,
	}, body)
}

// PublishToFanout 发布消息到Fanout交换机（广播模式）
// exchangeName: 交换机名称
// data: JSON可序列化的数据
func (c *Client) PublishToFanout(ctx context.Context, exchangeName string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}

	return c.Publish(ctx, &PublishOption{
		Exchange:    exchangeName,
		RoutingKey:  "", // Fanout模式路由键为空
		ContentType: Json,
		Persistent:  true,
	}, body)
}

// PublishToRoutingKey 发布消息到Direct交换机（路由模式）/Topic交换机（主题模式）
// exchangeName: 交换机名称
// routingKey: 路由键（Direct交换机精确匹配/Direct交换机支持通配符）
// data: JSON可序列化的数据
func (c *Client) PublishToRoutingKey(ctx context.Context, exchangeName, routingKey string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}

	return c.Publish(ctx, &PublishOption{
		Exchange:    exchangeName,
		RoutingKey:  routingKey,
		ContentType: Json,
		Persistent:  true,
	}, body)
}

// ==================== 简化的消费方法 ====================

// unifiedMessageHandler 封装了业务处理和 Ack/Nack 逻辑
// handler 接收消息体并返回错误
// 注意：该封装假定为手动确认模式。如果 Consume 调用设置了 AutoAck=true，
// 则内部的 d.Ack/Nack 会被底层 AMQP 库忽略。
func unifiedMessageHandler(handler func(data []byte) error) func(d amqp.Delivery) {
	return func(d amqp.Delivery) {
		// 调用业务处理函数
		if err := handler(d.Body); err != nil {
			log.Printf("Message handler failed for queue %s: %v", d.RoutingKey, err)
			// 业务处理失败，Nack 并 requeue (让消息重回队列，通常用于可重试的瞬时错误)
			// 需要区分是否重试，防止无限循环
			// 检查错误是否是永久性的
			requeue := true // 重新入队
			var permErr PermanentError
			if errors.As(err, &permErr) && permErr.Permanent() {
				requeue = false // 永久性错误，不重回队列 如果配置了死信队列，会进入死信队列
				log.Printf("Message treated as permanent failure, sending to DLX.")
			}
			// Nack 消息，根据错误类型决定是否 Requeue
			d.Nack(false, requeue)
		} else {
			// 业务处理成功，Ack
			d.Ack(false)
		}
	}
}

// ConsumeFromQueue 消费队列中的消息 (作为所有Exchange模式的最终调用)
// queueName: 队列名称
// autoAck: 是否自动确认
// handler: 消息处理函数，接收反序列化的数据
func (c *Client) ConsumeFromQueue(ctx context.Context, queueName string, autoAck bool, handler func(data []byte) error) error {
	// 调用通用的 Consume 方法，并传入统一的消息处理封装
	return c.Consume(ctx, &ConsumeOption{
		Queue:   queueName,
		AutoAck: autoAck,
	}, unifiedMessageHandler(handler))
}

// ConsumeFromFanout 消费Fanout交换机的消息
// exchangeName: 交换机名称
// queueName: 队列名称（为空则自动创建临时队列） 发布订阅模式需要每个消费者使用不同的队列名，比如队列名拼上节点id
// autoAck: 是否自动确认
// handler: 消息处理函数
func (c *Client) ConsumeFromFanout(ctx context.Context, exchangeName, queueName string, autoAck bool, handler func(data []byte) error) error {
	// 1. 声明 Fanout 交换机 (Exchange 必须持久化)
	if err := c.DeclareExchange(ExchangeOption{
		Name:       exchangeName,
		Kind:       "fanout",
		Durable:    true,
		AutoDelete: false,
	}); err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}

	// 2. 确定队列参数
	opt := &QueueOption{
		Durable:    true, // 默认持久化
		AutoDelete: false,
		Exclusive:  false,
		Name:       queueName,
	}

	// 如果队列名为空，则创建临时独占队列 (标准的 Pub/Sub 模式)
	if queueName == "" {
		opt.Exclusive = true  // 独占，只能被一个消费者连接使用
		opt.AutoDelete = true // 消费者断开后自动删除
		opt.Durable = false   // 临时队列不需要持久化
		// Name 保持为空，QueueDeclare 会自动生成名字
	}

	// 3. 声明队列
	if err := c.DeclareQueue(opt); err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}

	queueName = opt.Name

	// 绑定队列到Fanout交换机
	if err := c.BindQueue(queueName, "", exchangeName); err != nil {
		return fmt.Errorf("bind queue failed: %w", err)
	}

	// 开始消费
	return c.ConsumeFromQueue(ctx, queueName, autoAck, handler)
}

// ConsumeFromDirect 消费Direct交换机的消息
// exchangeName: 交换机名称
// queueName: 队列名称 发布订阅模式需要每个消费者使用不同的队列名，比如队列名拼上节点id
// routingKey: 路由键
// autoAck: 是否自动确认
// handler: 消息处理函数
func (c *Client) ConsumeFromDirect(ctx context.Context, exchangeName, queueName, routingKey string, autoAck bool, handler func(data []byte) error) error {
	if queueName == "" || routingKey == "" {
		return errors.New("queueName and routingKey cannot be empty")
	}
	// 声明Direct交换机
	if err := c.DeclareExchange(ExchangeOption{
		Name:       exchangeName,
		Kind:       "direct",
		Durable:    true,
		AutoDelete: false,
	}); err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}

	// 声明队列
	if err := c.DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
	}); err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}

	// 绑定队列到Direct交换机
	if err := c.BindQueue(queueName, routingKey, exchangeName); err != nil {
		return fmt.Errorf("bind queue failed: %w", err)
	}

	// 开始消费
	return c.ConsumeFromQueue(ctx, queueName, autoAck, handler)
}

// ConsumeFromTopic 消费Topic交换机的消息
// exchangeName: 交换机名称
// queueName: 队列名称 发布订阅模式需要每个消费者使用不同的队列名，比如队列名拼上节点id
// routingKey: 路由键（支持通配符）
// autoAck: 是否自动确认
// handler: 消息处理函数
func (c *Client) ConsumeFromTopic(ctx context.Context, exchangeName, queueName, routingKey string, autoAck bool, handler func(data []byte) error) error {
	if queueName == "" || routingKey == "" {
		return errors.New("queueName and routingKey cannot be empty")
	}
	// 声明Topic交换机
	if err := c.DeclareExchange(ExchangeOption{
		Name:       exchangeName,
		Kind:       "topic",
		Durable:    true,
		AutoDelete: false,
	}); err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}

	// 声明队列
	if err := c.DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
	}); err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}

	// 绑定队列到Topic交换机
	if err := c.BindQueue(queueName, routingKey, exchangeName); err != nil {
		return fmt.Errorf("bind queue failed: %w", err)
	}

	// 开始消费
	return c.ConsumeFromQueue(ctx, queueName, autoAck, handler)
}

// ==================== 工作队列专用方法 ====================

// ConsumeWorkQueue 工作队列模式消费（带预取控制，简化调用）
// queueName: 队列名称
// consumerTag: 消费者标签
// prefetchCount: 预取消息数量
// handler: 消息处理函数
func (c *Client) ConsumeWorkQueue(ctx context.Context, queueName, consumerTag string, prefetchCount int, handler func(data []byte) error) error {
	// 封装消息处理逻辑：JSON解析 + Ack/Nack
	messageHandler := func(d amqp.Delivery) {
		if err := handler(d.Body); err != nil {
			log.Printf("Worker handler failed: %v", err)
			d.Nack(false, true) // Nack, Requeue
		} else {
			d.Ack(false) // Ack
		}
	}

	// 调用通用的 Consume 方法
	return c.Consume(ctx, &ConsumeOption{
		Queue:         queueName,
		Consumer:      consumerTag,
		AutoAck:       false,         // 工作队列模式通常需要手动确认
		PrefetchCount: prefetchCount, // 传入 Qos 参数
	}, messageHandler)
}
