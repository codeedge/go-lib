package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
	// 显式声明交换机，确保不会因为交换机不存在而丢消息
	if err := c.DeclareExchange(ExchangeOption{
		Name:    exchangeName,
		Kind:    "fanout",
		Durable: true,
	}); err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
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

// PublishToDirect 发布消息到Direct交换机（路由模式）
// exchangeName: 交换机名称
// routingKey: 路由键（Direct交换机精确匹配/Direct交换机支持通配符）
// data: JSON可序列化的数据
func (c *Client) PublishToDirect(ctx context.Context, exchangeName, routingKey string, data any) error {
	// 显式声明交换机。这里默认使用 direct，如果是 Topic 模式，MQ 也能兼容发送
	if err := c.DeclareExchange(ExchangeOption{
		Name:    exchangeName,
		Kind:    "direct",
		Durable: true,
	}); err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
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

// PublishToTopic 发布消息到 Topic 交换机（主题模式）
// exchangeName: 交换机名称
// routingKey: 路由键（Direct交换机精确匹配/Direct交换机支持通配符）
// data: JSON可序列化的数据
func (c *Client) PublishToTopic(ctx context.Context, exchangeName, routingKey string, data any) error {
	// 显式指定为 topic 类型
	if err := c.DeclareExchange(ExchangeOption{
		Name:    exchangeName,
		Kind:    "topic",
		Durable: true,
	}); err != nil {
		return fmt.Errorf("declare topic exchange failed: %w", err)
	}

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
func unifiedMessageHandler(queueName string, handler func(data []byte) error) func(d amqp.Delivery) {
	return func(d amqp.Delivery) {
		// 调用业务处理函数
		if err := handler(d.Body); err != nil {
			log.Printf("Message handler failed for queueName:%s Exchange:%s RoutingKey:%s err:%v\n", queueName, d.Exchange, d.RoutingKey, err.Error())
			// 业务处理失败，Nack 并 requeue (让消息重回队列，通常用于可重试的瞬时错误)
			// 需要区分是否重试，防止无限循环
			// 检查错误是否是永久性的
			requeue := true // 重新入队
			var permErr PermanentError
			if errors.As(err, &permErr) && permErr.Permanent() {
				requeue = false // 永久性错误，不重回队列 如果配置了死信队列，会进入死信队列
				log.Printf("Message treated as permanent failure, sending to DLX.queueName:%s Exchange:%s RoutingKey:%s\n", queueName, d.Exchange, d.RoutingKey)
			}
			// Nack 消息，根据错误类型决定是否 Requeue
			d.Nack(false, requeue)
		} else {
			// 业务处理成功，Ack
			d.Ack(false)
			log.Printf("Message treated as successful.queueName:%s Exchange:%s RoutingKey:%s\n", queueName, d.Exchange, d.RoutingKey)
		}
	}
}

// ConsumeFromQueue 消费队列中的消息 (作为所有Exchange模式的最终调用)
// queueName: 队列名称
// autoAck: 是否自动确认
// handler: 消息处理函数，接收反序列化的数据
func (c *Client) ConsumeFromQueue(ctx context.Context, queueName string, autoAck bool, handler func(data []byte) error) error {
	// 检查队列名称是否为空
	if queueName == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	// 声明队列
	if err := c.DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
	}); err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}

	// 调用通用的 Consume 方法，并传入统一的消息处理封装
	return c.Consume(ctx, &ConsumeOption{
		Queue:   queueName,
		AutoAck: autoAck,
	}, unifiedMessageHandler(queueName, handler))
}

// ConsumeWorkQueue 工作队列模式消费（带预取控制，简化调用）
// queueName: 队列名称
// consumerTag: 消费者标签
// prefetchCount: 预取消息数量
// handler: 消息处理函数
func (c *Client) ConsumeWorkQueue(ctx context.Context, queueName, consumerTag string, prefetchCount int, handler func(data []byte) error) error {
	// 检查队列名称是否为空
	if queueName == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	// 声明队列
	if err := c.DeclareQueue(&QueueOption{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
	}); err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}
	// 调用通用的 Consume 方法，并传入统一的消息处理封装
	return c.Consume(ctx, &ConsumeOption{
		Queue:         queueName,
		Consumer:      consumerTag,
		AutoAck:       false,         // 工作队列模式通常需要手动确认 因为需要手动确认配合 QoS/PrefetchCount 来确保消息不丢失（可靠性），并实现基于消费者处理能力的公平负载均衡。
		PrefetchCount: prefetchCount, // 传入 Qos 参数
	}, unifiedMessageHandler(queueName, handler))
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

// ==================== 延迟消息 (DLX原生模式和插件模式) ====================
/*
原生死信队列 (DLX) vs 插件 (Delayed Message) 的区分：
方案			原生死信模式 (DLX) - 当前采用													插件模式 (Delayed Message Exchange)
实现原理		消息在 A 队列过期后，被转投到 B 队列进行消费。										交换机暂存消息，到时后再投递给队列。
优点			无需安装任何东西，稳定性最高，适合固定时长的延迟（如统一10分钟取消）。					灵活性高，每条消息可以设置不同的过期时间。
限制			如果同一个队列设置不同过期时间，会产生“队头阻塞”（先发的没过期，后发的过期了也出不来）。		性能开销略大，且依赖第三方 Erlang 插件。
Golang 		配置必须配置 x-dead-letter-exchange 等 Args。									声明交换机时 Kind 必须设为 x-delayed-message。

说明与备注
方案 A：原生死信队列 (TTL + DLX)
原理：消息发送到 Delay Queue（没有消费者），设置 TTL（过期时间）。过期后，消息会被转发到配置的 Dead Letter Exchange，再进入 Target Queue。
代码实现：
dlx.ex. + targetQueue：动态生成死信交换机名，确保每个业务逻辑隔离。
delay.q. + targetQueue + ms：动态生成延迟队列，防止不同业务的延迟需求混在一起。
注意：RabbitMQ 的 TTL 检查是按序的。如果第一条消息延迟 10 分钟，第二条延迟 1 秒，第二条也要等 10 分钟过期。建议：每种常用的固定延迟时间（如 30s, 1m, 10m）对应一个延迟队列。

方案 B：延迟插件 (Delayed Message Plugin)
原理：在交换机层（Exchange）暂存消息，时间到了再投递给队列。
代码实现：
交换机类型必须设为 x-delayed-message。
通过 amqp.Table{"x-delayed-type": "direct"} 告诉插件它表现得像 Direct 交换机。
发布消息时在 Header 注入 x-delay 属性。
注意：必须在 RabbitMQ 服务器运行 rabbitmq-plugins enable rabbitmq-delayed-message-exchange 才能生效。
*/

// ==================== 延迟消息 (DLX 原生模式) ====================
// 优点：不需要安装插件。
// 缺点：存在“队头阻塞”，即如果先发了一个长延迟的消息，后发的短延迟消息也要等前面的过期。

// PublishDelay 发送延迟消息 (原生 DLX 模式)
// targetQueue: 最终处理消息的队列名
// delay: 延迟时间，例如 10 * time.Minute
func (c *Client) PublishDelay(ctx context.Context, targetQueue string, data any, delay time.Duration) error {
	// 1. 定义死信交换机和死信路由键 (这些是消息过期后流向的地方)
	// 约定死信交换机名（基于目标队列名拼接，保证通用性）
	dlxExchange := "dlx.ex." + targetQueue

	// 2. 声明中间“死信队列”
	// 注意：这个队列不能有消费者
	// 注意：不同的延迟时间建议分开队列，或者在发送处控制 TTL
	// 这里采用队列级 TTL，为了通用性，队列名带上延迟毫秒数
	delayMs := delay.Milliseconds()
	delayQueueName := fmt.Sprintf("delay.q.%s.%dms", targetQueue, delayMs)

	err := c.DeclareQueue(&QueueOption{
		Name:    delayQueueName,
		Durable: true,
		Args: amqp.Table{
			"x-dead-letter-exchange":    dlxExchange,    // 过期后投递到的死信交换机
			"x-dead-letter-routing-key": targetQueue,    // 过期后使用的路由键（即目标处理队列）
			"x-message-ttl":             int32(delayMs), // 消息在这个队列的存活时间
		},
	})
	if err != nil {
		return err
	}

	// 3. 消息发送到中间延迟队列
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return c.Publish(ctx, &PublishOption{
		Exchange:    "",
		RoutingKey:  delayQueueName,
		ContentType: Json,
		Persistent:  true,
	}, body)
}

// ConsumeDelay 消费延迟消息 (原生 DLX 模式)
func (c *Client) ConsumeDelay(ctx context.Context, targetQueue string, handler func(data []byte) error) error {
	dlxExchange := "dlx.ex." + targetQueue

	// 1. 声明最终处理消息的交换机 (死信交换机)
	if err := c.DeclareExchange(ExchangeOption{
		Name:    dlxExchange,
		Kind:    "direct",
		Durable: true,
	}); err != nil {
		return err
	}

	// 2. 声明最终处理队列并绑定到死信交换机
	if err := c.DeclareQueue(&QueueOption{Name: targetQueue, Durable: true}); err != nil {
		return err
	}

	// 3. 绑定死信交换机和处理队列
	if err := c.BindQueue(targetQueue, targetQueue, dlxExchange); err != nil {
		return err
	}

	// 4. 开始消费处理队列
	return c.ConsumeWorkQueue(ctx, targetQueue, "delay-worker", PrefetchCount, handler)
}

// ==================== 延迟消息 (插件模式: rabbitmq-delayed-message-exchange) ====================
// 优点：解决队头阻塞，支持单个消息设置不同延迟时间，更灵活。
// 缺点：需要 RabbitMQ 服务器安装插件。它是一个社区插件。RabbitMQ 的官方安装包里不包含它。你需要手动去 GitHub 下载对应的 .ez 文件，放到 RabbitMQ 的插件目录，才能执行 enable。 启用命令：rabbitmq-plugins enable rabbitmq_delayed_message_exchange

// PublishDelayPlugin 使用插件发送延迟消息
func (c *Client) PublishDelayPlugin(ctx context.Context, exchangeName, routingKey string, data any, delay time.Duration) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// 插件模式下，延迟时间是在消息 Header 里的 x-delay 字段设置
	ch, err := c.pubPool.Get()
	if err != nil {
		return err
	}
	defer c.pubPool.Put(ch)

	// 开启 Confirm 模式
	if err := ch.Confirm(false); err != nil {
		return err
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	err = ch.PublishWithContext(ctx, exchangeName, routingKey, false, false, amqp.Publishing{
		Headers: amqp.Table{
			"x-delay": delay.Milliseconds(), // 关键：插件识别此字段
		},
		ContentType:  Json,
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case confirm := <-confirms:
		if confirm.Ack {
			return nil
		}
		return fmt.Errorf("plugin message nack-ed")
	}
}

// ConsumeDelayPlugin 声明并消费插件模式的延迟交换机
func (c *Client) ConsumeDelayPlugin(ctx context.Context, exchangeName, queueName, routingKey string, handler func(data []byte) error) error {
	// 1. 声明插件类型的交换机 x-delayed-message
	if err := c.DeclareExchange(ExchangeOption{
		Name:    exchangeName,
		Kind:    "x-delayed-message", // 固定类型
		Durable: true,
		Args: amqp.Table{
			"x-delayed-type": "direct", // 内部路由逻辑类型
		},
	}); err != nil {
		return err
	}

	// 2. 正常绑定和消费
	if err := c.DeclareQueue(&QueueOption{Name: queueName, Durable: true}); err != nil {
		return err
	}
	c.BindQueue(queueName, routingKey, exchangeName)

	return c.ConsumeWorkQueue(ctx, queueName, "plugin-delay-worker", PrefetchCount, handler)
}

/*
一、一致性哈希方案对比与选型说明：

场景 A：插件模式 (x-consistent-hash 插件，且队列数随机器动态增减)
- 缺陷：
    1. 【动态扩容导致时序颠倒】：（最核心缺陷）
       假设初期 3 台机器对应 3 个队列，ID-100 路由在 Q2。扩容到 4 台机器并新增 Q4 后，哈希环重构。
       同一个 ID-100 的新消息会瞬间【漂移】到新队列 Q4。由于物理队列不同，Q4 的新消息会被新机器
       立即消费，而 Q2 里的老消息可能还在堆积或处理中。这导致"状态更新"早于"创建记录"入库，
       业务时序彻底错乱，即使用 version 字段也无法解决新老队列交织的问题。
    2. 【消费竞争导致聚合失效】：
       该插件只负责分发到队列。若多个消费者监听同一个队列，消息仍按轮询（Round-Robin）分发。
       这导致同一 ID 的多次请求散落在不同机器，内存缓存（Batch）无法对同一 ID 的数据进行闭环聚合。
    3. 【环境普适性与运维风险】：
       该插件需手动执行 `rabbitmq-plugins enable`。在云厂商（如阿里云、腾讯云）的托管版 AMQP 中
       通常不支持开启自定义插件。且每次扩容都要手动创建并绑定新队列，运维链路长，容错率低。

场景 B：逻辑分片模式 (PublishToLogicHash / x-single-active-consumer) -> 【当前短信入库业务采用：多阶段更新不保证顺序，需要保证同一id在一个节点处理】
- 原理：代码层固定 1024 Slot，映射到 N 个物理队列，利用 RabbitMQ 的 SAC 特性保证单点消费。
- 解决的问题：
    1. 严格有序：同一个 ID 永远进入同一个物理队列，且该队列在全网同时只能被一个实例处理。
    2. 内存聚合：因为单点消费，可以在内存中安全地对同一个 ID 的多次状态（Add/Update）进行合并入库。
    3. 动态扩容：增加机器时，RabbitMQ 会自动把部分队列的“活跃权”移交给新机器，过程平滑且不乱序。

二、 深度解析：什么是 x-single-active-consumer (SAC)？
你之前担心千万级数据扩容会乱序，x-single-active-consumer 就是解决这个问题的“银弹”。
1. 基本定义
通常情况下，RabbitMQ 的一个队列如果有多个消费者，它会用 轮询（Round-Robin） 方式把消息分给每个人。 开启 SAC 模式后，RabbitMQ 会在多个消费者中选出一个独占的“王者”（Active Consumer）。
唯一性：只有“王者”能收到消息并消费。
备选制：其他消费者会处于“连接成功但拿不到消息”的静默状态（Standby）。

2. 为什么它能解决“动态扩容”的乱序问题？
假设你现在有 50 个分片队列，2 台机器（A 和 B）：
初始状态：机器 A 拿到了队列 1-25 的 Active 权，机器 B 拿到 26-50。
扩容动作：你启动了机器 C，它也去监听这 50 个队列。
重新分配：RabbitMQ 发现有了新战力，它会执行 “消费权迁移”。
它会先向机器 A 发送一个 Basic.Cancel（取消通知），停止给 A 发送队列 1-10 的新消息。
关键点：它会等待机器 A 已经拿走的（Unacked）消息全部处理完或超时。
接管：确认 A 清空后，它才把队列 1-10 的 Active 权交给机器 C。
结果：对于同一个 ID，它永远不会出现“前一秒在 A 处理，后一秒在 C 处理”的重叠情况。

3. 配合 version 字段的“双保险”
SAC 保证了网络/中间件层面的单点顺序。
version 状态机 保证了数据库底层的最终一致性。 即使 SAC 因为极端网络分区出现短暂失效（虽然概率极低），数据库也会因为版本低而拒绝掉延迟到达的老消息。
*/

// PublishToHashExchange 发布消息到一致性哈希交换机
// RabbitMQ 插件本质上是内置在安装包里的，只是默认没有激活。启用插件：确保服务器执行了 rabbitmq-plugins enable rabbitmq_consistent_hash_exchange。
// exchangeName: 交换机名称
// hashKey: 哈希键（这里传入唯一 ID 的字符串，插件会对其进行哈希计算）
// data: 业务数据
// 针对 rabbitmq-consistent-hash-exchange 插件，由于它是一种特殊的交换机类型（x-consistent-hash），封装代码只需要在 ExchangeOption 的参数上进行微调。
func (c *Client) PublishToHashExchange(ctx context.Context, exchangeName, hashKey string, data any) error {
	// 1. 声明哈希交换机
	err := c.DeclareExchange(ExchangeOption{
		Name:    exchangeName,
		Kind:    "x-consistent-hash", // 插件提供的固定类型
		Durable: true,
	})
	if err != nil {
		return fmt.Errorf("declare hash exchange failed: %w", err)
	}

	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// 2. 发布消息
	// 注意：这里的 RoutingKey 就是 hashKey，插件会根据它决定去哪个队列
	return c.Publish(ctx, &PublishOption{
		Exchange:    exchangeName,
		RoutingKey:  hashKey,
		ContentType: Json,
		Persistent:  true,
	}, body)
}

// ConsumeFromHashExchange 从一致性哈希交换机消费
// RabbitMQ 插件本质上是内置在安装包里的，只是默认没有激活。启用插件：确保服务器执行了 rabbitmq-plugins enable rabbitmq_consistent_hash_exchange。
// exchangeName: 交换机名称
// queueName: 队列名称（分布式下，每个节点可以监听同一个队列名实现负载均衡，或者每个节点独立队列）
// weight: 权重，通常传 "10"，字符串形式 在一致性哈希交换机中，RoutingKey 不再是匹配字符，而是权重（Weight）。权重通常建议设为 "10"、"20" 等字符串。
// handler: 业务处理函数
func (c *Client) ConsumeFromHashExchange(ctx context.Context, exchangeName, queueName, weight string, handler func(data []byte) error) error {
	// 1. 声明哈希交换机
	if err := c.DeclareExchange(ExchangeOption{
		Name:    exchangeName,
		Kind:    "x-consistent-hash",
		Durable: true,
	}); err != nil {
		return err
	}

	// 2. 声明队列 (持久化)
	if err := c.DeclareQueue(&QueueOption{
		Name:    queueName,
		Durable: true,
	}); err != nil {
		return err
	}

	// 3. 绑定队列
	// 注意：在一致性哈希中，key 必须是权重的数字字符串（如 "10"）
	if err := c.BindQueue(queueName, weight, exchangeName); err != nil {
		return err
	}

	// 4. 调用你封装好的工作队列模式开始消费
	return c.ConsumeWorkQueue(ctx, queueName, "hash-worker", PrefetchCount, handler)
}

// ==================== 逻辑分片 Hash 路由模式 (手动控制 Slot) ====================
// 适用场景：千万级数据、内存聚合、严格保证 ID 顺序、支持平滑扩容。
// 原理：ID -> Hash -> Slot(1024) -> Queue(N) -> Single Active Consumer

// PublishToLogicHash 发布消息到逻辑分片队列
// exchangeName: 交换机名称 (建议用 Direct)
// routingKeyPrefix: 队列名前缀，如 "sms_task"
// id: 用于计算 Hash 的业务 ID (如 sms_id)
// queueCount: 物理队列的总数 (发送端和消费端必须一致，如 50)
// data: 业务数据
func (c *Client) PublishToLogicHash(ctx context.Context, exchangeName, routingKeyPrefix string, id int64, queueCount int, data any) error {
	// 1. 计算 Slot (固定 1024 分片确保分布均匀)
	idStr := strconv.FormatInt(id, 10)
	// 使用 CRC32 计算哈希值
	hashVal := crc32.ChecksumIEEE([]byte(idStr))
	slot := hashVal % 1024

	// 2. 映射到物理队列索引
	queueIdx := slot % uint32(queueCount)
	// 最终路由键，例如 "sms_task_8"
	routingKey := fmt.Sprintf("%s_%d", routingKeyPrefix, queueIdx)

	// 3. 直接复用你已有的 Direct 发布逻辑
	return c.PublishToDirect(ctx, exchangeName, routingKey, data)
}

// ConsumeFromLogicHash 消费逻辑分片队列 (单活跃消费者模式)
// routingKeyPrefix: 队列名前缀
// queueIdx: 当前实例负责的队列索引 (如果是启动多协程监听所有，则循环调用)
// prefetchCount: 预取数量 (建议 100-300)
// handler: 业务处理函数
func (c *Client) ConsumeFromLogicHash(ctx context.Context, routingKeyPrefix string, queueIdx int, prefetchCount int, handler func(data []byte) error) error {
	queueName := fmt.Sprintf("%s_%d", routingKeyPrefix, queueIdx)

	// 1. 声明带“单活跃消费者”特性的队列
	// 这是保证扩容不乱序的核心：即使多台机器同时监听这个队列，RabbitMQ 也只允许一台机器消费
	if err := c.DeclareQueue(&QueueOption{
		Name:    queueName,
		Durable: true,
		Args: amqp.Table{
			"x-single-active-consumer": true, // 开启单活跃消费者
		},
	}); err != nil {
		return fmt.Errorf("declare logic hash queue failed: %w", err)
	}

	// 2. 调用已有的工作队列模式进行消费
	// 注意：consumerTag 建议加上机器标识，方便在后台查看是哪台机器在活跃
	consumerTag := fmt.Sprintf("hash-worker-%d", queueIdx)
	return c.ConsumeWorkQueue(ctx, queueName, consumerTag, prefetchCount, handler)
}

// SetupLogicHashQueues 辅助方法：初始化所有 Hash 物理队列并绑定到交换机
// 在消费者程序启动时调用一次
func (c *Client) SetupLogicHashQueues(exchangeName, routingKeyPrefix string, queueCount int) error {
	for i := 0; i < queueCount; i++ {
		queueName := fmt.Sprintf("%s_%d", routingKeyPrefix, i)

		// 1. 声明队列 (带 SAC 特性)
		if err := c.DeclareQueue(&QueueOption{
			Name:    queueName,
			Durable: true,
			Args:    amqp.Table{"x-single-active-consumer": true},
		}); err != nil {
			return err
		}

		// 2. 绑定路由键 (路由键名 = 队列名)
		if err := c.BindQueue(queueName, queueName, exchangeName); err != nil {
			return err
		}
	}
	return nil
}
