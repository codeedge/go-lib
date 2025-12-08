package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Json = "application/json"
	Text = "text/plain"
)

// Config 连接配置
type Config struct {
	URL               string // RabbitMQ 连接地址，例如 amqp://user:pass@host:port/vhost
	MaxRetries        int    // 最大重试次数
	RetryBaseInterval int    // 重试基础间隔（秒）
	PublisherPoolSize int    // 生产者通道池大小
}

// Client RabbitMQ 客户端
type Client struct {
	conn      *amqp.Connection    // 底层 AMQP 连接
	config    Config              // 连接配置
	pubPool   *channelPool        // 生产者通道池
	mu        sync.RWMutex        // 读写锁，保护连接重建等操作
	closeChan chan struct{}       // 关闭信号通道
	queueMu   sync.RWMutex        // 队列声明锁
	queues    map[string]struct{} // 存储已声明的队列信息 虽然队列声明是幂等的，但为了减少io操作，这里使用一个 map 来存储已声明的队列信息
	// 消费者注册表，用于自动恢复
	consumerRegistry sync.Map // Key: queueName+consumerTag, Value: *ConsumerState
}

// MQ  全局公共变量
var (
	MQ *Client
)

func Init(cfg Config) (err error) {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.RetryBaseInterval <= 0 {
		cfg.RetryBaseInterval = 1
	}
	if cfg.PublisherPoolSize <= 0 {
		cfg.PublisherPoolSize = 3
	}

	conn, err := createConnectionWithRetry(cfg)
	if err != nil {
		return err
	}

	client := &Client{
		conn:      conn,
		config:    cfg,
		closeChan: make(chan struct{}),
		queues:    make(map[string]struct{}),
	}

	client.pubPool = newChannelPool(conn, cfg.PublisherPoolSize)

	go client.monitorConnection()
	MQ = client
	return nil
}

// 创建带重试的连接
func createConnectionWithRetry(cfg Config) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	for i := 0; i < cfg.MaxRetries; i++ {
		conn, err = amqp.Dial(cfg.URL)
		if err == nil {
			return conn, nil
		}

		waitTime := time.Duration(math.Pow(2, float64(i))) *
			time.Duration(cfg.RetryBaseInterval) * time.Second
		log.Printf("Connection attempt %d failed, retrying in %v: %v", i+1, waitTime, err)
		time.Sleep(waitTime)
	}

	return nil, fmt.Errorf("failed to connect after %d attempts: %w", cfg.MaxRetries, err)
}

// channelPool 通道池实现，用于复用 AMQP 通道
type channelPool struct {
	channels chan *amqp.Channel // 通道池
	conn     *amqp.Connection   // 关联的 AMQP 连接
}

func newChannelPool(conn *amqp.Connection, size int) *channelPool {
	pool := &channelPool{
		channels: make(chan *amqp.Channel, size),
		conn:     conn,
	}

	// 初始化通道
	for i := 0; i < size; i++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Printf("Error creating channel: %v", err)
			continue
		}
		pool.channels <- ch
	}

	return pool
}

func (p *channelPool) Get() (*amqp.Channel, error) {
	for {
		select {
		case ch := <-p.channels:
			if ch.IsClosed() {
				continue // 自动跳过已关闭通道
			}
			return ch, nil
		default:
			newCh, err := p.conn.Channel()
			if err != nil {
				return nil, err
			}
			return newCh, nil
		}
	}
}

func (p *channelPool) Put(ch *amqp.Channel) {
	select {
	case p.channels <- ch:
	default:
		ch.Close()
	}
}

// 监控连接状态
func (c *Client) monitorConnection() {
	notifyClose := c.conn.NotifyClose(make(chan *amqp.Error))

	select {
	case <-c.closeChan:
		log.Println("Connection closed manual-lock")
		return
	case err := <-notifyClose:
		log.Printf("Connection closed: %v", err)
		c.reconnect()
	}
}

// 重连处理
func (c *Client) reconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Println("Attempting to reconnect...")

	// 关闭旧连接
	if c.conn != nil {
		c.conn.Close()
	}

	newConn, err := createConnectionWithRetry(c.config)
	if err != nil {
		log.Printf("Permanent reconnect failure: %v", err)
		return
	}

	// 重建资源
	c.conn = newConn
	c.pubPool = newChannelPool(newConn, c.config.PublisherPoolSize)

	// 清空已声明队列记录，因为连接已重建
	c.queueMu.Lock()
	c.queues = make(map[string]struct{})
	c.queueMu.Unlock()

	log.Println("Reconnected successfully. Starting consumer recovery...")

	// 消费者核心恢复逻辑
	c.consumerRegistry.Range(func(key, value any) bool {
		state := value.(*ConsumerState)
		// 使用一个新的 goroutine 重新启动消费者，避免阻塞重连逻辑
		go func() {
			log.Printf("Attempting to recover consumer: %s", key)
			// 重新声明队列（因为连接断开，声明可能丢失）
			// 注意：这里假设所有消费者都需要持久化队列（工作队列模式）
			c.DeclareQueue(&QueueOption{
				Name:    state.Option.Queue,
				Durable: true,
			})

			// 重新调用 Consume。由于 Consume 内部会创建新通道、设置 QoS 并启动新的监听循环，
			// 且注册表已存在，因此这是安全的。
			if err := c.Consume(context.Background(), state.Option, state.Handler); err != nil {
				log.Printf("Failed to recover consumer %s: %v", key, err)
				// 恢复失败，从注册表中删除 (可选，如果希望永久失败则删除)
				// c.consumerRegistry.Delete(key)
			}
		}()
		return true // 继续迭代下一个
	})

	log.Println("Consumer recovery process initiated.")
	go c.monitorConnection()
}

// Close 关闭客户端
func (c *Client) Close() {
	close(c.closeChan)

	c.mu.Lock()
	defer c.mu.Unlock()

	close(c.pubPool.channels)

	for ch := range c.pubPool.channels {
		ch.Close()
	}

	if c.conn != nil {
		c.conn.Close()
	}
}

// ExchangeOption 声明交换机参数
type ExchangeOption struct {
	Name       string     // 交换机名称
	Kind       string     // 交换机类型（如 direct、fanout、topic、headers）
	Durable    bool       // 是否持久化
	AutoDelete bool       // 是否自动删除
	Internal   bool       // 是否为内部交换机
	NoWait     bool       // 是否不等待服务器响应
	Args       amqp.Table // 额外参数
}

func (c *Client) DeclareExchange(opt ExchangeOption) error {
	ch, err := c.pubPool.Get()
	if err != nil {
		return err
	}
	defer c.pubPool.Put(ch)

	return ch.ExchangeDeclare(
		opt.Name,
		opt.Kind,
		opt.Durable,
		opt.AutoDelete,
		opt.Internal,
		opt.NoWait,
		opt.Args,
	)
}

// QueueOption 声明队列参数
type QueueOption struct {
	Name       string     // 队列名称
	Durable    bool       // 是否持久化
	AutoDelete bool       // 是否自动删除
	Exclusive  bool       // 是否排他队列
	NoWait     bool       // 是否不等待服务器响应
	Args       amqp.Table // 额外参数
}

func (c *Client) DeclareQueue(opt *QueueOption) error {
	// 1. 读锁检查 (只对用户指定的命名队列进行缓存检查)
	if opt.Name != "" {
		c.queueMu.RLock()
		if _, exists := c.queues[opt.Name]; exists {
			c.queueMu.RUnlock()
			return nil // 本地命中，性能优化成功
		}
		c.queueMu.RUnlock()
	}

	// 获取通道
	ch, err := c.pubPool.Get()
	if err != nil {
		return err
	}
	defer c.pubPool.Put(ch)

	// 2. 声明队列
	queue, err := ch.QueueDeclare(
		opt.Name,
		opt.Durable,
		opt.AutoDelete,
		opt.Exclusive,
		opt.NoWait,
		opt.Args,
	)
	if err != nil {
		return err
	}

	log.Printf("queue declare %s", queue.Name)

	// 3. 回填队列名 (必须保留，因为 BindQueue 和 Consume 需要实际名字)
	if opt.Name == "" {
		opt.Name = queue.Name
	}

	// 4. 改进的记录逻辑：仅记录持久队列或非自动删除队列
	// 临时队列具有 AutoDelete=true 且 Durable=false 的特性。
	// 我们只记录那些会长期存在的队列。
	if opt.Durable || !opt.AutoDelete {
		c.queueMu.Lock()
		// 确保使用回填后的 opt.Name
		c.queues[opt.Name] = struct{}{}
		c.queueMu.Unlock()
	}
	return nil
}

// BindQueue 队列绑定
func (c *Client) BindQueue(queue, key, exchange string) error {
	// 检查队列名称是否为空
	if queue == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	ch, err := c.pubPool.Get()
	if err != nil {
		return err
	}
	defer c.pubPool.Put(ch)

	return ch.QueueBind(
		queue,
		key,
		exchange,
		false, // noWait
		nil,
	)
}

// PublishOption 发布消息参数
type PublishOption struct {
	Exchange    string // 交换机名称
	RoutingKey  string // 路由键 简单模式、工作队列模式使用队列名 发布订阅模式的广播模式置空、Direct精确匹配和Topic通配符模式作为匹配交换机用
	Mandatory   bool   // 如果为 true，消息无法路由到队列时会返回给生产者
	Immediate   bool   // 如果为 true，消息无法立即投递到消费者会返回给生产者
	ContentType string // 消息内容类型
	Persistent  bool   // 是否持久化消息
}

func (c *Client) Publish(ctx context.Context, opt *PublishOption, body []byte) (err error) {
	if opt.RoutingKey == "" && opt.Exchange == "" {
		return fmt.Errorf("路由键和交换机名称不能同时为空")
	}
	// 只有在直接发送到队列（exchange为空）时才声明队列
	if opt.Exchange == "" && opt.RoutingKey != "" {
		// 声明队列
		err = c.DeclareQueue(&QueueOption{Name: opt.RoutingKey, Durable: true})
		if err != nil {
			return err
		}
	}
	// 发布者应该用池
	// 发布者 (Publisher) 的操作是无状态的：它只是短暂地获取一个通道，发送消息，然后立即归还。
	// 1.高并发/高吞吐：通道池能有效应对高频、突发的发布请求，通过复用通道来减少创建/关闭的开销，提高性能。
	// 2.无状态性：发布操作不需要设置 Qos 等会污染通道状态的配置，池中的通道可以安全地被复用。
	ch, err := c.pubPool.Get()
	if err != nil {
		return err
	}
	defer c.pubPool.Put(ch)

	deliveryMode := amqp.Transient
	if opt.Persistent {
		deliveryMode = amqp.Persistent
	}

	return ch.PublishWithContext(
		ctx,
		opt.Exchange,
		opt.RoutingKey,
		opt.Mandatory,
		opt.Immediate,
		amqp.Publishing{
			ContentType:  opt.ContentType,
			Body:         body,
			DeliveryMode: deliveryMode,
		},
	)
}

// ConsumerState 存储消费者重建所需的所有信息
type ConsumerState struct {
	Option  *ConsumeOption      // 消费参数
	Handler func(amqp.Delivery) // 消息处理函数
}

// ConsumeOption 消费消息参数
type ConsumeOption struct {
	Queue         string     // 队列名称
	Consumer      string     // 消费者标签
	AutoAck       bool       // 是否自动确认
	Exclusive     bool       // 是否排他消费者
	NoLocal       bool       // 不接收自身发布的消息（RabbitMQ 不支持）
	NoWait        bool       // 是否不等待服务器响应
	PrefetchCount int        // Qos 预取消息数量，大于 0 时设置 QoS
	Args          amqp.Table // 额外参数
}

func (c *Client) Consume(ctx context.Context, opt *ConsumeOption, handler func(amqp.Delivery)) error {
	// 检查队列名称是否为空
	if opt.Queue == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	// --- 注册消费者状态 ---
	consumer := "::consumer" // 消费者标签为空则使用::consumer作为重连恢复的消费者标签
	if opt.Consumer != "" {
		consumer = opt.Consumer
	}
	// 消费者 Key: 队列名 + 消费者标签
	key := fmt.Sprintf("%s-%s", opt.Queue, consumer)

	// 存储消费者状态，以便重连后恢复
	c.consumerRegistry.Store(key, &ConsumerState{
		Option:  opt,
		Handler: handler,
	})

	// 消费者不应该用池
	// 消费者 (Consumer) 的操作是有状态和长期的。
	// 1.QoS 配置：消费者需要设置 prefetchCount (QoS) 来控制消息流，这是一个通道级别的状态。如果通道被归还并复用，会污染其他消费者的设置。
	// 2.长期持有：消费者需要长期独占一个通道来接收消息流，直到程序关闭或连接断开。

	// 1.直接从 Connection 创建新的专用通道
	ch, err := c.conn.Channel()
	if err != nil {
		// 启动失败，移除注册
		c.consumerRegistry.Delete(key)
		return err
	}
	// 2. 根据 PrefetchCount 设置 QoS (QoS只对当前专用通道生效)
	if opt.PrefetchCount > 0 {
		err = ch.Qos(
			// 每次预取一个任务 prefetchCount是每个消费者在预取消息的数量，比如设置为10的话，消费者会一次性获取10条消息，处理完再取下一批。
			// 这样能提高吞吐量，但可能增加内存使用。如果设置为1，就是每次处理完一条再取下一条，更公平但可能降低效率。0（无限制，即尽可能多预取）。
			opt.PrefetchCount,
			// 不限制消息总数 prefetchsize是预取消息的总大小，以字节为单位。比如设置为1024的话，消费者最多预取总大小不超过1KB的消息。通常设置为 0 表示不限制
			0,
			// 应用在当前通道 决定 QoS 设置是否作用于当前 Channel 的所有消费者。通常设置为 false
			false,
		)
		if err != nil {
			ch.Close()
			c.consumerRegistry.Delete(key)
			return fmt.Errorf("set QoS failed: %w", err)
		}
		log.Printf("Consumer for queue %s set QoS prefetch=%d", opt.Queue, opt.PrefetchCount)
	}

	deliveries, err := ch.ConsumeWithContext(
		ctx,
		opt.Queue,
		opt.Consumer,
		opt.AutoAck,
		opt.Exclusive,
		opt.NoLocal,
		opt.NoWait,
		opt.Args,
	)
	if err != nil {
		// 如果消费启动失败，关闭创建的通道
		ch.Close()
		c.consumerRegistry.Delete(key)
		return err
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println("Consumer panic:", r)
			}
		}()
		for d := range deliveries {
			handler(d)
		}
		// 当 deliveries channel 关闭时（通常是因为连接断开），关闭通道
		log.Println("Delivery channel closed, Dedicated channel closing....")
		ch.Close()
		// ** 不从注册表删除 **：通道关闭通常意味着连接断开，此时需要等待 reconnect 逻辑来重建该消费者
	}()

	return nil
}
