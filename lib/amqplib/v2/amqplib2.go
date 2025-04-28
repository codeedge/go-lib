package v2

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
	ConsumerPoolSize  int    // 消费者通道池大小
}

// Client RabbitMQ 客户端
type Client struct {
	conn      *amqp.Connection    // 底层 AMQP 连接
	config    Config              // 连接配置
	pubPool   *channelPool        // 生产者通道池
	consPool  *channelPool        // 消费者通道池
	mu        sync.RWMutex        // 读写锁，保护连接重建等操作
	closeChan chan struct{}       // 关闭信号通道
	queueMu   sync.RWMutex        // 队列声明锁
	queues    map[string]struct{} // 存储已声明的队列信息
}

// MQ  全局公共变量
var (
	MQ *Client
)

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
	if cfg.ConsumerPoolSize <= 0 {
		cfg.ConsumerPoolSize = 3
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
	client.consPool = newChannelPool(conn, cfg.ConsumerPoolSize)

	go client.monitorConnection()
	MQ = client
	return nil
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
	c.consPool = newChannelPool(newConn, c.config.ConsumerPoolSize)

	// 清空已声明队列记录，因为连接已重建
	c.queueMu.Lock()
	c.queues = make(map[string]struct{})
	c.queueMu.Unlock()

	log.Println("Reconnected successfully")
	go c.monitorConnection()
}

// Close 关闭客户端
func (c *Client) Close() {
	close(c.closeChan)

	c.mu.Lock()
	defer c.mu.Unlock()

	close(c.pubPool.channels)
	close(c.consPool.channels)

	for ch := range c.pubPool.channels {
		ch.Close()
	}
	for ch := range c.consPool.channels {
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
	// 检查队列名称是否为空
	if opt.Name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	// 先检查队列是否已声明
	c.queueMu.RLock()
	if _, exists := c.queues[opt.Name]; exists {
		c.queueMu.RUnlock()
		return nil
	}
	c.queueMu.RUnlock()

	// 获取通道
	ch, err := c.pubPool.Get()
	if err != nil {
		return err
	}
	defer c.pubPool.Put(ch)

	// 声明队列
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

	// 记录队列信息
	c.queueMu.Lock()
	c.queues[opt.Name] = struct{}{}
	c.queueMu.Unlock()

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
	RoutingKey  string // 路由键
	Mandatory   bool   // 如果为 true，消息无法路由到队列时会返回给生产者
	Immediate   bool   // 如果为 true，消息无法立即投递到消费者会返回给生产者
	ContentType string // 消息内容类型
	Persistent  bool   // 是否持久化消息
}

func (c *Client) Publish(ctx context.Context, opt *PublishOption, body []byte) error {
	// 检查队列名称是否为空
	if opt.RoutingKey == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	// 先声明队列
	c.DeclareQueue(&QueueOption{Name: opt.RoutingKey, Durable: true})

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

// ConsumeOption 消费消息参数
type ConsumeOption struct {
	Queue     string     // 队列名称
	Consumer  string     // 消费者标签
	AutoAck   bool       // 是否自动确认
	Exclusive bool       // 是否排他消费者
	NoLocal   bool       // 不接收自身发布的消息（RabbitMQ 不支持）
	NoWait    bool       // 是否不等待服务器响应
	Args      amqp.Table // 额外参数
}

func (c *Client) Consume(ctx context.Context, opt *ConsumeOption, handler func(amqp.Delivery)) error {
	// 检查队列名称是否为空
	if opt.Queue == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	ch, err := c.consPool.Get()
	if err != nil {
		return err
	}
	defer c.consPool.Put(ch)

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
		return err
	}

	go func() {
		for d := range deliveries {
			handler(d)
		}
		log.Println("Delivery channel closed, reinitializing consumer...")
	}()

	return nil
}
