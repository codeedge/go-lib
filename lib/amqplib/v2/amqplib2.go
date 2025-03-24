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

// Config 连接配置
type Config struct {
	URL               string // amqp://user:pass@host:port/vhost
	MaxRetries        int    // 最大重试次数
	RetryBaseInterval int    // 重试基础间隔（秒）
	PublisherPoolSize int    // 生产者通道池大小
	ConsumerPoolSize  int    // 消费者通道池大小
}

// Client RabbitMQ 客户端
type Client struct {
	conn      *amqp.Connection
	config    Config
	pubPool   *channelPool
	consPool  *channelPool
	mu        sync.RWMutex
	closeChan chan struct{}
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

// New 创建新客户端
func New(cfg Config) (*Client, error) {
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
		return nil, err
	}

	client := &Client{
		conn:      conn,
		config:    cfg,
		closeChan: make(chan struct{}),
	}

	client.pubPool = newChannelPool(conn, cfg.PublisherPoolSize)
	client.consPool = newChannelPool(conn, cfg.ConsumerPoolSize)

	go client.monitorConnection()
	return client, nil
}

// 通道池实现
type channelPool struct {
	channels chan *amqp.Channel
	conn     *amqp.Connection
	mu       sync.Mutex
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

// ExchangeOption 声明交换机
type ExchangeOption struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
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

// QueueOption 声明队列
type QueueOption struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func (c *Client) DeclareQueue(opt QueueOption) (amqp.Queue, error) {
	ch, err := c.pubPool.Get()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer c.pubPool.Put(ch)

	return ch.QueueDeclare(
		opt.Name,
		opt.Durable,
		opt.AutoDelete,
		opt.Exclusive,
		opt.NoWait,
		opt.Args,
	)
}

// BindQueue 队列绑定
func (c *Client) BindQueue(queue, key, exchange string) error {
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

// PublishOption 发布消息
type PublishOption struct {
	Exchange    string
	RoutingKey  string
	Mandatory   bool
	Immediate   bool
	ContentType string
	Persistent  bool
}

func (c *Client) Publish(ctx context.Context, opt PublishOption, body []byte) error {
	ch, err := c.pubPool.Get()
	if err != nil {
		return err
	}
	defer c.pubPool.Put(ch)

	deliveryMode := amqp.Transient
	if opt.Persistent {
		deliveryMode = amqp.Persistent
	}

	return ch.PublishWithContext(ctx,
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

// ConsumeOption 消费消息
type ConsumeOption struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

func (c *Client) Consume(opt ConsumeOption, handler func(amqp.Delivery)) error {
	ch, err := c.consPool.Get()
	if err != nil {
		return err
	}
	defer c.consPool.Put(ch)

	deliveries, err := ch.Consume(
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
