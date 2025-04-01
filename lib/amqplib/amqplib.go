package amqplib

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 声明队列等方法放在一个包含channel的结构体中，client创建为公共的，在init初始化。并且给client加个方法，返回包含channel的结构体，后面的操作都用包含channel的结构体操作
// 有缺陷，断连后及时自动重连，创建的channel不能自动更新 需要池或者map等方式来同时重新创建channel，这样就不能通过返回值每次使用同一个channel的方式使用了，要不就每次创建要不
// 使用前判断一下channel状态

// Config 连接配置
type Config struct {
	URL               string // amqp://user:pass@host:port/vhost
	MaxRetries        int    // 最大重试次数
	RetryBaseInterval int    // 重试基础间隔（秒）
}

// Client RabbitMQ 客户端（简化版）
type Client struct {
	conn       *amqp.Connection
	config     Config
	mu         sync.RWMutex
	closeChan  chan struct{}
	connActive bool
}

// ChannelContext 包含 Channel 的操作上下文
type ChannelContext struct {
	ch   *amqp.Channel
	conn *amqp.Connection
}

// MQClient 全局公共变量
var (
	MQClient *Client
)

// 创建带重试的连接（保持不变）
func createConnectionWithRetry(cfg Config) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	baseInterval := cfg.RetryBaseInterval
	if baseInterval <= 0 {
		baseInterval = 1
	}

	for i := 0; i < maxRetries; i++ {
		conn, err = amqp.Dial(cfg.URL)
		if err == nil {
			return conn, nil
		}

		waitTime := time.Duration(math.Pow(2, float64(i))) *
			time.Duration(baseInterval) * time.Second
		log.Printf("Connection attempt %d failed, retrying in %v: %v", i+1, waitTime, err)
		time.Sleep(waitTime)
	}

	return nil, fmt.Errorf("failed to connect after %d attempts: %w", maxRetries, err)
}

// New 创建新客户端 特殊场景需要新建连接提高并发时使用
func New(cfg Config) (*Client, error) {
	conn, err := createConnectionWithRetry(cfg)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:       conn,
		config:     cfg,
		closeChan:  make(chan struct{}),
		connActive: true,
	}

	go client.monitorConnection()
	return client, nil
}

// Init 初始化公共Client
func Init(cfg Config) error {
	conn, err := createConnectionWithRetry(cfg)
	if err != nil {
		return err
	}

	MQClient = &Client{
		conn:       conn,
		config:     cfg,
		closeChan:  make(chan struct{}),
		connActive: true,
	}

	go MQClient.monitorConnection()
	return nil
}

// NewChannelContext 创建一个包含 Channel 的操作上下文
func (c *Client) NewChannelContext() (*ChannelContext, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.connActive {
		return nil, fmt.Errorf("connection is closed")
	}
	// 创建 Channel
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	return &ChannelContext{ch: ch, conn: c.conn}, nil
}

// Close 关闭 ChannelContext
func (c *ChannelContext) Close() {
	c.ch.Close()
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

	// 标记旧连接失效
	c.connActive = false
	if c.conn != nil {
		c.conn.Close()
	}

	newConn, err := createConnectionWithRetry(c.config)
	if err != nil {
		log.Printf("Permanent reconnect failure: %v", err)
		return
	}

	// 更新连接状态
	c.conn = newConn
	c.connActive = true
	log.Println("Reconnected successfully")

	// 重新启动监控
	go c.monitorConnection()
}

// Close 关闭客户端
func (c *Client) Close() {
	close(c.closeChan)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}
	c.connActive = false
}

func (c *Client) Close2() {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}
	c.connActive = false
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

func (c *ChannelContext) DeclareExchange(opt ExchangeOption) error {
	return c.ch.ExchangeDeclare(
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

func (c *ChannelContext) DeclareQueue(opt QueueOption) (amqp.Queue, error) {
	return c.ch.QueueDeclare(
		opt.Name,
		opt.Durable,
		opt.AutoDelete,
		opt.Exclusive,
		opt.NoWait,
		opt.Args,
	)
}

// BindQueue 队列绑定
func (c *ChannelContext) BindQueue(queue, key, exchange string) error {
	return c.ch.QueueBind(
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

func (c *ChannelContext) Publish(ctx context.Context, opt PublishOption, body []byte) error {
	deliveryMode := amqp.Transient
	if opt.Persistent {
		deliveryMode = amqp.Persistent
	}

	return c.ch.PublishWithContext(ctx,
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

func (c *ChannelContext) Consume(opt ConsumeOption, handler func(amqp.Delivery)) error {
	deliveries, err := c.ch.Consume(
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
		log.Println("Stopped consuming from queue:", opt.Queue)
	}()

	return nil
}
