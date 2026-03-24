package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codeedge/go-lib/lib/exit"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Json          = "application/json"
	Text          = "text/plain"
	PrefetchCount = 100 // Qos 预取消息数量 100-300之间比较合理
)

// Config 连接配置
type Config struct {
	URL                    string // RabbitMQ 连接地址，例如 amqp://user:pass@host:port/vhost
	MaxRetries             int    // 最大重试次数
	RetryBaseInterval      int    // 重试基础间隔（秒）
	PublisherPoolSize      int    // 生产者通道池大小
	EnablePublisherConfirm bool   // 是否开启生产者确认模式
	ConnectName            string // 连接名称
}

// Rabbit RabbitMQ 客户端
type Rabbit struct {
	conn      *amqp.Connection    // 底层 AMQP 连接
	config    Config              // 连接配置
	pubPool   *channelPool        // 生产者通道池
	mu        sync.RWMutex        // 读写锁，保护连接重建等操作
	closeChan chan struct{}       // 关闭信号通道
	queueMu   sync.RWMutex        // 队列声明锁
	queues    map[string]struct{} // 存储已声明的队列信息 虽然队列声明是幂等的，但为了减少io操作，这里使用一个 map 来存储已声明的队列信息
	// 消费者注册表，用于自动恢复
	consumerRegistry sync.Map       // Key: queueName+consumerTag, Value: *ConsumerState
	safeExit         *exit.SafeExit // 优雅退出
	consumeWG        sync.WaitGroup // 跟踪所有通过消费的任务
	shuttingDown     atomic.Bool    // 优雅关闭状态标志
	// --- 新增：重连控制 ---
	isConnecting  int32      // 0: 正常, 1: 正在重连
	reconnectCond *sync.Cond // 用于阻塞和唤醒请求
}

// mq  全局公共变量
var (
	client *Rabbit
)

func Client() *Rabbit {
	return client
}

func Init(cfg Config, safeExit *exit.SafeExit) (err error) {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}

	if cfg.RetryBaseInterval <= 0 {
		cfg.RetryBaseInterval = 1
	}

	if cfg.PublisherPoolSize <= 0 {
		cfg.PublisherPoolSize = 3
	}

	if cfg.PublisherPoolSize > 200 {
		cfg.PublisherPoolSize = 200
	}

	conn, err := createConnectionWithRetry(cfg)
	if err != nil {
		return err
	}

	client = &Rabbit{
		conn:      conn,
		config:    cfg,
		closeChan: make(chan struct{}),
		queues:    make(map[string]struct{}),
		safeExit:  safeExit,
	}
	// 初始化信号灯
	// 修改：Cond 绑定已有的读写锁（mu 是 RWMutex，底层包含 Mutex）
	// 注意：Cond 需要 Locker 接口，mu.RLocker() 或 mu 都可以，这里推荐绑定 mu
	client.reconnectCond = sync.NewCond(&client.mu)

	client.pubPool = client.newChannelPool()
	client.notifyBlocked()

	go client.monitorConnection()

	// 注册全局优雅退出处理
	client.safeExit.WG.Add(1)
	go client.gracefulShutdown()
	return nil
}

// gracefulShutdown 优雅关闭处理
func (r *Rabbit) gracefulShutdown() {
	defer r.safeExit.WG.Done()      // 步骤1：通知全局组，本协调员任务已结束
	<-r.safeExit.StopContext.Done() // 阻塞，直到收到停止信号

	log.Println("rabbitmq-log:mq 接收到退出信号，正在停止新消息流入...")

	r.shuttingDown.Store(true) // 步骤2：阻止新任务提交

	// 步骤3：等待所有已提交任务完成 (依赖内部组 poolSafeWG)
	done := make(chan struct{})
	go func() {
		r.consumeWG.Wait()
		close(done)
	}()

	// 步骤4：带超时等待
	select {
	case <-done:
		log.Println("rabbitmq-log:所有mq消费任务已完成")
	case <-time.After(30 * time.Second):
		log.Println("rabbitmq-log:mq消费任务等待超时，强制退出")
	}
	// 最后再关闭连接
	r.Close()
	log.Println("rabbitmq-log:mq消费者已关闭")
}

// 创建带重试的连接
func createConnectionWithRetry(cfg Config) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error
	// 核心修改：使用 DialConfig 替代 Dial，并设置心跳
	amqpCfg := amqp.Config{
		Heartbeat: 30 * time.Second, // 30秒心跳，检测半开连接
		// Dial:      amqp.DefaultDial(time.Second * 10), // 建立连接的超时时间
		Dial: func(network, addr string) (net.Conn, error) {
			conn, err := net.DialTimeout(network, addr, 10*time.Second)
			if err != nil {
				return nil, err
			}
			// 关键：设置 TCP 层面的 KeepAlive，配合 AMQP 心跳双重保险
			if tc, ok := conn.(*net.TCPConn); ok {
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(30 * time.Second)
			}
			return conn, nil
		},
		Properties: amqp.Table{"connection_name": cfg.ConnectName},
	}

	conn, err = amqp.DialConfig(cfg.URL, amqpCfg)
	if err == nil {
		log.Printf("rabbitmq-log:Connection success\n")
		return conn, nil
	}

	// 第一阶段：指数退避重试
	for i := 0; i < cfg.MaxRetries; i++ {
		conn, err = amqp.DialConfig(cfg.URL, amqpCfg)
		if err == nil {
			log.Printf("rabbitmq-log:Connection established after %d total attempts\n", i+1)
			return conn, nil
		}

		waitTime := time.Duration(math.Pow(2, float64(i))) *
			time.Duration(cfg.RetryBaseInterval) * time.Second
		log.Printf("rabbitmq-log:Connection attempt %d failed, retrying in %v: %v\n", i+1, waitTime, err)
		time.Sleep(waitTime)
	}

	// 第二阶段：超过重试次数后，每分钟重试一次，直到成功
	retryCount := cfg.MaxRetries
	for {
		conn, err = amqp.DialConfig(cfg.URL, amqpCfg)
		if err == nil {
			log.Printf("rabbitmq-log:Connection established after %d total attempts\n", retryCount+1)
			return conn, nil
		}

		retryCount++
		log.Printf("rabbitmq-log:Connection attempt %d failed, retrying in 1 minute: %v\n", retryCount, err)
		time.Sleep(1 * time.Minute)
	}

	// return nil, fmt.Errorf("failed to connect after %d attempts: %w", cfg.MaxRetries, err)
}

// channelPool 通道池实现，用于复用 AMQP 通道
type channelPool struct {
	channels chan *amqp.Channel // 通道池
	conn     *amqp.Connection   // 关联的 AMQP 连接
}

func (r *Rabbit) newChannelPool() *channelPool {
	pool := &channelPool{
		channels: make(chan *amqp.Channel, r.config.PublisherPoolSize),
		conn:     r.conn,
	}

	// 初始化通道
	for i := 0; i < r.config.PublisherPoolSize; i++ {
		ch, err := r.createChannelWithTimeout(5 * time.Second)
		if err != nil {
			log.Printf("rabbitmq-log:Error creating channel: %v\n", err)
			continue
		}
		pool.channels <- ch
	}

	return pool
}

// Block监控
func (r *Rabbit) notifyBlocked() {
	// 增加监听，打印报警日志
	blocked := make(chan amqp.Blocking)
	r.conn.NotifyBlocked(blocked)
	go func() {
		for b := range blocked {
			if b.Active {
				log.Printf("rabbitmq-log:警告！服务器资源不足，连接被阻塞: %s", b.Reason)
			} else {
				log.Printf("rabbitmq-log:连接阻塞已解除")
			}
		}
		log.Printf("rabbitmq-log:连接已断开，停止阻塞状态监听") // 验证协程是否销毁
	}()
}

// 抽取公用的带超时创建函数
func (r *Rabbit) createChannelWithTimeout(timeout time.Duration) (*amqp.Channel, error) {
	type res struct {
		ch  *amqp.Channel
		err error
	}
	done := make(chan res, 1)

	go func() {
		ch, err := r.conn.Channel()
		if err != nil {
			log.Printf("rabbitmq-log:Error creating channel: %v\n", err)
		} else {
			// 只有配置开启时才启用 Confirm 模式
			if r.config.EnablePublisherConfirm {
				// 开启 Confirm 生产者确认模式：确保消息不丢失。
				// 注意：必须配合 select timeout 使用，防止网络假死导致程序永久阻塞。
				// 处理：超时则销毁通道，触发重连，保障系统高可用。
				// Publish方法结合 confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1)) 处理。
				err = ch.Confirm(false) // 开启确认模式
				if err != nil {
					log.Printf("rabbitmq-log:开启Publisher Confirm模式失败 err:%v\n", err)
				} else {
					log.Println("rabbitmq-log:已开启Publisher Confirm模式")
				}
			}
		}
		done <- res{ch, err}
	}()

	select {
	case <-time.After(timeout):
		// 2. 关键处理：超时后启动一个“清理者”
		go func() {
			// 异步等待那个可能迟到的通道
			r := <-done
			if r.err == nil && r.ch != nil {
				log.Println("rabbitmq-log:关闭Get()超时后创建的延迟通道")
				r.ch.Close() // 关掉它，不让它泄露
			}
		}()
		return nil, errors.New("从连接创建新通道超时")
	case r := <-done:
		return r.ch, r.err
	}
}

// GetChannel 获取一个通道
func (r *Rabbit) GetChannel() (*amqp.Channel, error) {
	for {
		// 1. 检查重连状态
		if atomic.LoadInt32(&r.isConnecting) == 1 {
			r.reconnectCond.L.Lock()
			for atomic.LoadInt32(&r.isConnecting) == 1 {
				log.Println("rabbitmq-log: 连接恢复中，请求排队...")
				r.reconnectCond.Wait()
			}
			r.reconnectCond.L.Unlock()
		}

		// 2. 获取当前的池子（受读写锁保护，防止在获取瞬间被替换）
		r.mu.RLock()
		pool := r.pubPool
		r.mu.RUnlock()

		if pool == nil {
			return nil, errors.New("通道池未初始化")
		}

		// 3. 尝试从池中拿
		select {
		case ch, ok := <-pool.channels:
			if ok {
				if !ch.IsClosed() {
					return ch, nil
				}
				// 通道碎了，去新建
			} else {
				// 池子被关闭了（说明重连了），重新循环拿新池子
				continue
			}
		default:
			// 池子空了，去新建
		}

		// 4. 新建通道
		ch, err := r.createChannelWithTimeout(5 * time.Second)
		if err != nil {
			log.Printf("rabbitmq-log:创建通道失败: %v", err)
			// 如果创建失败是因为连接断了，触发重连
			if errors.Is(err, amqp.ErrClosed) || strings.Contains(err.Error(), "closed") {
				if atomic.CompareAndSwapInt32(&r.isConnecting, 0, 1) {
					r.conn.Close()
				}
			}
			// 建议在这里加一个微小的 Sleep
			time.Sleep(100 * time.Millisecond)
			// 重试循环
			continue
		}
		return ch, nil
	}
}

func (r *Rabbit) PutChannel(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}

	r.mu.RLock()
	pool := r.pubPool
	r.mu.RUnlock()

	if pool == nil { // 防御性检查
		ch.Close()
		return
	}

	// 增加 recover 机制，防止 Put 时池子刚好被 Close 导致的 panic
	defer func() {
		if recover() != nil {
			ch.Close()
		}
	}()

	select {
	case pool.channels <- ch:
	default:
		ch.Close() // 池子满了
	}
}

// 监控连接状态
func (r *Rabbit) monitorConnection() {
	notifyClose := r.conn.NotifyClose(make(chan *amqp.Error))
	// 增加一个定时器，每 5 分钟尝试检查一次连接状态（主动心跳辅助）
	// 即使 NotifyClose 没反应，这里的逻辑也能作为兜底
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-r.closeChan:
			log.Println("rabbitmq-log:收到手动关闭信号，停止监控")
			return
		case err, ok := <-notifyClose:
			if !ok {
				log.Println("rabbitmq-log:连接通知通道已关闭，尝试重连...")
			} else {
				log.Printf("rabbitmq-log:检测到连接断开: %v\n", err)
			}
			r.reconnect()
			return // 重连后退出旧的监控，新的在 reconnect 里启动
		case <-ticker.C:
			// 主动探测：如果此时连接已经 Closed 却没触发上面的 case
			if r.conn.IsClosed() {
				log.Println("rabbitmq-log:主动巡检发现连接已断开，触发重连")
				r.reconnect()
				return
			}
		}
	}
}

// 重连处理
func (r *Rabbit) reconnect() {
	// 如果正在关闭，直接退出，不要再重连
	if r.shuttingDown.Load() {
		log.Println("rabbitmq-log:检测到客户端正在关闭，跳过重新连接")
		return
	}

	// 确保状态为 1（正在重连）
	atomic.StoreInt32(&r.isConnecting, 1)
	defer func() {
		atomic.StoreInt32(&r.isConnecting, 0)
		r.reconnectCond.L.Lock()
		r.reconnectCond.Broadcast() // 无论成功失败，都得让排队的人出来（失败了他们会再次触发重连）
		r.reconnectCond.L.Unlock()
	}()

	r.mu.Lock()

	log.Println("rabbitmq-log:正在重建连接资源...")

	// 1. 备份旧资源
	oldConn := r.conn
	oldPool := r.pubPool

	// 2. 创建新连接
	newConn, err := createConnectionWithRetry(r.config)
	if err != nil {
		r.mu.Unlock() // 记得解锁
		log.Printf("rabbitmq-log:重连彻底失败: %v。业务请求将被放行并触发下一轮探测。", err)
		return
	}

	// 3. 替换新资源
	r.conn = newConn
	r.pubPool = r.newChannelPool()
	r.notifyBlocked()

	// 4. 重置队列缓存
	// 清空已声明队列记录，因为连接已重建
	r.queueMu.Lock()
	r.queues = make(map[string]struct{})
	r.queueMu.Unlock()

	r.mu.Unlock() // 此时新连接已就绪，可以解锁让发布者使用了

	// 5. 异步优雅关闭旧资源 为什么要异步？因为 Close() 可能会因为网络 IO 阻塞一会儿，不能卡住重连流程
	go clearOldConn(oldConn, oldPool)
	// 消费者核心恢复逻辑
	go r.consumerRegistryFunc()
	// 重新监控
	go r.monitorConnection()

	log.Println("rabbitmq-log:重连成功，已唤醒业务请求。")
}

// 异步优雅关闭旧资源
func clearOldConn(conn *amqp.Connection, pool *channelPool) {
	log.Println("rabbitmq-log:开始清理旧连接资源...")

	// 给清理操作设置一个总超时，防止因为网络 IO 导致 goroutine 永久挂起
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	cleanupDone := make(chan struct{})

	go func() {
		if pool != nil {
			close(pool.channels)
			for ch := range pool.channels {
				if !ch.IsClosed() {
					_ = ch.Close()
				}
			}
		}
		if conn != nil && !conn.IsClosed() {
			_ = conn.Close()
		}
		close(cleanupDone)
	}()

	select {
	case <-cleanupDone:
		log.Println("rabbitmq-log:旧资源已优雅回收")
	case <-timer.C:
		log.Println("rabbitmq-log:清理旧资源超时，强制放弃（可能连接已完全僵死）")
	}
	log.Println("rabbitmq-log:已成功清理旧连接资源")
}

// 消费者核心恢复逻辑
func (r *Rabbit) consumerRegistryFunc() {
	r.consumerRegistry.Range(func(key, value any) bool {
		state := value.(*ConsumerState)
		// 使用一个新的 goroutine 重新启动消费者，避免阻塞重连逻辑
		go func() {
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond) // 随机错峰 0-200ms
			log.Printf("rabbitmq-log:尝试恢复消费者: %s\n", key)
			// 重新声明队列（因为连接断开，声明可能丢失）
			// 注意：这里假设所有消费者都需要持久化队列（工作队列模式）
			r.DeclareQueue(&QueueOption{
				Name:    state.Option.Queue,
				Durable: true,
			})

			// 重新调用 Consume。由于 Consume 内部会创建新通道、设置 QoS 并启动新的监听循环，
			// 且注册表已存在，因此这是安全的。
			if err := r.Consume(context.Background(), state.Option, state.Handler); err != nil {
				log.Printf("rabbitmq-log:恢复消费者失败 %s: %v\n", key, err)
				// 恢复失败，从注册表中删除 (可选，如果希望永久失败则删除)
				// r.consumerRegistry.Delete(key)
			}
		}()
		return true // 继续迭代下一个
	})
}

// Close 关闭客户端
func (r *Rabbit) Close() {
	close(r.closeChan)

	r.mu.Lock()
	defer r.mu.Unlock()

	close(r.pubPool.channels)

	for ch := range r.pubPool.channels {
		ch.Close()
	}

	if r.conn != nil {
		r.conn.Close()
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

func (r *Rabbit) DeclareExchange(opt ExchangeOption) (err error) {
	ch, err := r.GetChannel()
	if err != nil {
		return err
	}
	var channelClosed bool
	// 2. 归还逻辑
	defer func() {
		if ch != nil {
			if channelClosed {
				ch.Close()
				return
			}
			if !ch.IsClosed() {
				r.PutChannel(ch)
			}
		}
	}()

	err = ch.ExchangeDeclare(
		opt.Name,
		opt.Kind,
		opt.Durable,
		opt.AutoDelete,
		opt.Internal,
		opt.NoWait,
		opt.Args,
	)
	if err != nil {
		log.Printf("rabbitmq-log:声明交换机失败: name:%s,err:%v\n", opt.Name, err)
		channelClosed = true
		return err
	}
	return nil
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

func (r *Rabbit) DeclareQueue(opt *QueueOption) error {
	// 1. 读锁检查 (只对用户指定的命名队列进行缓存检查)
	if opt.Name != "" {
		r.queueMu.RLock()
		if _, exists := r.queues[opt.Name]; exists {
			r.queueMu.RUnlock()
			return nil // 本地命中，性能优化成功
		}
		r.queueMu.RUnlock()
	}

	// 获取通道
	ch, err := r.GetChannel()
	if err != nil {
		return err
	}
	var channelClosed bool
	// 2. 归还逻辑
	defer func() {
		if ch != nil {
			if channelClosed {
				ch.Close()
				return
			}
			if !ch.IsClosed() {
				r.PutChannel(ch)
			}
		}
	}()

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
		log.Printf("rabbitmq-log:队列声明失败: queue:%s,err:%v\n", opt.Name, err)
		channelClosed = true
		return err
	}

	log.Printf("rabbitmq-log:队列声明 %s\n", queue.Name)

	// 3. 回填队列名 (必须保留，因为 BindQueue 和 Consume 需要实际名字)
	if opt.Name == "" {
		opt.Name = queue.Name
	}

	// 4. 改进的记录逻辑：仅记录持久队列或非自动删除队列
	// 临时队列具有 AutoDelete=true 且 Durable=false 的特性。
	// 我们只记录那些会长期存在的队列。
	if opt.Durable || !opt.AutoDelete {
		r.queueMu.Lock()
		// 确保使用回填后的 opt.Name
		r.queues[opt.Name] = struct{}{}
		r.queueMu.Unlock()
	}
	return nil
}

// BindQueue 队列绑定
func (r *Rabbit) BindQueue(queue, key, exchange string) (err error) {
	// 检查队列名称是否为空
	if queue == "" {
		return fmt.Errorf("队列名不可为空")
	}
	ch, err := r.GetChannel()
	if err != nil {
		return err
	}
	var channelClosed bool
	// 2. 归还逻辑
	defer func() {
		if ch != nil {
			if channelClosed {
				ch.Close()
				return
			}
			if !ch.IsClosed() {
				r.PutChannel(ch)
			}
		}
	}()

	err = ch.QueueBind(
		queue,
		key,
		exchange,
		false, // noWait
		nil,
	)
	if err != nil {
		log.Printf("rabbitmq-log:队列绑定失败: queue:%s,key:%s,exchange:%s,err:%v\n", queue, key, exchange, err)
		channelClosed = true
		return err
	}
	return nil
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

func (r *Rabbit) Publish(ctx context.Context, opt *PublishOption, body []byte) (err error) {
	// 增加重试循环：一共尝试 7 次
	for i := 0; i < 7; i++ {
		// 这里直接调用你原来的业务逻辑（或者封装好的 doPublish）
		err = r.doPublish(ctx, opt, body)
		if err == nil {
			return nil
		}

		// 只要发生错误且还没到最后一次，就重试
		if i < 6 {
			// 指数退避：1s, 2s, 4s, 8s, 16s, 32s
			waitTime := time.Duration(1<<i) * time.Second
			log.Printf("rabbitmq-log:发送失败(第%d次), %v 后重试: %v", i+1, waitTime, err)

			select {
			case <-time.After(waitTime):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}

func (r *Rabbit) doPublish(ctx context.Context, opt *PublishOption, body []byte) (err error) {
	log.Printf("rabbitmq-log:Publish info for Exchange:%s RoutingKey:%s body:%s\n", opt.Exchange, opt.RoutingKey, string(body))
	if opt.RoutingKey == "" && opt.Exchange == "" {
		return fmt.Errorf("路由键和交换机名称不能同时为空")
	}
	// 只有在直接发送到队列（exchange为空）时才声明队列
	if opt.Exchange == "" && opt.RoutingKey != "" {
		// 声明队列
		err = r.DeclareQueue(&QueueOption{Name: opt.RoutingKey, Durable: true})
		if err != nil {
			log.Printf("rabbitmq-log:Publish failed for Exchange:%s RoutingKey:%s body:%s err:%v\n", opt.Exchange, opt.RoutingKey, string(body), err)
			return err
		}
	}
	// 发布者应该用池
	// 发布者 (Publisher) 的操作是无状态的：它只是短暂地获取一个通道，发送消息，然后立即归还。
	// 1.高并发/高吞吐：通道池能有效应对高频、突发的发布请求，通过复用通道来减少创建/关闭的开销，提高性能。
	// 2.无状态性：发布操作不需要设置 Qos 等会污染通道状态的配置，池中的通道可以安全地被复用。
	// 1. 获取通道
	ch, err := r.GetChannel()
	if err != nil {
		log.Printf("rabbitmq-log:Publish failed for Exchange:%s RoutingKey:%s body:%s err:%v\n", opt.Exchange, opt.RoutingKey, string(body), err)
		return err
	}
	var channelClosed bool
	// 2. 归还逻辑
	defer func() {
		if ch != nil {
			if channelClosed || ch.IsClosed() {
				ch.Close()
			} else {
				r.PutChannel(ch)
			}
		}
	}()

	deliveryMode := amqp.Transient
	if opt.Persistent {
		deliveryMode = amqp.Persistent
	}

	// 如果没开启 Confirm 模式，直接走简单发送流程
	if !r.config.EnablePublisherConfirm {
		return ch.PublishWithContext(ctx, opt.Exchange, opt.RoutingKey, opt.Mandatory, opt.Immediate,
			amqp.Publishing{
				ContentType:  opt.ContentType,
				Body:         body,
				DeliveryMode: deliveryMode,
			})
	}

	// --- 以下是开启 Confirm 模式后的逻辑 ---
	// 3. 准备监听确认
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// 4. 发送消息
	err = ch.PublishWithContext(
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
	if err != nil {
		log.Printf("rabbitmq-log:Publish failed for Exchange:%s RoutingKey:%s body:%s err:%v\n", opt.Exchange, opt.RoutingKey, string(body), err)
		return err
	}
	// 等待确认 等待确认（必须加超时！）否则如果连接半路断了，这里会永久阻塞，导致出现“无日志无报错”现象
	select {
	case <-ctx.Done():
		return ctx.Err()
	case confirm, ok := <-confirms: // 注意：confirm模式必须配合 select timeout 使用，防止网络假死导致程序永久阻塞。
		if !ok {
			channelClosed = true
			return fmt.Errorf("confirm channel closed")
		}
		if confirm.Ack {
			return nil // 消息确认到达 Broker
		}
		return fmt.Errorf("message nack-ed by broker")
	case <-time.After(5 * time.Second): // 最后的逃生门
		channelClosed = true
		return errors.New("rabbitmq wait confirm timeout")
	}
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

func (r *Rabbit) Consume(ctx context.Context, opt *ConsumeOption, handler func(amqp.Delivery)) error {
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
	r.consumerRegistry.Store(key, &ConsumerState{
		Option:  opt,
		Handler: handler,
	})

	// 消费者不应该用池
	// 消费者 (Consumer) 的操作是有状态和长期的。
	// 1.QoS 配置：消费者需要设置 prefetchCount (QoS) 来控制消息流，这是一个通道级别的状态。如果通道被归还并复用，会污染其他消费者的设置。
	// 2.长期持有：消费者需要长期独占一个通道来接收消息流，直到程序关闭或连接断开。

	// 1.直接从 Connection 创建新的专用通道
	ch, err := r.conn.Channel()
	if err != nil {
		// 启动失败，移除注册
		r.consumerRegistry.Delete(key)
		return err
	}

	var prefetchCount = PrefetchCount
	// 2. 根据 PrefetchCount 设置 QoS (QoS只对当前专用通道生效)
	if opt.PrefetchCount > 0 {
		prefetchCount = opt.PrefetchCount
	}
	err = ch.Qos(
		// 每次预取一个任务 prefetchCount是每个消费者在预取消息的数量，比如设置为10的话，消费者会一次性获取10条消息，处理完再取下一批。
		// 这样能提高吞吐量，但可能增加内存使用。如果设置为1，就是每次处理完一条再取下一条，更公平但可能降低效率。0（无限制，即尽可能多预取）。
		prefetchCount,
		// 不限制消息总数 prefetchsize是预取消息的总大小，以字节为单位。比如设置为1024的话，消费者最多预取总大小不超过1KB的消息。通常设置为 0 表示不限制
		0,
		// 应用在当前通道 决定 QoS 设置是否作用于当前 Channel 的所有消费者。通常设置为 false
		false,
	)
	if err != nil {
		ch.Close()
		r.consumerRegistry.Delete(key)
		return fmt.Errorf("set QoS failed: %w", err)
	}
	log.Printf("rabbitmq-log:Consumer for queue %s set QoS prefetch=%d\n", opt.Queue, opt.PrefetchCount)

	// 创建一个派生自全局 StopContext 的上下文，或者直接使用 StopContext
	// 这样当 exit.StopContext 取消时，RabbitMQ 的消费监听会自动停止
	consumerCtx, cancel := context.WithCancel(r.safeExit.StopContext)

	deliveries, err := ch.ConsumeWithContext(
		consumerCtx,
		opt.Queue,
		opt.Consumer,
		opt.AutoAck,
		opt.Exclusive,
		opt.NoLocal,
		opt.NoWait,
		opt.Args,
	)
	if err != nil {
		cancel()
		// 如果消费启动失败，关闭创建的通道
		ch.Close()
		r.consumerRegistry.Delete(key)
		return err
	}

	// 创建一个局部的 WG 跟踪此通道发出的任务 定义在每个 Consume 调用内部，用来保护 ch (Channel) 不被提前关闭，确保 Ack 能发出去。 和每个消费者绑定，consumeWG是全局控制所有消费者的，不适合用在这里
	// 由于消息处理是异步的，在 range deliveries 结束后立即执行了 defer ch.Close()。
	// 如果此时异步的处理协程还没执行完 handler(msg)（包含里面的 Ack），通道（Channel）就已经被关闭了，后续的 msg.Ack() 将会报错：channel already closed
	var localWG sync.WaitGroup

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println("rabbitmq-log:Consumer panic:", r)
			}
			// 等待本通道发出的所有异步任务处理完
			localWG.Wait()
			cancel() // 确保退出时释放资源
			// 当 deliveries channel 关闭时（通常是因为连接断开），关闭通道
			ch.Close()
			log.Println("rabbitmq-log:Delivery channel closed, Dedicated channel closing....")
			// ** 不从注册表删除 **：通道关闭通常意味着连接断开，此时需要等待 reconnect 逻辑来重建该消费者
		}()
		for d := range deliveries {
			// 风险： 假设此时 StopContext 已经触发，deliveries 刚读取出最后一条消息，循环结束。在执行 consumeWG.Add(1) 之前，主协程的 consumeWG.Wait() 可能已经因为之前的任务刚好清零而穿透了。
			// 改进方案
			// 在派生处理协程之前，先检查上下文状态，并确保 Add 操作在循环内是安全的。
			// 检查是否已经关闭，避免在关闭瞬间还在开新协程
			select {
			case <-consumerCtx.Done():
				// 如果已取消，拒绝处理这条消息（让它由 mq 重新投递或丢弃）
				d.Nack(false, true)
				continue
			default:
			}
			// 此时不需要在这里判断 shuttingDown.Load()
			// 因为 Context 取消后，deliveries 通道会被 RabbitMQ 驱动关闭
			// if shuttingDown.Load() {
			// 	log.Printf("程序退出，消费者停止监听MQ。\n")
			// 	return
			// }
			// 增加等待组计数
			r.consumeWG.Add(1)
			localWG.Add(1) // 局部计数
			// 包装任务函数，确保资源清理
			go func(msg amqp.Delivery) { // 建议开启协程处理，不阻塞监听
				defer r.consumeWG.Done()
				defer localWG.Done() // 处理完任务减少局部计数
				handler(msg)
			}(d)
		}
	}()

	return nil
}
