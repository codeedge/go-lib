package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

/*
🎯 核心价值
SSE (Server-Sent Events) 服务包是一个基于 Go 语言的库，专为现代分布式系统设计。它使服务器能通过一个持续的 HTTP 连接向客户端（如浏览器）主动、单向地
实时推送数据（如通知、消息、实时指标）。其最大特点是原生支持集群部署和跨项目/服务的安全消息推送，无需客户端轮询，非常适合微服务架构。

📦 核心功能
功能特性						说明
集群支持 (Cluster Support)	多节点部署，通过 Redis 同步会话和路由消息，实现高可用和水平扩展。
跨服务推送 (Cross-Service)	服务A（如运营平台）可直接、精准地向服务B（如客户端）的特定用户或所有用户推送消息。
离线消息 (Offline Support)	用户不在线时，消息自动持久化至 Redis。用户重连后，立即接收错过的消息。
多端登录 (Multi-Device)		支持同一用户（userId）在多个设备（uuid）同时在线并接收消息。

其基本工作原理如下，确保了消息的精准投递：

🛠️ 如何使用
1.初始化：全部参数收敛到Config，未指定的字段用默认值。
sseService := sse.New(&sse.Config{
    Rds:               redisClient,
    NodeId:            1,
    ServerName:        "my-app",
    RemoteServerNames: []string{"operation-platform", "client-app"},
    // 可选调节项（不传用默认值）：
    // HeartbeatInterval: 30 * time.Second,
    // OfflineQueueTTL:   7 * 24 * time.Hour,
})
2.处理客户端连接：在 Gin 路由中提供一个端点。
router.GET("/sse", sseService.Connect)
// 或一次性注册全部SSE路由（连接、统计、推送测试），需挂在鉴权中间件之后：
sseGroup := router.Group("sse") // /sse、/sse/stats、/sse/test_push
sseService.RegisterRoutes(sseGroup)
3.发送消息：
// 1. 向本服务的用户123发送消息
sseService.SendToUser(123, "Hello!", 1)
// 2. 向本服务所有用户广播
sseService.BroadcastMessage("Hello, everyone!", 2)
// 3. 向远程服务 "client-app" 的用户123发送消息
sseService.SendToUser(123, "Cross-service msg", 1, "client-app")
// 4. 向远程服务 "operation-platform" 广播消息
sseService.BroadcastMessage("Broadcast to all", 3, "operation-platform")
⚙️ 部署注意
Redis：所有节点和服务必须连接同一个 Redis 实例或集群，这是功能基础。
反向代理 (Nginx)：若使用 Nginx，必须为 SSE 路径配置长连接与禁用缓冲：
location /sse/ {
    proxy_pass http://your_upstream;
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 86400s;
}
监控：关注 Redis 内存和节点连接数。
💎 总结
此 SSE 服务包是构建实时通知系统（如聊天消息、订单状态更新）、实时数据看板（如监控指标、股票行情）和跨服务实时通信的理想选择。它基于 HTTP 标准，开箱即用，
并能通过集群和精准的路由能力，支撑大规模、分布式的应用场景。
*/

// Config New的全部参数
// 必填：Rds、ServerName、NodeId；可选：其余字段零值自动取默认值
type Config struct {
	NodeId            int                   // 必填 节点id
	ServerName        string                // 必填 当前项目服务名，用作redisPrefix
	RemoteServerNames []string              // 允许推送的远程项目服务名列表
	Rds               redis.UniversalClient // 必填 Redis客户端
	SessionTTL        time.Duration         // 会话有效期（Redis会话key的TTL，由心跳续期） 默认5min 需≥2倍HeartbeatInterval
	HeartbeatInterval time.Duration         // 心跳间隔：续Redis会话TTL + 向客户端写ping帧 默认60s
	SlowClientTimeout time.Duration         // 单条消息投递超时，超时踢掉慢客户端防止拖死服务 默认3s
	OfflineQueueTTL   time.Duration         // 离线消息队列过期时间，每次写入刷新 默认30天
	MessageChanSize   int                   // 客户端消息缓冲大小 默认100
}

type Service struct {
	cfg                          Config                       // 归一化后的配置（零值已在New中填充为默认值）
	clients                      map[int64]map[string]*Client // 存储所有本地客户端连接 map[uid]map[uuid]*Client
	clientsSync                  sync.RWMutex                 // 读写锁
	ctx                          context.Context              // 可取消的上下文，Close时通知订阅协程退出
	cancel                       context.CancelFunc           // 配合ctx使用
	redisSessionKeyPattern       string                       // 会话存储 记录用户-客户端对应的节点id 用于快速查找用户节点 key是uid:uuid val是node
	redisSessionSetKey           string                       // 节点客户端集合 用于清理节点客户端 key是固定值+node val是uid:uuid
	redisUserUuidSetKeyPattern   string                       // 用户-客户端集合Key 记录用户所有的客户端id，单推时查找用户所有的客户端进行推送 key是uid，val是uuid
	redisPubSubChannelKeyPattern string                       // 发布订阅key key拼接的是node，如果是-1代表广播
	redisOfflineQueueKeyPattern  string                       // 离线消息队列Key key是uid val是消息体
	redisOnlineCountKeyPattern   string                       // 在线连接数Key key是nodeId val是连接数，注册/注销时写入，查询端汇总
	stopHeartbeat                chan struct{}                // 停止心跳信号
}

// Client 表示SSE客户端连接
type Client struct {
	userId      int64
	UUID        string
	messageChan chan string   // 待发送的原始JSON消息体，SSE编码统一由Connect完成
	done        chan struct{} // 关闭即通知Connect的Stream退出；messageChan永不close，避免向已关闭的channel发送panic
	closeOnce   sync.Once     // 保证done只被关闭一次
}

// Message 集群节点间传输的消息格式
type Message struct {
	UserId     int64  `json:"user_id"`     // 用户id
	UUID       string `json:"uuid"`        // 设备唯一id
	NodeId     int    `json:"node_id"`     // 服务器节点Id
	ServerName string `json:"server_name"` // 服务名
	Type       int    `json:"type"`        // 类型
	Data       any    `json:"data"`        // 数据
	Timestamp  int64  `json:"timestamp"`   // 时间戳
}

// pingMsg messageChan的心跳哨兵值：空字符串不代表数据，Connect收到后写入SSE注释帧（浏览器忽略，仅让连接上有字节流动）
const pingMsg = ""

// New 创建SSE服务实例，全部参数收敛到Config
// 配置非法时直接panic，属于部署期配置错误，应让进程立即失败而不是带病运行
func New(cfg *Config) *Service {
	c := mustNormalizeConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		cfg:     c,
		clients: make(map[int64]map[string]*Client),
		ctx:     ctx,
		// 1. 对于需要动态生成的Key，定义其**模式**（包含所有占位符）
		redisSessionKeyPattern:       "%s:sse:session:%d:%s",   // 模式: [serviceName]:sse:session:[userId]:[uuid]
		redisUserUuidSetKeyPattern:   "%s:sse:user:clients:%d", // 模式: [serviceName]:sse:user:clients:[userId]
		redisPubSubChannelKeyPattern: "%s:sse:cluster:%d",      // 模式: [serviceName]:sse:cluster:[node]
		redisOfflineQueueKeyPattern:  "%s:sse:offline-msg:%d",  // 模式: [serviceName]:sse:offline-msg:[userId]
		redisOnlineCountKeyPattern:   "%s:sse:node:online:%d",  // 模式: [serviceName]:sse:node:online:[nodeId]
		// 2. 对于纯粹本地的、与节点绑定的Key，直接**写死**
		redisSessionSetKey: fmt.Sprintf("%s:sse:node:clients:%d", c.ServerName, c.NodeId), // 直接生成最终字符串
		stopHeartbeat:      make(chan struct{}),
		cancel:             cancel,
	}

	// 初始化时清理redis本节点的会话记录
	// 必须同步执行：清理会删除节点花名册里记录的所有客户端，异步跑的话可能晚于客户端注册，
	// 把刚写入的会话误清掉——该客户端连接还在但路由层认为其离线，消息会误落离线队列且不会自愈
	s.cleanupStaleSessions()
	// 订阅本节点专属频道
	SafeGoWithRestart("subscribeNodeChannel", s.subscribeNodeChannel, 3, 10*time.Second)
	// 订阅全局控制频道（用于广播）
	SafeGoWithRestart("subscribeAllChannel", s.subscribeAllChannel, 3, 10*time.Second)
	// 启动心跳协程
	SafeGoWithRestart("heartbeat", s.heartbeat, 3, 10*time.Second)
	return s
}

// mustNormalizeConfig 复制配置、校验必填项并将零值字段填充为默认值；校验失败panic
func mustNormalizeConfig(cfg *Config) Config {
	if cfg == nil {
		panic("sse: config is nil")
	}
	if cfg.Rds == nil {
		panic("sse: Rds is required")
	}
	if cfg.ServerName == "" {
		panic("sse: ServerName is required")
	}
	if cfg.NodeId < 0 {
		panic(fmt.Sprintf("sse: NodeId must be >= 0, got %d", cfg.NodeId))
	}
	if cfg.SessionTTL > 0 && cfg.HeartbeatInterval > 0 && cfg.SessionTTL < 2*cfg.HeartbeatInterval {
		panic(fmt.Sprintf("sse: SessionTTL(%v) must be >= 2x HeartbeatInterval(%v)", cfg.SessionTTL, cfg.HeartbeatInterval))
	}

	c := *cfg
	if c.SessionTTL <= 0 {
		c.SessionTTL = 5 * time.Minute // 会话有效期 默认5min
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 60 * time.Second // 心跳间隔 默认60s
	}
	if c.SlowClientTimeout <= 0 {
		c.SlowClientTimeout = 3 * time.Second // 单条消息投递超时 默认3s
	}
	if c.OfflineQueueTTL <= 0 {
		c.OfflineQueueTTL = 30 * 24 * time.Hour // 离线队列过期时间 默认30天
	}
	if c.MessageChanSize <= 0 {
		c.MessageChanSize = 100 // 消息缓冲大小 默认100
	}
	return c
}

// getRedisKey 根据指定的模式生成完整的 Redis Key。
// serviceName: 目标服务名前缀，用于填充第一个 %s。
// keyPattern: 键的模式字符串，如 s.redisSessionKeyPattern。
// args: 可变参数，按顺序填充模式中的后续占位符（如 %d, %s）。
func (s *Service) getRedisKey(serviceName, keyPattern string, args ...interface{}) string {
	// 将所有参数组合：第一个是 serviceName，后面是 args
	allArgs := make([]interface{}, 0, len(args)+1)
	allArgs = append(allArgs, serviceName)
	allArgs = append(allArgs, args...)

	// 使用所有参数来格式化模式字符串
	return fmt.Sprintf(keyPattern, allArgs...)
}

// Connect SSE连接
func (s *Service) Connect(c *gin.Context) {
	/*
		# SSE接口超时时间特殊配置
		location /sse/ {
			proxy_pass http://your_gin_app_upstream;

							# 🔥 关键：为 SSE 设置极长的读取超时

							proxy_read_timeout 3600s; # 1小时，或更长如 86400s(24小时)

							# 同样建议禁用缓冲，确保消息实时推送
			proxy_buffering off;
			proxy_cache off;

							# ... 其他代理设置 ...
		}
	*/
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")

	// userId由路由上的鉴权中间件（JWT等）注入；未挂鉴权中间件时拒绝连接
	userId := c.GetInt64("user_id")
	if userId <= 0 {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}

	// 创建新客户端
	uuid := c.Query("uuid")
	if uuid == "" {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("uuid is empty"))
		return
	}
	// 预留支持用户多端登录，通过userid:uuid作为客户端id
	// clientId = fmt.Sprintf("%d:%s", userId, clientId)
	client := &Client{
		userId:      userId,
		UUID:        uuid,
		messageChan: make(chan string, s.cfg.MessageChanSize),
		done:        make(chan struct{}),
	}

	// 注册客户端
	s.registerClient(client)

	// 立即写注释帧并Flush：否则响应头要等第一条消息（最长一个心跳周期60s）才会发出，
	// 浏览器侧表现为连接建立卡顿（实测约31秒）
	c.Writer.WriteString(": connected\n\n")
	c.Writer.Flush()

	// 使用 Stream API 替代手动循环
	c.Stream(func(w io.Writer) bool {
		select {
		case msg := <-client.messageChan:
			if msg == pingMsg {
				// 心跳哨兵：写SSE注释帧，浏览器自动忽略，仅保持连接上有字节流动
				sse.Encode(w, sse.Event{Event: "ping", Data: ""})
				return true
			}
			// 发送事件：channel里是原始JSON，SSE编码统一在这里完成
			if err := sse.Encode(w, sse.Event{
				Data: msg,
			}); err != nil {
				// 写失败说明连接已坏（客户端断开或网络故障），立即注销并结束Stream
				s.unregisterClient(userId, uuid, client)
				return false
			}
			return true // 保持连接
		case <-client.done:
			// 服务端主动关闭此连接
			return false
		case <-c.Request.Context().Done():
			// 客户端断开连接
			s.unregisterClient(userId, uuid, client)
			return false // 断开连接
		}
	})
}

// registerLocal 将客户端注册进本地map（不触碰Redis），返回被顶掉的旧连接（如有）
func (s *Service) registerLocal(client *Client) (old *Client) {
	s.clientsSync.Lock()
	if _, ok := s.clients[client.userId]; !ok {
		s.clients[client.userId] = make(map[string]*Client)
	}
	old = s.clients[client.userId][client.UUID]
	s.clients[client.userId][client.UUID] = client
	s.clientsSync.Unlock()
	return old
}

// 注册客户端
func (s *Service) registerClient(client *Client) {
	// 同uuid重连时先顶掉旧连接，避免旧连接残留产生孤儿流
	if old := s.registerLocal(client); old != nil {
		old.closeOnce.Do(func() { close(old.done) })
	}

	// 用户id和客户端id拼接作为客户端唯一标识
	clientId := fmt.Sprintf("%d:%s", client.userId, client.UUID)

	// Redis注册会话 - 使用Pipeline批量操作
	pipe := s.cfg.Rds.Pipeline()
	sessionKey := s.getRedisKey(s.cfg.ServerName, s.redisSessionKeyPattern, client.userId, client.UUID)
	// sessionKey := s.redisSessionKey + clientId
	pipe.Set(s.ctx, sessionKey, s.cfg.NodeId, s.cfg.SessionTTL)
	pipe.SAdd(s.ctx, s.redisSessionSetKey, clientId)
	pipe.Expire(s.ctx, s.redisSessionSetKey, s.cfg.SessionTTL)

	// 将 clientId 添加到用户对应的设备Set中
	userClientsKey := s.getRedisKey(s.cfg.ServerName, s.redisUserUuidSetKeyPattern, client.userId)
	pipe.SAdd(s.ctx, userClientsKey, client.UUID)
	pipe.Expire(s.ctx, userClientsKey, s.cfg.SessionTTL) // 保持TTL一致

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("Error registering client: %v", err)
	}

	// 同步本节点在线连接数到Redis
	s.syncOnlineCount()

	// 注册成功后，异步发送离线消息
	SafeGoWithRestart("deliverOfflineMessages", func() {
		s.deliverOfflineMessages(client)
	}, 0, 0)
}

// deliverOfflineMessages 发送并清空用户的离线消息
func (s *Service) deliverOfflineMessages(client *Client) {
	userOfflineQueueKey := s.getRedisKey(s.cfg.ServerName, s.redisOfflineQueueKeyPattern, client.userId)
	for {
		// 消费者已退出（连接被关闭/断开）则停止投递
		select {
		case <-client.done:
			return
		default:
		}

		// 使用RPOP从列表尾部取出消息（保证先进先出）
		messageData, err := s.cfg.Rds.RPop(s.ctx, userOfflineQueueKey).Bytes()
		if errors.Is(err, redis.Nil) { // Redis.Nil表示列表已空
			return
		}
		if err != nil {
			log.Printf("Failed to get offline message for %d: %v", client.userId, err)
			return
		}

		// channel里是原始JSON，SSE编码统一由Connect完成
		select {
		case client.messageChan <- string(messageData):
		case <-time.After(s.cfg.SlowClientTimeout):
			// 客户端消费不动，本条塞回队列头部（LPUSH恢复原顺序），剩余消息留在队列等下次连接再投
			log.Printf("client slow during offline delivery, stop: uid:%d uuid:%s", client.userId, client.UUID)
			s.cfg.Rds.LPush(s.ctx, userOfflineQueueKey, messageData)
			return
		case <-client.done:
			// 投递等待期间连接被关闭，本条塞回队列避免丢失
			s.cfg.Rds.LPush(s.ctx, userOfflineQueueKey, messageData)
			return
		}
	}
}

// 注销客户端
// current 当前活跃连接：Stream退出时传自己，只有当map里记录的仍是自己时才清理，
// 防止被顶掉的旧连接断开时误删新连接的注册信息；传nil表示无条件注销
func (s *Service) unregisterClient(userId int64, uuid string, current *Client) {
	s.clientsSync.Lock()
	client := s.clients[userId][uuid]
	if current != nil && client != current {
		// 已被同uuid新连接顶替，不属于本次断开，跳过清理
		s.clientsSync.Unlock()
		return
	}
	if client != nil {
		delete(s.clients[userId], uuid)
		if len(s.clients[userId]) == 0 {
			delete(s.clients, userId)
		}
	}
	s.clientsSync.Unlock()
	if client != nil {
		client.closeOnce.Do(func() { close(client.done) })
		s.syncOnlineCount()
	}

	// 用户id和客户端id拼接作为客户端唯一标识
	clientId := fmt.Sprintf("%d:%s", userId, uuid)

	// Redis注销会话 - 使用Pipeline批量操作
	pipe := s.cfg.Rds.Pipeline()
	sessionKey := s.getRedisKey(s.cfg.ServerName, s.redisSessionKeyPattern, userId, uuid)
	pipe.Del(s.ctx, sessionKey)
	pipe.SRem(s.ctx, s.redisSessionSetKey, clientId)

	// 从用户对应的设备Set中移除该 uuid
	// userUuidsKey := fmt.Sprintf("%s%d", s.redisUserUuidSetKey, userId)
	userUuidsKey := s.getRedisKey(s.cfg.ServerName, s.redisUserUuidSetKeyPattern, userId)
	pipe.SRem(s.ctx, userUuidsKey, uuid)

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("Error unregistering client: %v", err)
	}
}

// SendToUser 向指定用户的所有在线设备发送消息（集群感知）
// userId 指定服务的用户id
// msgData msgType 消息内容和类型
// serverNames 发送的服务名 不指定则发送本服务
func (s *Service) SendToUser(userId int64, msgData any, msgType int, serverNames ...string) {
	var serverList = s.getServerNames(serverNames...)
	for _, serverName := range serverList {
		// 1. 构造消息体
		msg := &Message{
			UserId:     userId,
			ServerName: serverName,
			Type:       msgType,
			Data:       msgData,
			Timestamp:  time.Now().Unix(),
		}
		// 2. 获取该用户所有的 clientId
		userUuidsKey := s.getRedisKey(serverName, s.redisUserUuidSetKeyPattern, userId)
		uuids, err := s.cfg.Rds.SMembers(s.ctx, userUuidsKey).Result()
		if err != nil {
			log.Printf("Error getting client list for user %d: %v", userId, err)
		}

		// 3. 遍历所有 uuid，Pipeline批量查询各设备所在节点，避免每设备一次GET的串行RTT
		pipe := s.cfg.Rds.Pipeline()
		cmds := make([]*redis.StringCmd, len(uuids))
		for i, uuid := range uuids {
			sessionKey := s.getRedisKey(serverName, s.redisSessionKeyPattern, userId, uuid)
			cmds[i] = pipe.Get(s.ctx, sessionKey)
		}
		_, err = pipe.Exec(s.ctx)

		// 4. 逐设备投递；所有设备都投递失败（不在线或会话已过期）才存离线消息，防止消息静默丢失
		delivered := false
		for i, uuid := range uuids {
			nodeId, err := cmds[i].Int()
			if err != nil {
				continue // 会话不存在，该设备离线
			}
			deviceMsg := *msg
			deviceMsg.UUID = uuid
			deviceMsg.NodeId = nodeId
			if s.routeToClient(&deviceMsg, nodeId) {
				delivered = true
			}
		}
		if !delivered {
			s.storeOfflineMessage(msg)
		}
	}
}

// routeToClient 按已知节点id投递：本节点直接投，跨节点publish
func (s *Service) routeToClient(msg *Message, nodeId int) bool {
	if nodeId == s.cfg.NodeId && msg.ServerName == s.cfg.ServerName {
		// 客户端在本节点，直接发送
		s.deliverToClient(msg)
		return true
	}

	// 发送到目标节点
	msgData, _ := json.Marshal(msg)
	if err := s.cfg.Rds.Publish(s.ctx, s.getRedisKey(msg.ServerName, s.redisPubSubChannelKeyPattern, nodeId), string(msgData)).Err(); err != nil {
		log.Printf("Error publishing to node %d: %v", nodeId, err)
		return false
	}
	return true
}

// BroadcastMessage 向所有客户端广播消息
func (s *Service) BroadcastMessage(msgData any, msgType int, serverNames ...string) {
	var serverList = s.getServerNames(serverNames...)
	for _, serverName := range serverList { // 1. 构造消息体
		msg := &Message{
			NodeId:     -1,
			ServerName: serverName,
			Type:       msgType,
			Data:       msgData,
			Timestamp:  time.Now().Unix(),
		}
		data, _ := json.Marshal(msg)
		// 给所有节点推送
		if err := s.cfg.Rds.Publish(s.ctx, s.getRedisKey(serverName, s.redisPubSubChannelKeyPattern, -1), string(data)).Err(); err != nil {
			log.Printf("Error publishing broadcast: %v", err)
		}
	}
}

func (s *Service) getServerNames(serverNames ...string) (serverList []string) {
	if len(serverNames) > 0 {
		for _, name := range serverNames {
			matched := false
			for _, serverName := range s.cfg.RemoteServerNames {
				if name == serverName {
					matched = true
					break
				}
			}
			if matched {
				serverList = append(serverList, name)
			} else {
				// 目标服务不在允许列表内，静默丢弃会导致调用方误以为推送成功，记日志暴露配置问题
				log.Printf("server name %q not in remote allowlist, dropped", name)
			}
		}
	} else {
		serverList = []string{s.cfg.ServerName}
	}
	return serverList
}

// deliverToClient 本节点投递：将原始JSON写入客户端channel
// messageChan永不close（关闭改由done通知），但慢客户端缓冲写满时会阻塞，
// 因此带超时投递，超时则踢掉该客户端，避免拖死调用方
func (s *Service) deliverToClient(msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("json.Marshal err:%v", err)
		return
	}

	s.clientsSync.RLock()
	client := s.clients[msg.UserId][msg.UUID]
	s.clientsSync.RUnlock()
	if client == nil {
		// 用户不在线则保存到离线消息
		s.storeOfflineMessage(msg)
		return
	}

	select {
	case client.messageChan <- string(data):
	case <-time.After(s.cfg.SlowClientTimeout):
		log.Printf("client slow, kicking: uid:%d uuid:%s", msg.UserId, msg.UUID)
		// 踢掉慢客户端并同步清理Redis会话（传nil无条件注销），消息转入离线队列
		s.unregisterClient(msg.UserId, msg.UUID, nil)
		s.storeOfflineMessage(msg)
	}
}

// storeOfflineMessage 将消息存入用户的离线队列 (Redis List)
func (s *Service) storeOfflineMessage(msg *Message) bool {
	// 为每个用户创建一个独立的List
	userOfflineQueueKey := s.getRedisKey(msg.ServerName, s.redisOfflineQueueKeyPattern, msg.UserId)
	// 使用LPUSH将消息存入列表头部，并设置整个Key的TTL
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("json.Marshal err:%v", err)
		return false
	}
	err = s.cfg.Rds.LPush(s.ctx, userOfflineQueueKey, data).Err()
	if err != nil {
		log.Printf("Failed to store offline message for %d: %v", msg.UserId, err)
		return false
	}
	// 设置整个队列的过期时间；每次写入都刷新，持续活跃的用户队列不会被误清
	if err := s.cfg.Rds.Expire(s.ctx, userOfflineQueueKey, s.cfg.OfflineQueueTTL).Err(); err != nil {
		log.Printf("Failed to set offline queue TTL for %d: %v", msg.UserId, err)
	}
	return true
}

// 订阅本节点专属频道
func (s *Service) subscribeNodeChannel() {
	pubsub := s.cfg.Rds.Subscribe(s.ctx, s.getRedisKey(s.cfg.ServerName, s.redisPubSubChannelKeyPattern, s.cfg.NodeId))
	defer pubsub.Close()

	for {
		_msg, err := pubsub.ReceiveMessage(s.ctx)
		if err != nil {
			// Close后ctx取消，正常退出
			if errors.Is(s.ctx.Err(), context.Canceled) {
				return
			}
			log.Printf("Error receiving message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		s.subscribeChannel(_msg)
	}
}

// 订阅全局控制频道（用于广播）
func (s *Service) subscribeAllChannel() {
	pubsub := s.cfg.Rds.Subscribe(s.ctx, s.getRedisKey(s.cfg.ServerName, s.redisPubSubChannelKeyPattern, -1))
	defer pubsub.Close()

	for {
		_msg, err := pubsub.ReceiveMessage(s.ctx)
		if err != nil {
			// Close后ctx取消，正常退出
			if errors.Is(s.ctx.Err(), context.Canceled) {
				return
			}
			log.Printf("Error receiving message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		s.subscribeChannel(_msg)
	}
}

// 订阅频道消息处理 全部是本服务的消息，不用判断服务名
func (s *Service) subscribeChannel(_msg *redis.Message) {
	var msg Message
	if err := json.Unmarshal([]byte(_msg.Payload), &msg); err != nil {
		log.Printf("Error unmarshalling message: %v", err)
		return
	}
	// 目标客户端Id为空说明是群发消息
	if msg.UserId == 0 {
		// 锁内快照客户端引用，投递放到锁外，避免广播阻塞注册/注销
		type clientRef struct {
			userId int64
			UUID   string
		}
		refs := make([]clientRef, 0)
		s.clientsSync.RLock()
		for userId, clients := range s.clients {
			for uuid := range clients {
				refs = append(refs, clientRef{userId: userId, UUID: uuid})
			}
		}
		s.clientsSync.RUnlock()

		// 广播投递放独立协程：某个慢客户端的超时等待（最长SlowClientTimeout）不阻塞订阅循环收发后续消息
		go func() {
			for _, ref := range refs {
				// 创建消息副本，避免修改原始消息
				_msg := msg
				_msg.UserId = ref.userId
				_msg.UUID = ref.UUID
				s.deliverToClient(&_msg)
			}
		}()
	} else {
		// 只处理目标为本节点的消息
		if msg.NodeId == s.cfg.NodeId {
			s.deliverToClient(&msg)
		}
	}
}

// Close 关闭服务
func (s *Service) Close() {
	close(s.stopHeartbeat)
	// 清理所有本地客户端
	s.clientsSync.Lock()
	for _, clients := range s.clients {
		for _, c := range clients {
			c.closeOnce.Do(func() { close(c.done) })
		}
	}
	s.clients = make(map[int64]map[string]*Client)
	s.clientsSync.Unlock()

	// 同步在线计数（map已清空，会删除本节点的计数Key）
	s.syncOnlineCount()

	// 清理Redis中的本节点会话（此时ctx未取消，清理命令才能到达Redis）
	s.cleanupStaleSessions()

	s.cancel() // 通知订阅协程退出
}

// syncOnlineCount 将本节点当前在线连接数写入Redis（注册/注销时同步，心跳续期TTL）
// 写绝对值而非INCR/DECR增量：某次写入丢失会被下次事件自然纠正，不会累积误差；
// 无连接时直接删Key，查询端自然不计；节点崩溃后Key随TTL过期自动从汇总中消失
func (s *Service) syncOnlineCount() {
	s.clientsSync.RLock()
	clients := 0
	for _, m := range s.clients {
		clients += len(m)
	}
	s.clientsSync.RUnlock()

	key := s.getRedisKey(s.cfg.ServerName, s.redisOnlineCountKeyPattern, s.cfg.NodeId)
	if clients == 0 {
		s.cfg.Rds.Del(s.ctx, key)
		return
	}
	s.cfg.Rds.Set(s.ctx, key, clients, s.cfg.HeartbeatInterval*3/2)
}

// OnlineCount 汇总集群各节点的在线连接数（各节点注册/注销时实时写入Redis）
// 返回按nodeId分列的计数与总数；redisErr非空表示扫描Redis失败
func (s *Service) OnlineCount() (perNode map[int]int, total int, redisErr string) {
	perNode = make(map[int]int)
	prefix := fmt.Sprintf(strings.TrimSuffix(s.redisOnlineCountKeyPattern, "%d"), s.cfg.ServerName)

	// SCAN找出所有节点的计数Key
	var keys []string
	var cursor uint64
	for {
		batch, next, err := s.cfg.Rds.Scan(s.ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return perNode, total, err.Error()
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(keys) == 0 {
		return perNode, 0, ""
	}

	vals, err := s.cfg.Rds.MGet(s.ctx, keys...).Result()
	if err != nil {
		return perNode, total, err.Error()
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		n, err := strconv.Atoi(fmt.Sprint(v))
		if err != nil {
			continue
		}
		nodeId, err := strconv.Atoi(strings.TrimPrefix(keys[i], prefix))
		if err != nil {
			continue
		}
		perNode[nodeId] = n
		total += n
	}
	return perNode, total, ""
}

// 心跳协程：1.续Redis会话TTL 2.向所有客户端写SSE注释帧，防止Nginx等代理因空闲超时掐断连接
func (s *Service) heartbeat() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 收集当前所有在线客户端，Redis续期和HTTP心跳都用
			type ref struct {
				uid  int64
				uuid string
			}
			refs := make([]ref, 0)
			s.clientsSync.RLock()
			for uid, v := range s.clients {
				for uuid := range v {
					refs = append(refs, ref{uid: uid, uuid: uuid})
				}
			}
			s.clientsSync.RUnlock()

			// 使用Pipeline批量更新会话TTL
			pipe := s.cfg.Rds.Pipeline()
			userUuidsKeys := make(map[string]struct{}) // userUuidsKey按用户去重，避免同一用户多设备重复Expire
			for _, r := range refs {
				sessionKey := s.getRedisKey(s.cfg.ServerName, s.redisSessionKeyPattern, r.uid, r.uuid)
				pipe.Expire(s.ctx, sessionKey, s.cfg.SessionTTL)
				userUuidsKey := s.getRedisKey(s.cfg.ServerName, s.redisUserUuidSetKeyPattern, r.uid)
				userUuidsKeys[userUuidsKey] = struct{}{}
			}
			for key := range userUuidsKeys {
				pipe.Expire(s.ctx, key, s.cfg.SessionTTL)
			}
			pipe.Expire(s.ctx, s.redisSessionSetKey, s.cfg.SessionTTL)
			// 续期在线计数Key（节点空闲没有连接事件时防止TTL过期导致计数从汇总中消失）
			pipe.Expire(s.ctx, s.getRedisKey(s.cfg.ServerName, s.redisOnlineCountKeyPattern, s.cfg.NodeId), s.cfg.HeartbeatInterval*3/2)

			_, err := pipe.Exec(s.ctx)
			if err != nil {
				log.Printf("Error renewing sessions: %v", err)
			}

			// HTTP心跳：向每个客户端写入ping哨兵，Connect侧转为SSE注释帧
			for _, r := range refs {
				s.clientsSync.RLock()
				client := s.clients[r.uid][r.uuid]
				s.clientsSync.RUnlock()
				if client == nil {
					continue
				}
				select {
				case client.messageChan <- pingMsg:
				default: // 缓冲满说明消费者已卡死或即将被踢，跳过本轮心跳
				}
			}
		case <-s.stopHeartbeat:
			return
		}
	}
}

// 清理redis本节点的会话记录
func (s *Service) cleanupStaleSessions() {
	// 获取本节点在 Redis 中记录的所有客户端 ID
	clients := s.cfg.Rds.SMembers(s.ctx, s.redisSessionSetKey).Val()

	pipe := s.cfg.Rds.Pipeline()
	for _, clientId := range clients {
		// 从用户对应的设备Set中移除该 clientId
		// SplitN限制切成2段，uuid本身可能包含":"（uuid来自客户端query参数，不可信）
		parts := strings.SplitN(clientId, ":", 2)
		if len(parts) == 2 { // 确保格式正确，如 "uid:uuid"
			uid, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				// 处理错误
				log.Printf("Error parsing clientId:%s err:%v", clientId, err)
				continue
			}
			uuid := parts[1]
			// 使用Pipeline批量删除会话记录
			sessionKey := s.getRedisKey(s.cfg.ServerName, s.redisSessionKeyPattern, uid, uuid)
			pipe.Del(s.ctx, sessionKey)
			userUuidsKey := s.getRedisKey(s.cfg.ServerName, s.redisUserUuidSetKeyPattern, uid)
			pipe.SRem(s.ctx, userUuidsKey, uuid)
		}
	}

	// 最后删除本节点的集合
	pipe.Del(s.ctx, s.redisSessionSetKey)

	// 执行所有命令
	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("Error cleaning up stale sessions: %v", err)
	}
}

// SafeGoWithRestart 安全地启动一个协程，并在panic后延迟自动重启（带次数限制）
// goroutineName: 协程名称
// f: 要执行的函数
// maxRestarts: 最大重启次数，防止无限重启耗尽资源
// restartDelay: 重启延迟时间，避免立即重启可能加剧问题
func SafeGoWithRestart(goroutineName string, f func(), maxRestarts int, restartDelay time.Duration) {
	restarts := 0
	var run func()
	run = func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC recovered in goroutine [%s]: %v\nStack Trace:\n%s. Restarts left: %d",
					goroutineName, r, string(debug.Stack()), maxRestarts-restarts)
				if restarts < maxRestarts {
					restarts++
					time.Sleep(restartDelay) // 延迟重启
					go run()                 // 重启协程
				} else {
					log.Printf("CRITICAL: Goroutine [%s] reached max restarts, exiting.", goroutineName)
				}
			}
		}()
		f()
	}
	go run()
}
