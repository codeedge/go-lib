package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Service struct {
	nodeId                int                // 节点id
	clients               map[string]*Client // 存储所有本地客户端连接
	clientsSync           sync.RWMutex       // 读写锁
	rdb                   *redis.Client      // Redis客户端
	ctx                   context.Context    // 上下文
	redisSessionKey       string             // 会话存储
	redisSessionSetKey    string             // 节点客户端集合key
	redisPubSubChannelKey string             // 发布订阅key
	redisOfflineQueueKey  string             // 离线消息队列Key前缀
	stopHeartbeat         chan struct{}      // 停止心跳信号
}

// Client 表示SSE客户端连接
type Client struct {
	userId      int64
	clientId    string
	c           *gin.Context
	messageChan chan string
}

// Message 集群节点间传输的消息格式
type Message struct {
	ClientId  string `json:"client_id"` // 目标客户端Id
	NodeId    int    `json:"node_id"`   // 模板客户节点Id
	Type      int    `json:"type"`      // 类型
	Data      any    `json:"data"`      // 数据
	Timestamp int64  `json:"timestamp"` // 时间戳
}

var SSE *Service

const (
	redisSessionPrefix = "sse:session:"       // 会话存储前缀
	redisSessionSet    = "sse:node:clients:"  // 节点客户端集合前缀
	redisPubSubChannel = "sse:cluster"        // 集群消息通道
	redisOfflineQueue  = "sse:offline-msg"    // 离线消息队列
	sessionTTL         = 5 * 60 * time.Second // 会话有效期
	heartbeatInterval  = 60 * time.Second     // 心跳间隔
)

// New 创建SSE服务实例
func New(rds *redis.Client, nodeId int, redisPrefix string) *Service {
	SSE = &Service{
		nodeId:                nodeId,
		clients:               make(map[string]*Client),
		rdb:                   rds,
		ctx:                   context.Background(),
		redisSessionKey:       fmt.Sprintf("%s:%s", redisPrefix, redisSessionPrefix),
		redisSessionSetKey:    fmt.Sprintf("%s:%s%d", redisPrefix, redisSessionSet, nodeId),
		redisPubSubChannelKey: fmt.Sprintf("%s:%s", redisPrefix, redisPubSubChannel),
		redisOfflineQueueKey:  fmt.Sprintf("%s:%s", redisPrefix, redisOfflineQueue),
		stopHeartbeat:         make(chan struct{}),
	}

	// 初始化时清理redis本节点的会话记录
	SafeGoWithRestart("cleanupStaleSessions", SSE.cleanupStaleSessions, 3, 10*time.Second)
	// 订阅本节点专属频道
	SafeGoWithRestart("subscribeNodeChannel", SSE.subscribeNodeChannel, 3, 10*time.Second)
	// 订阅全局控制频道（用于广播）
	SafeGoWithRestart("subscribeAllChannel", SSE.subscribeAllChannel, 3, 10*time.Second)
	// 启动心跳协程
	SafeGoWithRestart("heartbeat", SSE.heartbeat, 3, 10*time.Second)
	return SSE
}

// 清理redis本节点的会话记录
func (s *Service) cleanupStaleSessions() {

	// 获取本节点在 Redis 中记录的所有客户端 ID
	clients := s.rdb.SMembers(s.ctx, s.redisSessionSetKey).Val()

	// 使用Pipeline批量删除会话记录
	pipe := s.rdb.Pipeline()
	for _, clientId := range clients {
		sessionKey := s.redisSessionKey + clientId
		pipe.Del(s.ctx, sessionKey)
	}

	// 最后删除本节点的集合
	pipe.Del(s.ctx, s.redisSessionSetKey)

	// 执行所有命令
	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("Error cleaning up stale sessions: %v", err)
	}
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

	userId := c.GetInt64("user_id")

	// 创建新客户端
	clientId := c.Query("client_id")
	if clientId == "" {
		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("client_id is empty"))
		return
	}
	// 预留支持用户多端登录，通过userid:uuid作为客户端id
	clientId = fmt.Sprintf("%d:%s", userId, clientId)
	client := &Client{
		userId:      userId,
		clientId:    clientId,
		c:           c,
		messageChan: make(chan string, 100),
	}

	// 注册客户端
	s.registerClient(clientId, client)

	// 使用 Stream API 替代手动循环
	c.Stream(func(w io.Writer) bool {
		select {
		case msg, ok := <-client.messageChan:
			if !ok {
				return false // 通道已关闭
			}
			// 发送事件
			sse.Encode(w, sse.Event{
				Data: msg,
			})
			return true // 保持连接
		case <-c.Request.Context().Done():
			// 客户端断开连接
			s.unregisterClient(clientId)
			return false // 断开连接
		}
	})
}

// 注册客户端
func (s *Service) registerClient(clientId string, client *Client) {
	// 本地注册
	s.clientsSync.Lock()
	s.clients[clientId] = client
	s.clientsSync.Unlock()

	// Redis注册会话 - 使用Pipeline批量操作
	pipe := s.rdb.Pipeline()
	sessionKey := s.redisSessionKey + clientId
	pipe.Set(s.ctx, sessionKey, s.nodeId, sessionTTL)
	pipe.SAdd(s.ctx, s.redisSessionSetKey, clientId)
	pipe.Expire(s.ctx, s.redisSessionSetKey, sessionTTL)

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("Error registering client: %v", err)
	}

	// 注册成功后，异步发送离线消息
	SafeGoWithRestart("deliverOfflineMessages", func() {
		s.deliverOfflineMessages(client)
	}, 0, 0)
}

// deliverOfflineMessages 发送并清空用户的离线消息
func (s *Service) deliverOfflineMessages(client *Client) {
	userOfflineQueueKey := fmt.Sprintf("%s:%d", s.redisOfflineQueueKey, client.userId)
	// 循环取出并删除列表中的所有消息
	for {
		// 使用RPOP从列表尾部取出消息（保证先进先出）
		messageData, err := s.rdb.RPop(s.ctx, userOfflineQueueKey).Bytes()
		if errors.Is(err, redis.Nil) { // Redis.Nil表示列表已空
			break
		}
		if err != nil {
			log.Printf("Failed to get offline message for %d: %v", client.userId, err)
			break
		}

		// 重新格式化为SSE格式并发送给客户端
		formattedMsg := fmt.Sprintf("data: %s\n\n", string(messageData))
		client.messageChan <- formattedMsg
	}
	// 所有离线消息处理完后，删除Key（可选）
	s.rdb.Del(s.ctx, userOfflineQueueKey)
}

// 注销客户端
func (s *Service) unregisterClient(clientId string) {
	// 本地注销
	s.clientsSync.Lock()
	if client, ok := s.clients[clientId]; ok {
		close(client.messageChan)
		delete(s.clients, clientId)
	}
	s.clientsSync.Unlock()

	// Redis注销会话 - 使用Pipeline批量操作
	pipe := s.rdb.Pipeline()
	sessionKey := s.redisSessionKey + clientId
	pipe.Del(s.ctx, sessionKey)
	pipe.SRem(s.ctx, s.redisSessionSetKey, clientId)

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("Error unregistering client: %v", err)
	}
}

// 心跳协程，保持会话活跃
func (s *Service) heartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 获取所有客户端ID
			s.clientsSync.RLock()
			clientIds := make([]string, 0, len(s.clients))
			for id := range s.clients {
				clientIds = append(clientIds, id)
			}
			s.clientsSync.RUnlock()

			if len(clientIds) == 0 {
				continue
			}

			// 使用Pipeline批量更新会话TTL
			pipe := s.rdb.Pipeline()
			for _, clientId := range clientIds {
				sessionKey := s.redisSessionKey + clientId
				pipe.Expire(s.ctx, sessionKey, sessionTTL)
			}
			pipe.Expire(s.ctx, s.redisSessionSetKey, sessionTTL)

			_, err := pipe.Exec(s.ctx)
			if err != nil {
				log.Printf("Error renewing sessions: %v", err)
			}
		case <-s.stopHeartbeat:
			return
		}
	}
}

// 订阅本节点专属频道
func (s *Service) subscribeNodeChannel() {
	pubsub := s.rdb.Subscribe(s.ctx, fmt.Sprintf("%s:%d", s.redisPubSubChannelKey, s.nodeId))
	defer pubsub.Close()

	for {
		_msg, err := pubsub.ReceiveMessage(s.ctx)
		if err != nil {
			fmt.Println("Error receiving message:", err)
			// 处理错误，例如重新连接
			time.Sleep(1 * time.Second)
			continue
		}

		s.subscribeChannel(_msg)
	}
}

// 订阅全局控制频道（用于广播）
func (s *Service) subscribeAllChannel() {
	pubsub := s.rdb.Subscribe(s.ctx, s.redisPubSubChannelKey)
	defer pubsub.Close()

	for {
		_msg, err := pubsub.ReceiveMessage(s.ctx)
		if err != nil {
			fmt.Println("Error receiving message:", err)
			// 处理错误，例如重新连接
			time.Sleep(1 * time.Second)
			continue
		}

		s.subscribeChannel(_msg)
	}
}

func (s *Service) subscribeChannel(_msg *redis.Message) {
	var msg Message
	if err := json.Unmarshal([]byte(_msg.Payload), &msg); err != nil {
		fmt.Println("Error unmarshalling message:", err)
		return
	}
	// 目标客户端Id为空说明是群发消息
	if msg.ClientId == "" {
		for k, _ := range s.clients {
			msg.ClientId = k
			s.deliverToClient(&msg)
		}
	} else {
		// 只处理目标为本节点的消息
		if msg.NodeId == s.nodeId {
			s.deliverToClient(&msg)
		}
	}
}

// SendToClient 向指定客户端发送消息（集群感知）
func (s *Service) SendToClient(msg *Message) bool {
	// 查找客户端所在节点
	sessionKey := s.redisSessionKey + msg.ClientId
	var nodeId = -1 // 节点id从0开始，设置-1防止和空值混淆
	var err error
	nodeId, err = s.rdb.Get(s.ctx, sessionKey).Int()
	if err != nil || nodeId == -1 {
		// 客户端不在线
		return false
	}

	msg.NodeId = nodeId
	msg.Timestamp = time.Now().Unix()

	if nodeId == s.nodeId {
		// 客户端在本节点，直接发送
		s.deliverToClient(msg)
		return true
	}

	// 发送到目标节点
	msgData, _ := json.Marshal(msg)
	s.rdb.Publish(s.ctx, fmt.Sprintf("%s:%d", s.redisPubSubChannelKey, nodeId), string(msgData))
	return true
}

// BroadcastMessage 向所有客户端广播消息
func (s *Service) BroadcastMessage(msg *Message) {
	// 创建广播消息
	msg.Timestamp = time.Now().Unix()

	msgData, _ := json.Marshal(msg)
	// 给所有节点推送
	s.rdb.Publish(s.ctx, s.redisPubSubChannelKey, string(msgData))
}

// 发送消息到指定客户端
func (s *Service) deliverToClient(msg *Message) {
	s.clientsSync.RLock()
	defer s.clientsSync.RUnlock()

	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("json.Marshal err:", err)
		return
	}

	if client, ok := s.clients[msg.ClientId]; ok {
		msg := fmt.Sprintf("data: %s\n\n", data)
		client.messageChan <- msg
	} else {
		// 用户不在线则保存到离线消息
		s.storeOfflineMessage(msg.ClientId, data)
	}
}

// storeOfflineMessage 将消息存入用户的离线队列 (Redis List)
func (s *Service) storeOfflineMessage(clientId string, messageData []byte) bool {
	userClientId := strings.Split(clientId, ":")
	if len(userClientId) != 2 {
		return false
	}
	// 为每个用户创建一个独立的List
	userOfflineQueueKey := fmt.Sprintf("%s:%s", s.redisOfflineQueueKey, userClientId[0])
	// 使用LPUSH将消息存入列表头部，并设置整个Key的TTL
	err := s.rdb.LPush(s.ctx, userOfflineQueueKey, messageData).Err()
	if err != nil {
		log.Printf("Failed to store offline message for %s: %v", userClientId[0], err)
		return false
	}
	return true
}

// Close 关闭服务
func (s *Service) Close() {
	close(s.stopHeartbeat)

	// 清理所有本地客户端
	s.clientsSync.Lock()
	for clientId, client := range s.clients {
		close(client.messageChan)
		delete(s.clients, clientId)
	}
	s.clientsSync.Unlock()

	// 清理Redis中的本节点会话
	s.cleanupStaleSessions()
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
