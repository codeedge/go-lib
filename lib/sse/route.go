package sse

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes 在给定路由组上注册SSE相关路由（该组需已挂鉴权中间件）：
//
//	ANY  [group]             SSE连接（等价于直接挂 Service.Connect）
//	GET  [group]/stats       集群在线连接数统计（各节点注册/注销时实时写入Redis，此处汇总）
//	GET  [group]/test_push   推送测试 t=1单推（带uid发给指定用户，不带发给自己） t=2群发
//	                         可选server参数指定目标服务名（跨服务推送，需在RemoteServerNames白名单内），留空发本服务
//
// 用法：
//
//	sseGroup := r.Group("sse") // 最终路径: /sse、/sse/stats、/sse/test_push
//	sseService.RegisterRoutes(sseGroup)
func (s *Service) RegisterRoutes(g *gin.RouterGroup) {
	g.Any("", s.Connect)
	g.GET("stats", s.handleStats)
	g.GET("test_push", s.handleTestPush)
}

// handleStats 集群在线连接数统计
func (s *Service) handleStats(c *gin.Context) {
	perNode, total, redisErr := s.OnlineCount()
	c.JSON(http.StatusOK, gin.H{"total": total, "nodes": perNode, "redis_err": redisErr})
}

// handleTestPush 推送测试接口，仅供本地联调使用
func (s *Service) handleTestPush(c *gin.Context) {
	t := c.DefaultQuery("t", "1")
	var serverNames []string
	if server := c.Query("server"); server != "" {
		serverNames = append(serverNames, server)
	}

	msg := fmt.Sprintf("SSE测试消息 时间:%s t:%s", time.Now().Format("15:04:05"), t)
	if len(serverNames) > 0 {
		msg += " server:" + serverNames[0]
	}

	switch t {
	case "2":
		s.BroadcastMessage(msg, 1, serverNames...)
	default:
		// t=1: 指定uid则发给该用户，否则发给自己
		uid := c.GetInt64("user_id")
		if q := c.Query("uid"); q != "" {
			v, err := strconv.ParseInt(q, 10, 64)
			if err != nil || v <= 0 {
				c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "uid参数无效"})
				return
			}
			uid = v
		}
		s.SendToUser(uid, msg, 1, serverNames...)
	}
	c.JSON(http.StatusOK, gin.H{"code": 1, "data": msg})
}
