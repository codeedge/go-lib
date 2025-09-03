package sse

import (
	"testing"
)

func Test1(t *testing.T) {
	//..需要初始化redis等
	//New(rds.Rdb, 1, "redisPrefix")
	SSE.BroadcastMessage(&Message{
		Type: 1,
		Data: "群发",
	})
	SSE.SendToUser(1, "私聊", 2)
	SSE.Close()
}
