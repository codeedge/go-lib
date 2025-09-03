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
	SSE.SendToClient(&Message{
		ClientId: "0:123456",
		Type:     2,
		Data:     "私聊",
	})
	SSE.Close()
}
