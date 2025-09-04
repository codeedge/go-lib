package sse

import (
	"testing"
)

func Test1(t *testing.T) {
	//..需要初始化redis等
	//New(rds.Rdb, 1, "redisPrefix")
	SSE.BroadcastMessage("群发", 1)
	SSE.SendToUser(0, "私聊", 2)
	SSE.Close()
}
