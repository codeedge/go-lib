package sonyflake

import (
	"context"
	"feige-cloud-backend/pkg/logs"
	"feige-cloud-backend/pkg/redis"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	nodeIDKey = "global-nodeId-key"
)

var _nodeId int
var once sync.Once

// NodeID 获取节点id（循环分配方案）
// nodeCount 节点总数
func NodeID(rdb goredis.UniversalClient, nodeCount int, projectName ...any) (node int) {
	once.Do(func() {
		if nodeCount == 0 {
			nodeCount = 128
		}
		ctx := context.Background()
		// Lua脚本：原子获取下一个nodeIDKey
		script := `
		local key = KEYS[1]
		local maxId = tonumber(ARGV[1])

		-- 检查key是否存在
		local exists = redis.call('EXISTS', key)

		if exists == 1 then
			-- 存在则递增并取模
			local nodeId = tonumber(redis.call('GET', key))
			nodeId = (nodeId + 1) % maxId
			redis.call('SET', key, nodeId)
			return nodeId
		else
			-- 不存在则初始化
			redis.call('SET', key, 0)
			return 0
		end
	`

		var err error
		for i := 0; i < 3; i++ {
			_nodeId, err = rdb.Eval(ctx, script, []string{redis.Join(nodeIDKey, projectName)}, nodeCount).Int()
			if err == nil {
				return
			}
			logs.Errorf("NodeID attempt %d error: %v", i+1, err)
			// 可加短暂休眠，避免立即重试
			time.Sleep(time.Millisecond * 100)
		}

		panic(fmt.Sprintf("NodeID failed after 3 retries: %v", err))
	})
	return _nodeId
}
