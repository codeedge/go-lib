package sonyflake

import (
	"fmt"
	"os"
	"strconv"
)

var nodeID = -1
var workerID = -1

// GetNodeID 获取节点ID
// 优先从环境变量NODE_ID读取，如果没有则返回4-10之间的随机数
// 节点id 所有项目共用，代表每个项目的集群唯一节点id，用于拼接雪花id机器id，SSE和MQ发布订阅队列的key后缀区分等场景。
func GetNodeID() int {
	if nodeID > 0 {
		return nodeID
	}

	// 从环境变量读取
	nodeIDStr := os.Getenv("NODE_ID")
	if nodeIDStr != "" {
		if id, err := strconv.Atoi(nodeIDStr); err == nil {
			nodeID = id
			fmt.Printf("GetNodeID From Env:%d\n", nodeID)
			return nodeID
		}
	}

	// 不配置直接panic
	panic("NODE_ID 未配置")
}

// GetWorkerID 获取雪花id机器id：将项目 ID 和节点 ID 合并为 10 位的 Snowflake Worker ID
// 如果多个项目都对一个表产生数据，多个项目之间也需要不同的雪花机器id，通过项目id和节点id移位来区分。一台机器部署多份也算一个新的机器，机器id和项目id都是从0到31。
func GetWorkerID(projectID int) int {
	if workerID > 0 {
		return workerID
	}
	nodeId := GetNodeID()
	// 5位限制，最大值为 31
	const max5Bit = 31

	if projectID > max5Bit || projectID < 0 {
		panic("projectID 必须在 0-31 之间")
	}
	if nodeId > max5Bit || nodeId < 0 {
		panic("nodeId 必须在 0-31 之间")
	}

	// 逻辑：项目ID左移5位，空出后5位给机器ID，然后进行按位或运算
	// 二进制示例：ProjectID(01011) << 5 | nodeId(00001) = 0101100001
	workerID = (projectID << 5) | nodeId
	fmt.Printf("GetWorkerID:%d\n", workerID)
	return workerID
}
