package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

// goredis文档 https://redis.uptrace.dev/zh/guide/go-redis.html

// Client Redis 客户端封装
type Client struct {
	rdb redis.UniversalClient
}

// New 创建普通客户端
func New(options *redis.Options) (*Client, error) {
	c := &Client{
		rdb: redis.NewClient(options),
	}
	if err := c.ping(); err != nil {
		return nil, err
	}
	return c, nil
}

// 主从模式说明：https://cloud.tencent.com/developer/article/2169883
// 主从模式是三种模式中最简单的，在主从复制中，数据库分为两类：主数据库(master)和从数据库(slave)。
// 主从模式的特点：
// - 主数据库可以进行读写操作，当读写操作导致数据变化时会自动将数据同步给从数据库
// - 从数据库一般是只读的，并且接收主数据库同步过来的数据
// - 一个master可以拥有多个slave，但是一个slave只能对应一个master
// - slave挂了不影响其他slave的读和master的读和写，重新启动后会将数据从master同步过来
// - master挂了以后，不影响slave的读，但redis不再提供写服务，master重启后redis将重新对外提供写服务
// - master挂了以后，不会在slave节点中重新选一个master
// 弊端：不具备高可用性，当master挂掉以后，Redis将不能再对外提供写入服务
// 主从模式需要配置多个地址分别初始化单节点redis即redis.NewClient，写的时候使用主节点，读的时候使用从节点
// 所以最好是使用哨兵或者集群模式

// NewFailover 创建哨兵模式1
// 哨兵模式在主从模式的基础上增加了自动故障转移功能
// sentinel哨兵会监控master和slave，当master挂了以后会自动选举新的master
func NewFailover(options *redis.FailoverOptions) (*Client, error) {
	// &redis.FailoverOptions{
	//    MasterName:    "master-name",
	//    SentinelAddrs: []string{":9126", ":9127", ":9128"},
	// }
	c := &Client{
		rdb: redis.NewFailoverClient(options),
	}
	if err := c.ping(); err != nil {
		return nil, err
	}
	return c, nil
}

// NewFailoverCluster 创建哨兵模式2
// 从 go-redis v8 版本开始，可以使用 NewFailoverClusterClient 把只读命令路由到从节点
// 注意：NewFailoverClusterClient 借助了 Cluster Client 实现，不支持 DB 选项（只能操作 DB 0）
func NewFailoverCluster(options *redis.FailoverOptions) (*Client, error) {
	// options = &redis.FailoverOptions{
	//    MasterName:    "master-name",
	//    SentinelAddrs: []string{":9126", ":9127", ":9128"},
	//
	//    // 你可以选择把只读命令路由到最近的节点，或者随机节点，二选一
	//    RouteByLatency: true,  // 最近的节点
	//    //RouteRandomly: true, // 随机节点
	// }
	c := &Client{
		rdb: redis.NewFailoverClusterClient(options),
	}
	if err := c.ping(); err != nil {
		return nil, err
	}
	return c, nil
}

// NewCluster 创建集群模式
// go-redis 支持 Redis Cluster 客户端，redis.ClusterClient 表示集群对象
// 对集群内每个 redis 节点使用 redis.Client 对象进行通信，每个 redis.Client 会拥有单独的连接池
// 注意：集群模式下不支持 DB 选项，只能操作 DB 0
func NewCluster(options *redis.ClusterOptions) (*Client, error) {
	// options = &redis.ClusterOptions{
	//    Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
	// }
	cluster := redis.NewClusterClient(options)
	// 遍历每个节点检查连通性
	err := cluster.ForEachShard(context.Background(), func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	if err != nil {
		return nil, fmt.Errorf("redis集群初始化失败:%v", err)
	}
	log.Printf("redis-cluster-init success\n")
	// 只遍历主节点请使用：ForEachMaster，只遍历从节点请使用：ForEachSlave
	return &Client{rdb: cluster}, nil
}

// NewRing 创建分片模式
// Ring 分片客户端采用一致性 HASH 算法在多个 redis 服务器之间分发 key，每个节点承担一部分 key 的存储
// Ring 客户端会监控每个节点的健康状况，并从 Ring 中移除掉宕机的节点，当节点恢复时会再加入到 Ring 中
// 节点和节点之间没有一致性，仅仅是通过多个节点分摊流量的方式来处理更多的请求
// 如果更注重一致性、分区、安全性，请使用 Redis Cluster
func NewRing(options *redis.RingOptions) (*Client, error) {
	// options = &redis.RingOptions{
	//    Addrs: map[string]string{
	//        "shard1": "localhost:7000",
	//        "shard2": "localhost:7001",
	//        "shard3": "localhost:7002",
	//    },
	// }
	ring := redis.NewRing(options)
	err := ring.ForEachShard(context.Background(), func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	if err != nil {
		return nil, fmt.Errorf("redis ring初始化失败:%v", err)
	}
	log.Printf("redis-ring-init success\n")
	return &Client{rdb: ring}, nil
}

// NewUniversal 创建通用客户端
// UniversalClient 并不是一个客户端，而是对 Client、ClusterClient、FailoverClient 客户端的包装
// 根据不同的选项，客户端的类型如下：
// 1. 如果指定了 MasterName 选项，则返回 FailoverClient 哨兵客户端
// 2. 如果 Addrs 是 2 个以上的地址，则返回 ClusterClient 集群客户端
// 3. 其他情况，返回 Client 单节点客户端
func NewUniversal(options *redis.UniversalOptions) (*Client, error) {
	// 示例如下，更多设置请参照 redis.UniversalOptions:
	// 单节点: redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{":6379"}})
	// 集群:   redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{":6379", ":6380"}})
	// 哨兵:   redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{":6379"}, MasterName: "mymaster"})
	c := &Client{
		rdb: redis.NewUniversalClient(options),
	}
	if err := c.ping(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) ping() error {
	pong, err := c.rdb.Ping(context.Background()).Result()
	if errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		return fmt.Errorf("redis失败:%v", err)
	}
	log.Printf("redis-init:%v\n", pong)
	return nil
}

// Rdb 获取 Redis 客户端
func (c *Client) Rdb() redis.UniversalClient {
	return c.rdb
}

// Join 拼接 cacheKey
// 步骤1：转换所有参数为字符串并过滤空值
// 步骤2：处理变长参数
// 步骤3：拼接最终Key并处理边界情况
func Join(key string, args ...any) string {
	// 步骤1：转换所有参数为字符串并过滤空值
	segments := make([]string, 0, len(args)+1)
	if key != "" {
		segments = append(segments, key)
	}

	// 步骤2：处理变长参数
	for _, arg := range args {
		s := fmt.Sprint(arg)
		if s != "" { // 过滤空字符串参数
			segments = append(segments, s)
		}
	}

	// 步骤3：拼接最终Key并处理边界情况
	if len(segments) == 0 {
		return ""
	}
	return strings.Join(segments, ":")
}
