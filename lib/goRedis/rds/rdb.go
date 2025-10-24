package rds

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/golang/groupcache/consistenthash"
	"github.com/redis/go-redis/v9"
	"hash/crc32"
	"log"
	"strings"
	"time"
)

// goredis文档 https://redis.uptrace.dev/zh/guide/go-redis.html

var Rdb *redis.Client
var Rs *redsync.Redsync

func Init(options *redis.Options) {
	Rdb = redis.NewClient(options)
	pong, err := Rdb.Ping(context.Background()).Result()
	if errors.Is(err, redis.Nil) {
		fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		fmt.Errorf("redis失败:%v", err)
	} else {
		log.Printf("redis-init:%v\n", pong)
		setRedsync()
	}
}

// 主从模式说明：https://cloud.tencent.com/developer/article/2169883
// 2.1 主从模式简介
// 主从模式是三种模式中最简单的，在主从复制中，数据库分为两类：主数据库(master)和从数据库(slave)。其中，主从复制有如下特点：
// 主数据库可以进行读写操作，当读写操作导致数据变化时会自动将数据同步给从数据库；
// 从数据库一般是只读的，并且接收主数据库同步过来的数据；
// 一个master可以拥有多个slave，但是一个slave只能对应一个master；
// slave挂了不影响其他slave的读和master的读和写，重新启动后会将数据从master同步过来；
// master挂了以后，不影响slave的读，但redis不再提供写服务，master重启后redis将重新对外提供写服务；
// master挂了以后，不会在slave节点中重新选一个master；
// 主从模式的弊端就是不具备高可用性，当master挂掉以后，Redis将不能再对外提供写入操作，因此sentinel哨兵模式应运而生。
// 主从模式需要配置多个地址分别初始化单节点redis即redis.NewClient，写的时候使用主节点，读的时候使用从节点，主节点挂了则服务不可用，无法更新数据。
// 所以最好是使用哨兵或者集群模式。

// InitFailover 哨兵模式1
func InitFailover(options *redis.FailoverOptions) {
	// &redis.FailoverOptions{
	//	MasterName:    "master-name",
	//	SentinelAddrs: []string{":9126", ":9127", ":9128"},
	// }
	Rdb := redis.NewFailoverClient(options)
	pong, err := Rdb.Ping(context.Background()).Result()
	if errors.Is(err, redis.Nil) {
		fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		fmt.Errorf("redis失败:%v", err)
	} else {
		log.Printf("redis-init:%v\n", pong)
	}
}

// InitFailoverCluster 哨兵模式2
// 从 go-redis v8 版本开始，你可以尝试使用 NewFailoverClusterClient 把只读命令路由到从节点，请注意，
// NewFailoverClusterClient 借助了 Cluster Client 实现，不支持 DB 选项（只能操作 DB 0）：
func InitFailoverCluster(options *redis.FailoverOptions) {
	// options = &redis.FailoverOptions{
	//	MasterName:    "master-name",
	//	SentinelAddrs: []string{":9126", ":9127", ":9128"},
	//
	//	// 你可以选择把只读命令路由到最近的节点，或者随机节点，二选一
	//	//RouteByLatency: true,// 最近的节点
	//	//RouteRandomly:  true,// 随机节点
	// }
	Rdb := redis.NewFailoverClusterClient(options)
	pong, err := Rdb.Ping(context.Background()).Result()
	if errors.Is(err, redis.Nil) {
		fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		fmt.Errorf("redis失败:%v", err)
	} else {
		log.Printf("redis-init:%v\n", pong)
	}
}

// InitCluster 集群模式
// go-redis 支持 Redis Cluster 客户端，如下面示例，redis.ClusterClient 表示集群对象，对集群内每个 redis 节点使用 redis.Client 对象进行通信，
// 每个 redis.Client 会拥有单独的连接池。
func InitCluster(options *redis.ClusterOptions) {
	// options = &redis.ClusterOptions{
	//	Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
	// }
	Rdb := redis.NewClusterClient(options)
	// 遍历每个节点：
	err := Rdb.ForEachShard(context.Background(), func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	if errors.Is(err, redis.Nil) {
		fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		fmt.Errorf("redis失败:%v", err)
	}

	// 只遍历主节点请使用： ForEachMaster， 只遍历从节点请使用： ForEachSlave
	// 你也可以自定义的设置每个节点的初始化:
	Rdb = redis.NewClusterClient(&redis.ClusterOptions{
		NewClient: func(opt *redis.Options) *redis.Client {
			// user, pass := userPassForAddr(opt.Addr)
			// opt.Username = user
			// opt.Password = pass

			return redis.NewClient(opt)
		},
	})
}

// InitRing 分片模式
// Ring 分片客户端，是采用了一致性 HASH 算法在多个 redis 服务器之间分发 key，每个节点承担一部分 key 的存储。
// Ring 客户端会监控每个节点的健康状况，并从 Ring 中移除掉宕机的节点，当节点恢复时，会再加入到 Ring 中。这样实现了可用性和容错性，
// 但节点和节点之间没有一致性，仅仅是通过多个节点分摊流量的方式来处理更多的请求。如果你更注重一致性、分区、安全性，请使用 Redis Cluster。
func InitRing(options *redis.RingOptions) {
	// 创建一个由三个节点组成的 Ring 客户端，更多设置请参照 redis.RingOptions:
	options = &redis.RingOptions{
		Addrs: map[string]string{
			// shardName => host:port
			"shard1": "localhost:7000",
			"shard2": "localhost:7001",
			"shard3": "localhost:7002",
		},
	}
	Rdb := redis.NewRing(options)
	// 你可以像其他客户端一样执行命令：
	if err := Rdb.Set(context.Background(), "foo", "bar", 0).Err(); err != nil {
		panic(err)
	}

	// 遍历每个节点:
	err := Rdb.ForEachShard(context.Background(), func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	if errors.Is(err, redis.Nil) {
		fmt.Errorf("redis异常:%v", err)
	} else if err != nil {
		fmt.Errorf("redis失败:%v", err)
	}

	// 节点选项配置
	// 你可以手动设置连接节点，例如设置用户名和密码：
	Rdb = redis.NewRing(&redis.RingOptions{
		NewClient: func(opt *redis.Options) *redis.Client {
			// user, pass := userPassForAddr(opt.Addr)
			// opt.Username = user
			// opt.Password = pass

			return redis.NewClient(opt)
		},
	})

	// 自定义 Hash 算法
	// go-redis 默认使用 Rendezvous Hash 算法将 Key 分布到多个节点上，你可以更改为其他 Hash 算法：

	Rdb = redis.NewRing(&redis.RingOptions{
		NewConsistentHash: func(shards []string) redis.ConsistentHash {
			return consistenthash.New(100, crc32.ChecksumIEEE)
		},
	})
}

// InitUniversal Go Redis Universal 通用客户端
// UniversalClient 并不是一个客户端，而是对 Client 、 ClusterClient 、 FailoverClient 客户端的包装。
// 根据不同的选项，客户端的类型如下：
// 1.如果指定了 MasterName 选项，则返回 FailoverClient 哨兵客户端。
// 2.如果 Addrs 是 2 个以上的地址，则返回 ClusterClient 集群客户端。
// 3.其他情况，返回 Client 单节点客户端。
func InitUniversal(options *redis.UniversalOptions) {
	// 示例如下，更多设置请参照 redis.UniversalOptions:
	var rdb redis.UniversalClient
	rdb = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})

	// *redis.ClusterClient.
	rdb = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379", ":6380"},
	})

	// *redis.FailoverClient.
	rdb = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:      []string{":6379"},
		MasterName: "mymaster",
	})

	rdb.Ping(context.Background())
}

func setRedsync() {
	pool := goredis.NewPool(Rdb)
	Rs = redsync.New(pool)
}

// NewMutex 设置分布式锁 配置重试3次，重试延迟50毫秒 默认的锁会重试32次，响应过慢
func NewMutex(name string, options ...redsync.Option) *redsync.Mutex {
	opts := []redsync.Option{
		redsync.WithTries(3), redsync.WithRetryDelay(time.Millisecond * 50),
	}
	if len(options) > 0 {
		opts = append(opts, options...)
	}
	return Rs.NewMutex(name, opts...)
}

// LockExtend 创建带续租的分布式锁并执行任务
// 适合场景：分布式系统并发时只允许一个进程执行一些耗时操作，无法保证锁在释放前执行完，需要给锁续租，直到程序执行完后释放锁，并停止续租
// lockKey 锁的key
// expiry 锁的过期时间
// task 执行的任务
func LockExtend(lockKey string, expiry time.Duration, task func()) {
	if expiry < 1 {
		expiry = 10 * time.Second
	}
	mutex := Rs.NewMutex(lockKey, redsync.WithExpiry(expiry)) // 创建一个带有过期时间的互斥锁
	if err := mutex.Lock(); err != nil {
		log.Println(err)
		return //  锁获取失败直接退出
	}

	done := make(chan bool)

	// 开启一个goroutine，周期性地续租锁
	go func() {
		ticker := time.NewTicker(expiry / 2) // 按照需求调整 每隔过期时间的一半续租一次
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ok, err := mutex.Extend()
				if !ok || err != nil {
					log.Printf("Failed to extend lock: ok:%v err:%v", ok, err)
					return
				}
			case <-done:
				return
			}
		}
	}()

	// 执行需要锁的工作
	task()

	// 通知goRoutine停止续租
	close(done)
	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := mutex.Unlock(); !ok || err != nil {
		log.Println("unlock failed")
	}
}

// LockAwaitOnce 创建带续租的分布式锁并执行任务，等待执行，一直循环获取锁直到获得成功
// 适合场景：定时任务集群模式只需要一台机器执行任务，其他机器需要一直尝试获取锁，防止获取锁的机器重启或者宕机导致没有机器执行任务
// 适合启动只会执行1次的初始化程序防止并发启动，比如初始化定时任务,在task中添加定时任务
// 这种锁适合定时任务作为单独项目启动，集群部署只有一个机器执行任务，其他机器作为备用只有当主机器挂了才会执行任务。或者是定时任务和其他功能的项目写在一起的集群部署，只需要一台机器执行定时任务的场景。
// 如果想均衡让所有机器执行任务，需要在每个任务执行开始单独加锁每次获取锁再去执行。定时定点执行的可以加个长一点时间的锁控制只有一台执行，每隔几分钟这种定时任务就需要单独处理了，不好控制锁的时间，
// 需要用到本方法给不同的任务不同的锁也能在一定程度上均衡机器
// 或者有其他方案，比如：实现注册机制，集群的机器都注册到一个列表，每次执行都随机分配一个机器id，分配的机器id和当前机器一致则执行
// lockKey 锁的key
// expiry 锁的过期时间
// task 执行的任务
// clear 在续期失败时清理执行的任务，比如清理定时任务，防止续期失败后其他机器和本机器多次执行了定时任务
func LockAwaitOnce(lockKey string, expiry time.Duration, task func(), clear ...func()) {
	go func() {
		if expiry < 1 {
			expiry = 10 * time.Second
		}
		// 创建一个带有过期时间的互斥锁 设置重试3次，重试之间等待的时间长度50毫秒
		mutex := Rs.NewMutex(lockKey, redsync.WithExpiry(expiry), redsync.WithTries(3), redsync.WithRetryDelay(time.Millisecond*50))
		// 一直循环尝试获取锁，获取成功则执行任务
		for {
			if err := mutex.Lock(); err != nil {
				// 获取失败睡眠一半的时间再重试
				time.Sleep(expiry / 2)
				continue
			}

			log.Printf("LockAwaitOnce mutex.Lock() 得到锁 time:%v", time.Now().Format(time.RFC3339Nano))
			// 创建通道用于协调续租协程
			renewalFailed := make(chan struct{})

			go func() {
				// 任务完成后的处理
				// 任务完成后，阻塞等待，直到收到续租失败的信号
				select {
				case <-renewalFailed:
					if len(clear) > 0 {
						clear[0]()
					}
					// 主动释放锁，避免死锁
					if unlockOk, unlockErr := mutex.Unlock(); !unlockOk || unlockErr != nil {
						log.Printf("LockAwaitOnce Failed to unlock after extend failure: ok:%v err:%v time:%v", unlockOk, unlockErr, time.Now().Format(time.RFC3339Nano))
					}
					return
				}
			}()

			// 开启一个goroutine，周期性地续租锁
			go func() {
				ticker := time.NewTicker(expiry / 2) // 按照需求调整 每隔过期时间的一半续租一次
				defer ticker.Stop()

				for range ticker.C {
					ok, err := mutex.Extend()
					if !ok || err != nil {
						log.Printf("LockAwaitOnce Failed to extend lock: ok:%v err:%v time:%v", ok, err, time.Now().Format(time.RFC3339Nano))
						close(renewalFailed) // 通知主协程续租失败
						return
					}
				}
			}()

			// 执行需要锁的工作，使用匿名函数来限制 recover 的作用域
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("LockAwaitOnce Task panic recovered: %v, key: %s time:%v", r, lockKey, time.Now().Format(time.RFC3339Nano))
						close(renewalFailed) // 通知主协程续租失败
					}
				}()
				task()
			}()

			// 获取成功后不用退出循环，这样本机如果续期失败还可以继续尝试参与进来，可以把过期时间设置久一点，这样请求redis的频率就低了。
			// return
		}
	}()
}

// Join 拼接cacheKey
func Join(key string, args ...interface{}) string {
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
