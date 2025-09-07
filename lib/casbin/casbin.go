package casbin

import (
	"fmt"
	"github.com/casbin/casbin/v2"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	rediswatcher "github.com/casbin/redis-watcher/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"sync"
)

/*
1. Casbin 的策略加载机制
Casbin 默认设计是将策略全量加载到内存，原因如下：
性能优先：权限检查（Enforce）通常是高频操作，内存计算比实时数据库查询快几个数量级。
规则复杂性：Casbin 的策略规则可能涉及复杂逻辑（如 RBAC 中的角色继承、条件匹配），内存中处理更高效。
一致性保证：全量加载确保策略在检查时是完整的快照，避免因分页或条件查询导致的数据不一致。
因此，Casbin 必须在内存中维护全量策略，而无法直接“按条件查询数据库”进行权限验证。

2. 自动加载策略的性能问题
通过 StartAutoLoadPolicy(30 * time.Second) 设置的定时全量加载机制，确实会导致以下问题：
全表扫描开销：每次执行 SELECT * FROM casbin_rule 会读取所有策略记录，数据量大时 I/O 和内存压力显著。
频繁重复加载：即使策略未变更，也会定期触发全表查询，浪费资源。

3. 解决方案
根据场景需求，选择以下优化方式：

方案 1：延长自动加载间隔（平衡实时性与性能）
// 调整为每小时同步一次（根据实际需求调整）
e.StartAutoLoadPolicy(1 * time.Hour)
适用场景：策略变更频率较低（如每天更新不超过几次）。
优点：简单，减少全表查询频率。
缺点：策略更新会有最大 1 小时的延迟。

方案 2：禁用自动加载，手动同步策略
// 关闭自动加载
// e.StartAutoLoadPolicy(0) // 或直接不调用此方法

// 在策略变更时手动触发加载
func UpdatePolicy() {
    // 1. 更新数据库中的策略
    // 2. 手动重新加载
    e.LoadPolicy()
}
适用场景：策略更新由你的代码主动触发（如管理后台操作）。
优点：完全避免不必要的全表查询。
缺点：需确保所有策略变更都手动调用 LoadPolicy()。

方案 3：使用 Watcher 实现增量通知（推荐）
通过 Watcher 监听策略变更事件（如 Redis、ETCD 等），而非轮询数据库：

// 使用 Redis Watcher 示例
w, _ := rediswatcher.NewWatcher("127.0.0.1:6379", redis.Pool{})
e.SetWatcher(w)

// 在策略变更时发布通知
func UpdatePolicy() {
    // 1. 更新数据库中的策略
    // 2. 通过 Watcher 通知其他实例
    w.Update()
}
适用场景：分布式系统或需要实时同步。
优点：按需触发加载，避免轮询全表。
缺点：需额外维护消息队列或 Watcher 服务。

方案 4：优化数据库查询
为 casbin_rule 表添加索引，减少全表扫描开销：
sql
-- 确保 id 主键索引
ALTER TABLE casbin_rule ADD PRIMARY KEY (id);
适用场景：策略数据量极大但无法避免全量加载。

优点：降低单次查询开销。
缺点：不解决频繁查询的根本问题。

4. 深入理解 Casbin 的设计权衡
内存 vs 数据库查询：
Casbin 选择内存全量加载，是因为在 99% 的场景中，策略数据规模可控（几千到几万条），内存占用极小（每条策略约 100 字节，1 万条仅 1MB），而权限检查的吞吐量（如每秒万次）远高于数据库查询能力。

实时性妥协：
若业务要求策略变更秒级生效，需接受定期全量加载或引入 Watcher；若可容忍分钟级延迟，调整间隔是更简单的选择。

总结
Casbin 必须全量加载策略到内存，这是其高性能设计的核心机制。
频繁的 SELECT * FROM casbin_rule 是由自动加载策略触发的，可通过调整间隔、改用 Watcher 或手动加载优化。
数据量极大时（如超 100 万条），需重新评估 Casbin 的适用性，或通过分片、业务拆分减少单表策略数量。
*/

// 支持多配置文件

var (
	enforcerMap = make(map[string]*casbin.SyncedEnforcer, 1)
	_default    = "default"
	lock        sync.Mutex
)

type Config struct {
	Key           string   // 配置的名称 默认为空代表默认的casbin配置
	Path          string   // 配置的完整路径
	DB            *gorm.DB // gorm
	RedisAddr     string   // 配置Watcher监听策略变更 redis模式
	RedisPassword string   // redis的密码
}

func Init(config *Config) {
	if config == nil || config.Path == "" || config.DB == nil {
		panic("配置未初始化")
	}
	lock.Lock()
	defer lock.Unlock()

	if config.Key == "" {
		config.Key = _default
	} else {
		_default = config.Key
	}

	if _, ok := enforcerMap[config.Key]; !ok {
		// 配置前缀，针对多个配置文件时需要指定不同的casbin表
		prefix := ""
		if config.Key != "" {
			prefix = config.Key + "_"
		}

		a, err := gormadapter.NewAdapterByDBUseTableName(config.DB, prefix, "casbin_rule")
		if err != nil {
			fmt.Printf("casbin连接数据库错误: %v\n", err)
			return
		}

		e, err := casbin.NewSyncedEnforcer(config.Path, a)
		if err != nil {
			fmt.Printf("初始化casbin错误: %v\n", err)
			return
		}
		// 配置自动同步（示例：每隔30秒自动加载策略） 会定期全表刷新，对数据库压力较大，使用下面的监听刷新模式
		//e.StartAutoLoadPolicy(30 * time.Second)

		if config.RedisAddr != "" {
			// 配置Watcher监听策略变更（如Redis） https://github.com/casbin/redis-watcher
			watcher, err := rediswatcher.NewWatcher(config.RedisAddr, rediswatcher.WatcherOptions{
				Options: redis.Options{
					Network:  "tcp",
					Password: config.RedisPassword,
				},
				Channel: "/casbin",
				//OptionalUpdateCallback: rediswatcher.DefaultUpdateCallback(e),// 设置回调函数 或者写在下面的SetUpdateCallback
				// Only exists in test, generally be true
				IgnoreSelf: true})
			if err != nil {
				fmt.Printf("rediswatcher.NewWatcher err: %v\n", err)
				return
			}
			err = e.SetWatcher(watcher)
			if err != nil {
				fmt.Printf("rediswatcher.SetWatcher err: %v\n", err)
				return
			}
			// 设置回调函数
			err = watcher.SetUpdateCallback(rediswatcher.DefaultUpdateCallback(e))
			if err != nil {
				fmt.Printf("rediswatcher.SetUpdateCallback err: %v\n", err)
				return
			}
		}

		enforcerMap[config.Key] = e
	}
}

func Enforcer(key ...string) *casbin.SyncedEnforcer {
	configKey := ""
	if len(key) == 0 {
		configKey = _default
	} else {
		configKey = key[0]
	}
	return enforcerMap[configKey]
}
