package casbin

import (
	"fmt"
	"github.com/casbin/casbin/v2"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	rediswatcher "github.com/casbin/redis-watcher/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// Config casbin 配置
type Config struct {
	Key           string   // 配置的名称，用于区分多个 casbin 实例（表前缀和 watcher 频道）
	Path          string   // 配置文件路径
	DB            *gorm.DB // 数据库连接
	RedisAddr     string   // Redis 地址，用于 Watcher 监听策略变更
	RedisPassword string   // Redis 密码
}

// New 创建 casbin 实例
func New(config *Config) *casbin.SyncedEnforcer {
	if config == nil || config.Path == "" || config.DB == nil {
		panic("casbin配置未初始化")
	}

	// 配置前缀，针对多个配置文件时需要指定不同的 casbin 表
	prefix := ""
	if config.Key != "" {
		prefix = config.Key + "_"
	}

	a, err := gormadapter.NewAdapterByDBUseTableName(config.DB, prefix, "casbin_rule")
	if err != nil {
		panic(fmt.Sprintf("casbin连接数据库错误: %v", err))
	}

	e, err := casbin.NewSyncedEnforcer(config.Path, a)
	if err != nil {
		panic(fmt.Sprintf("初始化casbin错误: %v", err))
	}

	// 配置自动同步（示例：每隔30秒自动加载策略） 会定期全表刷新，对数据库压力较大，使用下面的监听刷新模式
	//e.StartAutoLoadPolicy(30 * time.Second)

	if config.RedisAddr != "" {
		// 配置Watcher监听策略变更（如Redis） redis集群模式使用NewWatcherWithCluster方法 https://github.com/casbin/redis-watcher
		watcher, err := rediswatcher.NewWatcher(config.RedisAddr, rediswatcher.WatcherOptions{
			Options: redis.Options{
				Network:  "tcp",
				Password: config.RedisPassword,
			},
			Channel: fmt.Sprintf("/casbin:%s", config.Key),
			// Only exists in test, generally be true
			IgnoreSelf: true})
		if err != nil {
			panic(fmt.Sprintf("rediswatcher.NewWatcher err: %v", err))
		}
		err = e.SetWatcher(watcher)
		if err != nil {
			panic(fmt.Sprintf("rediswatcher.SetWatcher err: %v", err))
		}
		// 设置回调函数
		err = watcher.SetUpdateCallback(rediswatcher.DefaultUpdateCallback(e))
		if err != nil {
			panic(fmt.Sprintf("rediswatcher.SetUpdateCallback err: %v", err))
		}
	}

	return e
}
