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
	"time"
)

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
	if _, ok := enforcerMap[config.Key]; !ok {
		// 配置前缀，针对多个配置文件时需要指定不同的casbin表
		prefix := ""
		if config.Key != "" {
			prefix = config.Key + "_"
		}

		a, err := gormadapter.NewAdapterByDBUseTableName(config.DB, prefix, "casbin_rule")
		if err != nil {
			fmt.Sprintf("casbin连接数据库错误: %v", err)
			panic(err)
		}

		e, err := casbin.NewSyncedEnforcer(config.Path, a)
		if err != nil {
			fmt.Sprintf("初始化casbin错误: %v", err)
			panic(err)
		}
		// 配置自动同步（示例：每隔30秒自动加载策略）
		e.StartAutoLoadPolicy(30 * time.Second)

		if config.RedisAddr != "" {
			// 配置Watcher监听策略变更（如Redis） https://github.com/casbin/redis-watcher
			watcher, _ := rediswatcher.NewWatcher(config.RedisAddr, rediswatcher.WatcherOptions{
				Options: redis.Options{
					Network:  "tcp",
					Password: config.RedisPassword,
				},
				Channel: "/casbin",
				// Only exists in test, generally be true
				IgnoreSelf: true})
			e.SetWatcher(watcher)
		}

		if config.Key == "" {
			config.Key = _default
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
