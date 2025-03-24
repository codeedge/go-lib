package gredis

import (
	"github.com/gogf/gf/os/gcfg"
	"github.com/kdcer/go-lib/lib/gredis/config"
	"github.com/kdcer/go-lib/lib/gredis/mode"
	"github.com/kdcer/go-lib/lib/gredis/mode/alone"
	"github.com/kdcer/go-lib/lib/gredis/mode/sentinel"

	"github.com/gogf/gf/frame/g"
	"github.com/gogf/gf/os/glog"
)

// 创建redis
// redis模式 1-单机, 2-哨兵集群
func CreateDefaultGredisByConfig(redisMode int8) {
	redisConfigName := "redisAlone"
	if redisMode == 2 {
		redisConfigName = "redisSentinel"
	}
	CreateGredisByConfig(redisMode, redisConfigName, DefaultRedisName)
}

// 创建redis
// redis模式 1-单机, 2-哨兵集群
// configName 配置名: redisAlone, redisSentinel
// redisName 1-单机, 2-哨兵集群
func CreateGredisByConfig(redisMode int8, configName string, redisName string) {
	var mode mode.IMode
	if redisMode == 0 || redisMode > 2 {
		panic("不存在任务redis redisMode配置")
	}

	switch redisMode {
	case 1: //1-单机
		mode = alone.NewByConfig(CreateGredisConfigByGfConfig(configName, g.Config()))
		glog.Info("redis default使用单机方式")
	case 2: //哨兵集群
		mode = sentinel.NewByConfig(
			g.Config().GetString("redisSentinel.masterName", "mymaster1"),
			CreateGredisConfigByGfConfig(configName, g.Config()))
		glog.Info("redis default使用哨兵集群方式")
	default:
		panic("不存在任务redis配置")
	}
	CreateRedisGoByRedisName(redisName, mode)
}

// 创建redis, 构建的reidsGo并不存在于redisMap中, 需要手动添加(适用于独立构建, 不让外部获取的客户端)
// redis模式 1-单机, 2-哨兵集群
// configName 配置名: redisAlone, redisSentinel
// redisName 1-单机, 2-哨兵集群
func CreateGredisByConfigAndGfConfig(redisConfig *config.RedisConfig, gfConfig *gcfg.Config) *Redigo {
	var mode mode.IMode
	redisMode := gfConfig.GetInt8("g2cache.redis.mode")
	if redisMode == 0 || redisMode > 2 {
		panic("不存在任务redis redisMode配置")
	}

	switch redisMode {
	case 1: //1-单机
		mode = alone.NewByConfig(redisConfig)
		glog.Info("redis default使用单机方式")
	case 2: //哨兵集群
		mode = sentinel.NewByConfig(
			g.Config().GetString("redisSentinel.masterName", "mymaster1"), redisConfig)
		glog.Info("redis default使用哨兵集群方式")
	default:
		panic("不存在任务redis配置")
	}
	return CreateRedisGo(mode)
}

////创建gredis配置
//func createGredisConfig(configName string) *config.RedisConfig {
//	//redis模式 1-单机, 2-哨兵集群
//	addr := g.Config().GetString(configName + ".addr")           //多个地址逗号隔开(方便配置)
//	dataBase := g.Config().GetInt(configName+".dataBase", 0)     //数据库序号
//	password := g.Config().GetString(configName+".password", "") //鉴权密码，默认空
//	maxActive := g.Config().GetInt(configName+".maxActive", 200) //最大连接数，默认0无限制 (s)
//	maxIdle := g.Config().GetInt(configName+".maxIdle", 10)      //最多保持空闲连接数，默认2*runtime.GOMAXPROCS(0)
//	redisConfig := config.NewConfig2(
//		addr,
//		dataBase,
//		password,
//		maxActive,
//		maxIdle,
//	)
//	return redisConfig
//}

// 创建gredis配置
func CreateGredisConfigByGfConfig(configName string, gfConfig *gcfg.Config) *config.RedisConfig {
	//redis模式 1-单机, 2-哨兵集群
	addr := gfConfig.GetString(configName + ".addr")           //多个地址逗号隔开(方便配置)
	dataBase := gfConfig.GetInt(configName+".dataBase", 0)     //数据库序号
	password := gfConfig.GetString(configName+".password", "") //鉴权密码，默认空
	maxActive := gfConfig.GetInt(configName+".maxActive", 200) //最大连接数，默认0无限制 (s)
	maxIdle := gfConfig.GetInt(configName+".maxIdle", 10)      //最多保持空闲连接数，默认2*runtime.GOMAXPROCS(0)
	redisConfig := config.NewConfig2(
		addr,
		dataBase,
		password,
		maxActive,
		maxIdle,
	)
	return redisConfig
}
