// 2022年3月2日13:50:36
// casbin封装
// 使用init初始化，调用方法使用Enforcer

package casbin

import (
	"fmt"
	"strings"

	"github.com/gogf/gf/os/gmlock"

	"github.com/casbin/casbin/v2"
	xormadapter "github.com/casbin/xorm-adapter/v2"
	"github.com/gogf/gf/frame/g"
)

var (
	enforcerMap       = make(map[string]*casbin.SyncedEnforcer, 0)
	enforcerConfigMap map[string]string
)

func SetConfig(configMap map[string]string) {
	if _, ok := configMap["default"]; !ok {
		panic("必须有default配置")
	}
	enforcerConfigMap = configMap
}

func Init(driverName, dataSourceName, key string) {
	if enforcerConfigMap == nil {
		panic("配置未初始化")
	}
	gmlock.Lock(key)
	defer gmlock.Unlock(key)
	if _, ok := enforcerMap[key]; !ok {
		// 要使用自己定义的数据库rbac_db,最后的true很重要.默认为false,使用缺省的数据库名casbin,不存在则创建
		//a, err := xormadapter.NewAdapter(driverName, dataSourceName, true)
		//if err != nil {
		//	fmt.Println("casbin连接数据库错误: %v", err)
		//	panic(err)
		//}

		// 配置前缀，针对多个配置文件时需要指定不同的casbin表
		tablePrefix := ""
		if key != "default" {
			tablePrefix = key + "_"
		}

		a, err := xormadapter.NewAdapterWithTableName(driverName, dataSourceName, tablePrefix+"casbin_rule", "", true)
		if err != nil {
			fmt.Println("casbin连接数据库错误: %v", err)
			panic(err)
		}

		e, err := casbin.NewSyncedEnforcer(enforcerConfigMap[key], a)
		if err != nil {
			fmt.Println("初始化casbin错误: %v", err)
			panic(err)
		}
		enforcerMap[key] = e
	}
}

// Init2 goframe配置文件专用
func Init2(key ...string) {
	configKey := ""
	if len(key) == 0 {
		configKey = "default"
	} else {
		configKey = key[0]
	}
	link := g.Config().GetString("database.link")
	if len(link) == 0 {
		panic("casbin数据库连接为空")
	}
	links := strings.Split(link, ":")
	if len(links) == 0 {
		panic("casbin数据库连接错误")
	}
	driverName := links[0]
	dataSourceName := strings.Replace(link, driverName+":", "", 1)
	Init(driverName, dataSourceName, configKey)
}

func Enforcer(key ...string) *casbin.SyncedEnforcer {
	configKey := ""
	if len(key) == 0 {
		configKey = "default"
	} else {
		configKey = key[0]
	}
	// 每次获取权限时要调用`LoadPolicy()`否则不会重新加载数据库数据
	err := enforcerMap[configKey].LoadPolicy()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	return enforcerMap[configKey]
}
