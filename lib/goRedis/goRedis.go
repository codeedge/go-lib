package goRedis

import (
	"context"
	"strings"
	"time"

	"github.com/gogf/gf/util/gconv"

	"github.com/gogf/gf/os/glog"
	"github.com/redis/go-redis/v9"
)

var Rdb *redis.Client

var projectName string // redis前缀

func InitRedis(options *redis.Options) {
	Rdb = redis.NewClient(options)
}

func InitRedisPreKey(preKey string) {
	projectName = preKey
}

func CheckAndDel(key, value string) (int, error) {
	cmd := Rdb.Eval(context.Background(), `if redis.call("get",KEYS[1]) == ARGV[1]
										then
											return redis.call("del",KEYS[1])
										else
											return 0
										end`, []string{key}, value)
	res, e := cmd.Int()
	return res, e
}

// SetValueIfNoExistExecFunc 设置value, 如果redis里没有该值的话, 就会执行execFunc
func SetValueIfNoExistExecFunc(key string, value interface{}, execFunc func(), ex ...int64) (b bool) {
	if len(ex) > 0 && ex[0] > 0 {
		b = Rdb.SetNX(context.Background(), key, value, time.Second*time.Duration(ex[0])).Val()
	} else {
		b = Rdb.SetNX(context.Background(), key, value, 0).Val()
	}
	if !b {
		glog.Errorf("SetValueIfNoExistExecFunc 执行失败 key=%v, value=%v,ex=%v, b=%v", key, value, ex, b)
		return b
	}
	execFunc()
	return b
}

// GetRealCacheKey 获取实际cacheKey
func GetRealCacheKey(modelKey string, args ...interface{}) string {
	cacheKey := projectName + ":" + modelKey
	if len(args) > 0 {
		for i := 0; i < len(args); i++ {
			if i == 0 && strings.HasSuffix(modelKey, ":") {

			} else {
				cacheKey += ":"
			}
			cacheKey += gconv.String(args[i])
		}
	}
	return cacheKey
}
