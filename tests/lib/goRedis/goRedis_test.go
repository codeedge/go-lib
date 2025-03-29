package gredis

import (
	"context"
	"fmt"
	"testing"

	"github.com/gogf/gf/os/gtime"

	"github.com/codeedge/go-lib/lib/util"

	"github.com/codeedge/go-lib/lib/goRedis"
	"github.com/gogf/gf/frame/g"
	
	"github.com/redis/go-redis/v9"
)

func Test_redis(t *testing.T) {
	goRedis.InitRedis(&redis.Options{
		Addr:     g.Config().GetString("redis.addr"),
		Password: g.Config().GetString("redis.password"),
		DB:       0,
	})

	rdb := goRedis.Rdb
	rdb.Set(context.Background(), "key1", "1", 0)
	rdb.Get(context.Background(), "key1")
	rdb.Del(context.Background(), "key1")

	_, err := goRedis.CheckAndDel("key1", "1")
	t.Log(err)

	// 每天执行一次的任务演示，设置redis过期时间为晚上0点，使用redis锁防止集群启动重复执行
	// 确保每天只执行一次
	key := goRedis.GetRealCacheKey("RefreshToken:userId", 1)
	// 获取今天最晚时间
	todayLatestTime := util.GetTodayLatestTime()
	timeout := (todayLatestTime.TimestampMilli() - gtime.TimestampMilli()) / 1000
	if timeout < 0 {
		timeout = 0
	}

	goRedis.SetValueIfNoExistExecFunc(key, gtime.Datetime(), func() {
		fmt.Println(111)
	}, timeout)
}
