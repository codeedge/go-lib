package redisCache

import (
	"github.com/codeedge/go-lib/lib/goRedis"
	"githu
	"time"
)

var (
	Cache *cache.Cache
)

func InitCache() {
	// 需要先初始化redis
	Cache = cache.New(&cache.Options{
		Redis:      goRedis.Rdb,
		LocalCache: cache.NewTinyLFU(1000, time.Minute),
	})
}
