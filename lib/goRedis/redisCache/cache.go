package redisCache

import (
	"github.com/go-redis/cache/v9"
	"github.com/kdcer/go-lib/lib/goRedis"
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
