package gredis

import (
	"fmt"
	"github.com/gogf/gf/frame/g"
	"github.com/gomodule/redigo/redis"
	"github.com/kdcer/go-lib/lib/gredis"
	"github.com/kdcer/go-lib/lib/gredis/config"
	"github.com/kdcer/go-lib/lib/gredis/mode"
	"github.com/kdcer/go-lib/lib/gredis/mode/alone"
	"github.com/kdcer/go-lib/lib/gredis/mode/sentinel"
	"testing"
	"time"
)

func Test_redis_alone(t *testing.T) {
	//echoStr := "hello world"
	aRegido := gredis.New(alone.New(
		alone.Addr("192.168.2.110:6379"),
		alone.DialOpts(
			redis.DialReadTimeout(time.Second),    // 读取超时，默认time.Second
			redis.DialWriteTimeout(time.Second),   // 写入超时，默认time.Second
			redis.DialConnectTimeout(time.Second), // 连接超时，默认500*time.Millisecond
			redis.DialPassword("yw123456!@#"),     // 鉴权密码，默认空
			redis.DialDatabase(0),                 // 数据库号，默认0
			redis.DialKeepAlive(time.Minute*5),    // 默认5*time.Minute
		),
	))

	str1, err1 := aRegido.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SET", "test1111", "test11111")
	})
	if err1 != nil {
		fmt.Println(err1.Error())
	}
	fmt.Println(str1)

	str2, err2 := aRegido.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("GET", "test1111")
	})
	fmt.Println(str2)
	if err2 != nil {
		fmt.Println(err2.Error())
	}
}

func Test_redis_alone_01(t *testing.T) {
	aloneMode := alone.NewByConfig(
		config.NewConfig2(
			"192.168.2.110:6379",
			0,
			"yw123456!@#",
			2000,
			10,
		))

	sRedigo := gredis.New(aloneMode)

	str1, err1 := sRedigo.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SET", "test1111", "test11111")
	})
	if err1 != nil {
		fmt.Println(err1.Error())
	}
	fmt.Println(str1)

	str2, err2 := sRedigo.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("GET", "test1111")
	})
	fmt.Println(str2)
	if err2 != nil {
		fmt.Println(err2.Error())
	}

}

func Test_redis_sentinel_01(t *testing.T) {
	sentinelMode := sentinel.New(
		sentinel.Addrs([]string{"192.168.2.110:26379"}),
		sentinel.MasterName("mymaster1"),
		sentinel.PoolOpts(
			mode.MaxActive(200),     // 最大连接数，默认0无限制
			mode.MaxIdle(0),         // 最多保持空闲连接数，默认2*runtime.GOMAXPROCS(0)
			mode.Wait(false),        // 连接耗尽时是否等待，默认false
			mode.IdleTimeout(200),   // 空闲连接超时时间，默认0不超时
			mode.MaxConnLifetime(0), // 连接的生命周期，默认0不失效
			mode.TestOnBorrow(nil),  // 空间连接取出后检测是否健康，默认nil
		),
		sentinel.DialOpts(
			redis.DialReadTimeout(time.Second),    // 读取超时，默认time.Second
			redis.DialWriteTimeout(time.Second),   // 写入超时，默认time.Second
			redis.DialConnectTimeout(time.Second), // 连接超时，默认500*time.Millisecond
			redis.DialPassword("yw123456!@#"),     // 鉴权密码，默认空
			redis.DialDatabase(0),                 // 数据库号，默认0
			redis.DialKeepAlive(time.Minute*5),    // 默认5*time.Minute
		),
		sentinel.SentinelDialOpts(
			redis.DialKeepAlive(time.Minute*5), // 默认5*time.Minute
		),
	)

	sRedigo := gredis.New(sentinelMode)

	str1, err1 := sRedigo.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SET", "test1111", "test11111")
	})
	if err1 != nil {
		fmt.Println(err1.Error())
	}
	fmt.Println(str1, err1)

	str2, err2 := sRedigo.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("GET", "test1111")
	})
	fmt.Println(str2)
	if err2 != nil {
		fmt.Println(err2.Error())
	}

}

func Test_redis_sentinel_02(t *testing.T) {
	sentinelMode := sentinel.NewByConfig(
		"mymaster1",
		config.NewConfig2(
			"192.168.2.110:26379",
			0,
			"yw123456!@#",
			2000,
			10,
		))

	sRedigo := gredis.New(sentinelMode)

	str1, err1 := sRedigo.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SET", "test1111", "test11111")
	})
	if err1 != nil {
		fmt.Println(err1.Error())
	}
	fmt.Println(str1)

	str2, err2 := sRedigo.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("GET", "test1111")
	})
	fmt.Println(str2)
	if err2 != nil {
		fmt.Println(err2.Error())
	}

}

func Test_redis_Geo_01(t *testing.T) {
	aloneMode := alone.NewByConfig(
		&config.RedisConfig{
			Addr:           "192.168.2.110:6379",
			DataBase:       0,
			Password:       "yw123456!@#",
			IdleTimeout:    300 * time.Second,
			MaxActive:      10,
			MaxIdle:        0,
			Wait:           false,
			KeepAlive:      time.Minute * 5,
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   time.Second,
			ConnectTimeout: time.Second,
		})
	sRedigo := gredis.New(aloneMode)

	var (
		key = "geo-test"
	)
	m1 := gredis.GeoLocation{
		Name: "m1",
		Coord: gredis.Coord{
			Longitude: 1.11,
			Latitude:  2.22,
		},
	}
	m2 := gredis.GeoLocation{
		Name: "m2",
		Coord: gredis.Coord{
			Longitude: 11.11,
			Latitude:  -22.22,
		},
	}
	m3 := gredis.GeoLocation{
		Name: "m3",
		Coord: gredis.Coord{
			Longitude: 111.111,
			Latitude:  22.222,
		},
	}

	sRedigo.GeoAdd(key, m1, m2, m3)

	rets, _ := sRedigo.GeoPos(key, m1.Name, m2.Name, m3.Name)
	fmt.Print("===>>> GeoPos:")
	g.Dump(rets)

	dist, _ := sRedigo.GeoDist(key, m1.Name, m2.Name, gredis.GEO_UNIT_TYPE_km)
	fmt.Print("===>>> GeoDist:")
	fmt.Println(dist)

	radius, _ := sRedigo.GeoRadius(key, gredis.Coord{Longitude: m1.Coord.Longitude, Latitude: m1.Coord.Latitude},
		100000, gredis.GEO_UNIT_TYPE_km, "asc", 2, gredis.WithCoord, gredis.WithHash, gredis.WithDist)
	fmt.Print("===>>> GeoRadius:")
	g.Dump(radius)

	radiusByName, _ := sRedigo.GeoRadiusByName(key, m1.Name, 100000, gredis.GEO_UNIT_TYPE_km, "desc", 1, gredis.WithCoord, gredis.WithHash, gredis.WithDist)
	fmt.Print("===>>> GeoRadiusByName:")
	g.Dump(radiusByName)

	hashValues, _ := sRedigo.GeoHash(key, m1.Name, m2.Name, m3.Name)
	fmt.Print("===>>> GeoHash:")
	g.Dump(hashValues)

}
