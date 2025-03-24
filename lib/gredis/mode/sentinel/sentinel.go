package sentinel

import (
	"github.com/kdcer/go-lib/lib/gredis/config"
	"github.com/kdcer/go-lib/lib/gredis/mode"
	"runtime"
	"strings"
	"time"

	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"
)

// redis 哨兵
type sentinelMode struct {
	pool *redis.Pool
}

func (sam *sentinelMode) GetPool() *redis.Pool {
	return sam.pool
}

func (sm *sentinelMode) GetConn() redis.Conn {
	return sm.pool.Get()
}

func (sm *sentinelMode) NewConn() (redis.Conn, error) {
	return sm.pool.Dial()
}

func (sm *sentinelMode) String() string {
	return "sentinel"
}

var _ mode.IMode = &sentinelMode{}

func NewByConfig(masterName string, config *config.RedisConfig) *sentinelMode {
	sentinelMode := New(
		Addrs(strings.Split(config.Addr, ",")),
		MasterName(masterName),
		PoolOpts(
			mode.MaxActive(config.MaxActive),     // 最大连接数，默认0无限制
			mode.MaxIdle(config.MaxIdle),         // 最多保持空闲连接数，默认2*runtime.GOMAXPROCS(0)
			mode.Wait(config.Wait),               // 连接耗尽时是否等待，默认false
			mode.IdleTimeout(config.IdleTimeout), // 空闲连接超时时间，默认0不超时
			mode.MaxConnLifetime(0),              // 连接的生命周期，默认0不失效
			mode.TestOnBorrow(nil),               // 空间连接取出后检测是否健康，默认nil
		),
		DialOpts(
			redis.DialReadTimeout(config.ReadTimeout),       // 读取超时，默认time.Second
			redis.DialWriteTimeout(config.WriteTimeout),     // 写入超时，默认time.Second
			redis.DialConnectTimeout(config.ConnectTimeout), // 连接超时，默认500*time.Millisecond
			redis.DialPassword(config.Password),             // 鉴权密码，默认空
			redis.DialDatabase(config.DataBase),             // 数据库号，默认0
			redis.DialKeepAlive(config.KeepAlive),           // 默认5*time.Minute
		),
		SentinelDialOpts(
			redis.DialKeepAlive(config.KeepAlive), // 默认5*time.Minute
		),
	)
	return sentinelMode
}

func New(optFuncs ...OptFunc) *sentinelMode {
	opts := options{
		addrs:      []string{"127.0.0.1:26379"},
		masterName: "mymaster1",
		poolOpts:   mode.DefaultPoolOpts(),
		dialOpts:   mode.DefaultDialOpts(),
	}
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}
	if len(opts.sentinelDialOpts) == 0 {
		opts.sentinelDialOpts = opts.dialOpts
	}
	st := &sentinel.Sentinel{
		Addrs:      opts.addrs,
		MasterName: opts.masterName,
		Pool: func(addr string) *redis.Pool {
			stp := &redis.Pool{
				Wait:    true,
				MaxIdle: runtime.GOMAXPROCS(0),
				Dial: func() (redis.Conn, error) {
					return redis.Dial("tcp", addr, opts.sentinelDialOpts...)
				},
				TestOnBorrow: func(c redis.Conn, t time.Time) (err error) {
					_, err = c.Do("PING")
					return
				},
			}
			return stp
		},
	}
	pool := &redis.Pool{
		Dial: func() (conn redis.Conn, err error) {
			addr, err := st.MasterAddr()
			if err != nil {
				return
			}
			return redis.Dial("tcp", addr, opts.dialOpts...)
		},
	}
	for _, poolOptFunc := range opts.poolOpts {
		poolOptFunc(pool)
	}
	return &sentinelMode{pool: pool}
}
