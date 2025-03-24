package alone

import (
	"github.com/gomodule/redigo/redis"
	"github.com/kdcer/go-lib/lib/gredis/config"
	"github.com/kdcer/go-lib/lib/gredis/mode"
	"time"
)

// 单机
type standAloneMode struct {
	pool *redis.Pool
}

func (sam *standAloneMode) GetPool() *redis.Pool {
	return sam.pool
}

func (sam *standAloneMode) GetConn() redis.Conn {
	return sam.pool.Get()
}

func (sam *standAloneMode) NewConn() (redis.Conn, error) {
	return sam.pool.Dial()
}

func (sam *standAloneMode) String() string {
	return "alone"
}

var _ mode.IMode = &standAloneMode{}

func NewByConfig(config *config.RedisConfig) *standAloneMode {
	standAloneMode := New(
		Addr(config.Addr),
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
	)
	return standAloneMode
}

func New(optFuncs ...OptFunc) *standAloneMode {
	opts := options{
		addr:     "127.0.0.1:6379",
		dialOpts: mode.DefaultDialOpts(),
		poolOpts: mode.DefaultPoolOpts(),
	}
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}
	pool := &redis.Pool{
		Dial: func() (conn redis.Conn, e error) {
			return redis.Dial("tcp", opts.addr, opts.dialOpts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
	for _, poolOptFunc := range opts.poolOpts {
		poolOptFunc(pool)
	}
	return &standAloneMode{pool: pool}
}
