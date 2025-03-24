package config

import (
	"time"
)

// redis config
type RedisConfig struct {
	Addr           string        `json:"addr"`           //多个地址逗号隔开(方便配置)
	DataBase       int           `json:"dataBase"`       //数据库序号
	Password       string        `json:"password"`       //鉴权密码，默认空
	IdleTimeout    time.Duration `json:"idleTimeout"`    //空闲连接超时时间，默认0不超时 (s)
	MaxActive      int           `json:"maxActive"`      // 最大连接数，默认0无限制 (s)
	MaxIdle        int           `json:"maxIdle"`        // 最多保持空闲连接数，默认2*runtime.GOMAXPROCS(0)
	Wait           bool          `json:"wait"`           // 连接耗尽时是否等待，默认false (s)
	KeepAlive      time.Duration `json:"keepAlive"`      //空闲连接超时时间，默认0不超时 (s)
	ReadTimeout    time.Duration `json:"readTimeout"`    // 读取超时，默认time.Second
	WriteTimeout   time.Duration `json:"writeTimeout"`   // 写入超时，默认time.Second
	ConnectTimeout time.Duration `json:"connectTimeout"` // 连接超时，默认500*time.Millisecond
}

func NewConfig1(addr string, dataBase int, password string) *RedisConfig {
	redisConfig := &RedisConfig{
		Addr:           addr,
		DataBase:       dataBase,
		Password:       password,
		IdleTimeout:    300 * time.Second,
		MaxActive:      2000,
		MaxIdle:        0,
		Wait:           false,
		KeepAlive:      time.Minute * 5,
		ReadTimeout:    time.Second,
		WriteTimeout:   time.Second,
		ConnectTimeout: time.Second,
	}
	return redisConfig
}

func NewConfig2(addr string, dataBase int, password string, maxActive int, maxIdle int) *RedisConfig {
	redisConfig := &RedisConfig{
		Addr:           addr,
		DataBase:       dataBase,
		Password:       password,
		IdleTimeout:    300 * time.Second,
		MaxActive:      maxActive,
		MaxIdle:        maxIdle,
		Wait:           false,
		KeepAlive:      time.Minute * 5,
		ReadTimeout:    time.Second,
		WriteTimeout:   time.Second,
		ConnectTimeout: time.Second,
	}
	return redisConfig
}
