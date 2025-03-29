package gredis

import (
	"context"
	"fmt"
	"github.com/codeedge/go-lib/lib/gredis"
	"github.com/codeedge/go-lib/lib/gredis/config"
	"github.com/codeedge/go-lib/lib/gredis/mode/alone"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/util/gconv"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"testing"
	"time"
)

func Test_pub_sub_01(t *testing.T) {
	//aloneMode := alone.NewByConfig(
	//	config.NewConfig2(
	//		"192.168.2.110:6379",
	//		0,
	//		"yw123456!@#",
	//		2000,
	//		10,
	//
	//	))

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

	sRedigo.Set("test", "123")
	fmt.Println(sRedigo.Get("test"))

	ctx, cancel := context.WithCancel(context.Background())
	//ctx, _ := context.WithCancel(context.Background())
	consume := func(msg redis.Message) error {
		glog.Printf("recv channel:%s, msg: %s", msg.Channel, msg.Data)
		if gconv.String(msg.Data) == "cancel" {
			glog.Println("执行cancel()")
			cancel()
		}
		return nil
	}
	// 异常情况下自动重新订阅
	go func() {
		if err := sRedigo.Sub(ctx, consume, "channel"); err != nil {
			glog.Errorf("subscribe err: %v", err)
		}
	}()

	for i := 0; i < 20; i++ {
		glog.Printf("-------------- %d -----------------", i)
		time.Sleep(time.Second)
		_, err := sRedigo.Publish("channel", "hello, "+strconv.Itoa(i))
		if err != nil {
			glog.Fatal(err)
		}
		time.Sleep(time.Second)
		//cancel()
	}
	forever := make(chan struct{})
	<-forever
}

func Test_pub_sub_02(t *testing.T) {
	//aloneMode := alone.NewByConfig(
	//	config.NewConfig2(
	//		"192.168.2.110:6379",
	//		0,
	//		"yw123456!@#",
	//		2000,
	//		10,
	//
	//	))

	aloneMode := alone.NewByConfig(
		&config.RedisConfig{
			//Addr:           "192.168.2.110:6379",
			Addr:     "192.168.3.103:6379",
			DataBase: 0,
			//Password:       "yw123456!@#",
			Password:       "zs123456",
			IdleTimeout:    300 * time.Second,
			MaxActive:      10,
			MaxIdle:        0,
			Wait:           false,
			KeepAlive:      time.Minute * 5,
			ReadTimeout:    0 * time.Second,
			WriteTimeout:   time.Second,
			ConnectTimeout: time.Second,
		})
	sRedigo := gredis.New(aloneMode)

	sRedigo.Set("test", "123")
	fmt.Println(sRedigo.Get("test"))

	//ctx, cancel := context.WithCancel(context.Background())
	ctx, _ := context.WithCancel(context.Background())
	consume := func(msg redis.Message) error {
		glog.Printf("recv msg: %s", msg.Data)
		return nil
	}
	go func() {
		if err := sRedigo.Sub(ctx, consume, "channel"); err != nil {
			glog.Println("subscribe err: %v", err)
		}
	}()

	for i := 0; i < 10; i++ {
		glog.Printf("-------------- %d -----------------", i)
		time.Sleep(time.Second)
		_, err := sRedigo.Publish("channel", "hello, "+strconv.Itoa(i))
		if err != nil {
			glog.Fatal(err)
		}
		time.Sleep(time.Second)
		//cancel()
	}
	forever := make(chan struct{})
	<-forever
}
