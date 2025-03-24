package gredis

import (
	"context"
	"fmt"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/util/gconv"
	"github.com/gomodule/redigo/redis"
	"time"
)

// 发布和订阅
// ====================================================
type ConsumeFuc func(msg redis.Message) error

// 订阅
func (r *Redigo) Sub(ctx context.Context, consume ConsumeFuc, channels ...string) (err error) {
	//err = r.subBySubFunc(func(conn redis.PubSubConn) (err error) {
	//	return DoSubscribeFunc(ctx, conn, consume, channel...)
	//})
	//return err
	return r.subBySubFunc(ctx, consume, channels...)
}

// 将信息 message 发送到指定的频道 channel 。
func (r *Redigo) Publish(channel, message string) (int64, error) {
	res, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("PUBLISH", channel, message)
	})
	return res, e
}

////通过订阅函数订阅
//func (r *Redigo) subBySubFunc(fn SubFunc) (err error) {
//	conn, err := r.mode.NewConn()
//	if err != nil {
//		return
//	}
//	str, _ := redis.String(conn.Do("echo", "hello"))
//	fmt.Println("str======>>>>", str)
//	psConn := redis.PubSubConn{Conn: conn}
//	err = fn(psConn)
//	defer psConn.Close()
//	return err
//}

// 订阅函数
func (r *Redigo) subBySubFunc(ctx context.Context, consume ConsumeFuc, channels ...string) error {
	conn, err := r.mode.NewConn()
	if err != nil {
		fmt.Println(" r.mode.NewConn() 失败, 1s后重试, err=", err)
		time.Sleep(time.Second)
		return r.subBySubFunc(ctx, consume, channels...)
	}
	psConn := redis.PubSubConn{Conn: conn}
	defer psConn.Close()

	glog.Infof("redis pubsub DoSubscribeFunc channel: %v", channels)
	// 如果订阅失败，休息1秒后重新订阅（比如当redis服务停止服务或网络异常）
	if err := psConn.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return err
	}
	done := make(chan error, 1)
	//启动一个新的goroutine来接收消息
	go func() {
		for {
			switch msg := psConn.Receive().(type) {
			case error:
				//glog.Error("subscribe case error msg=", msg)
				done <- fmt.Errorf("redis pubsub receive err: %v", msg)
				return
			case redis.Message:
				glog.Debugf("subscribe case redis.Message pattern=%s, channel=%s, data=%s, ",
					msg.Pattern, msg.Channel, gconv.String(msg.Data))
				if err := consume(msg); err != nil {
					done <- err
					return
				}
			case redis.Subscription:
				glog.Debug("subscribe case redis.Subscription msgCount=", msg.Count)

				if msg.Count == 0 {
					// 所有频道均未订阅
					done <- nil
					return
				}

			}
		}
	}()

	//健康检查
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := psConn.Unsubscribe(); err != nil {
				return fmt.Errorf("redis pubsub unsubscribe err: %v", err)
			}
			return nil
		case err := <-done: //err 1s后重连
			glog.Error("subscribe err := <-done error=", err)
			time.Sleep(time.Second)
			return r.subBySubFunc(ctx, consume, channels...)
		case <-tick.C:
			if err := psConn.Ping(""); err != nil {
				return err
			}
		}
	}
	return nil
}
