package etcdctl

import (
	"context"
	"fmt"

	"github.com/gogf/gf/os/glog"

	"go.etcd.io/etcd/clientv3"

	"go.etcd.io/etcd/clientv3/concurrency"
)

type EtcdCtl struct {
	*clientv3.Client
}

// EtcdCtlResult 多返回值数据体定义
type EtcdCtlResult struct {
	Data interface{}
	Err  error
}

func New(config clientv3.Config) *EtcdCtl {
	var err error
	// 建立一个客户端
	etcdClient, err := clientv3.New(config)
	if err != nil {
		glog.Error(err)
		panic(err)
	}
	return &EtcdCtl{
		Client: etcdClient,
	}
}

// Lock 加锁
// 如果没有获取会一直等待 不同于redis没获取就直接返回 这个本身就属于等待锁定 这样就省去了一直循环或者递归的去尝试获取锁了
// lockKey 业务名  比如submit/productId
// f 要执行的函数 不需要返回值就返回nil,需要返回多个就返回[]interface{} 转换方法:data.([]interface{})
func (e *EtcdCtl) Lock(lockKey string, f func() interface{}) (data interface{}) {
	lockKey = "etcd-lock/" + lockKey
	// create two separate sessions for lock competition
	s1, err := concurrency.NewSession(e.Client)
	if err != nil {
		glog.Fatal(err)
	}
	defer s1.Close()
	m1 := concurrency.NewMutex(s1, lockKey)

	// 获取锁，如果锁被其他进程占用，则进入阻塞状态
	if err := m1.Lock(context.TODO()); err != nil {
		glog.Error(err)
	}
	// 执行任务
	data = f()
	fmt.Println("acquired lock for s1")
	// 释放锁
	if err := m1.Unlock(context.TODO()); err != nil {
		glog.Fatal(err)
	}
	fmt.Println("released lock for s1")
	return data
}
