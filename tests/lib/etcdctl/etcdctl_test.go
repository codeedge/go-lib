package etcdctl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/clientv3"

	"github.com/codeedge/go-lib/lib/etcdctl"
)

func Test_Lock(t *testing.T) {
	etcd := etcdctl.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		Username:    "root",
		Password:    "123456",
	})
	res := etcd.Lock("lockKey", func() interface{} {
		fmt.Println(1)
		return 1
	})

	fmt.Println(res)
}

func Test_Kv(t *testing.T) {
	etcd := etcdctl.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		Username:    "root",
		Password:    "123456",
	})
	_, err := etcd.Put(context.Background(), "go/test/a", "1")
	if err != nil {
		fmt.Println(err)
	}

	_, err = etcd.Get(context.Background(), "go/test/a", clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
	}

	resp, err := etcd.Get(context.Background(), "", clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)

	var (
		kv      clientv3.KV
		getResp *clientv3.GetResponse
	)
	// 用于读写etcd的键值对
	kv = clientv3.NewKV(etcd.Client)

	// 写入
	kv.Put(context.TODO(), "name1", "lesroad")
	kv.Put(context.TODO(), "name2", "haha")

	// 读取name为前缀的所有key
	if getResp, err = kv.Get(context.TODO(), "name", clientv3.WithPrefix()); err != nil {
		fmt.Println(err)
		return
	} else {
		// 获取成功
		fmt.Println(getResp.Kvs)
	}

	// 删除name为前缀的所有key (文章这里写错了,不是WithPrevKV)
	if _, err = kv.Delete(context.TODO(), "name", clientv3.WithPrefix()); err != nil {
		fmt.Println(err)
		return
	}
}

func Test_Lease(t *testing.T) {
	var (
		err            error
		lease          clientv3.Lease
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
		putResp        *clientv3.PutResponse
		kv             clientv3.KV
		getResp        *clientv3.GetResponse
	)
	etcd := etcdctl.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		Username:    "root",
		Password:    "123456",
	})
	// 申请一个lease(租约)
	lease = clientv3.NewLease(etcd.Client)

	// 申请一个10秒的lease
	if leaseGrantResp, err = lease.Grant(context.TODO(), 10); err != nil {
		fmt.Println(err)
		return
	}

	// 拿到租约id
	leaseId = leaseGrantResp.ID

	// 获得kv api子集
	kv = clientv3.NewKV(etcd.Client)

	// put一个kv，让它与租约关联起来
	if putResp, err = kv.Put(context.TODO(), "name", "lbwnb", clientv3.WithLease(leaseId)); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("写入成功", putResp.Header.Revision)

	// 定时看下key过期了没有   getResp.Count == len(getResp.Kvs)  Count代表查询到的键的数量
	for {
		if getResp, err = kv.Get(context.TODO(), "name"); err != nil {
			fmt.Println(err)
			return
		}
		if getResp.Count == 0 {
			fmt.Println("kv过期")
			break
		}

		fmt.Println("还没过期", getResp.Kvs)
		time.Sleep(2 * time.Second)
	}
}

// 应用场景：
//
// 配置有更新的时候，etcd都会实时通知订阅者，以此达到获取最新配置信息的目的。
// 分布式日志收集，监控应用（主题）目录下所有信息的变动。
func Test_Watch(t *testing.T) {
	var (
		kv                 clientv3.KV
		watchStartRevision int64
		watcher            clientv3.Watcher
		watchRespChan      <-chan clientv3.WatchResponse
		watchResp          clientv3.WatchResponse
		event              *clientv3.Event
	)

	etcd := etcdctl.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		Username:    "root",
		Password:    "123456",
	})

	// 用于读写etcd的键值对
	kv = clientv3.NewKV(etcd.Client)

	// 模拟etcd中kv的变化
	go func() {
		for {
			kv.Put(context.TODO(), "name", "lesroad")

			kv.Delete(context.TODO(), "name")

			time.Sleep(1 * time.Second)
		}
	}()

	// 创建一个监听器
	watcher = clientv3.NewWatcher(etcd.Client)

	// 启动监听 5秒后关闭
	ctx, cancelFunc := context.WithCancel(context.TODO())
	time.AfterFunc(5*time.Second, func() {
		cancelFunc()
	})
	watchRespChan = watcher.Watch(ctx, "name", clientv3.WithRev(watchStartRevision))

	// 处理kv变化事件
	for watchResp = range watchRespChan {
		fmt.Println(len(watchResp.Events)) // 打印1
		for _, event = range watchResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				fmt.Println("修改为", string(event.Kv.Value))
			case mvccpb.DELETE:
				fmt.Println("删除了", string(event.Kv.Key))
			}
		}
	}
}

// op操作替代get、put
func Test_Op(t *testing.T) {
	var (
		err    error
		kv     clientv3.KV
		putOp  clientv3.Op
		getOp  clientv3.Op
		opResp clientv3.OpResponse
	)

	etcd := etcdctl.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		Username:    "root",
		Password:    "123456",
	})

	// 用于读写etcd的键值对
	kv = clientv3.NewKV(etcd.Client)

	// 模拟etcd中kv的变化
	go func() {
		for {
			kv.Put(context.TODO(), "name", "lesroad")

			kv.Delete(context.TODO(), "name")

			time.Sleep(1 * time.Second)
		}
	}()

	kv = clientv3.NewKV(etcd.Client)

	// 创建Op :operator
	putOp = clientv3.OpPut("op", "replace")

	// 执行Op 用kv.Do取代 kv.Put kv.Get...
	if opResp, err = kv.Do(context.TODO(), putOp); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("写入Revision", opResp.Put().Header.Revision)

	// 创建Op
	getOp = clientv3.OpGet("op")

	// 执行Op
	if opResp, err = kv.Do(context.TODO(), getOp); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("数据Revision", opResp.Get().Kvs[0].ModRevision)
	fmt.Println("数据value", string(opResp.Get().Kvs[0].Value))
}
