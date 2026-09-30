package sse

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	projectredis "feige-cloud-backend/pkg/redis"
)

// SSE 全局变量（初始化模式前由boot注入），当前已改为instance.SSE()用法，保留作历史参考
// var SSE *Service

// TestSSE 本地集成测试：需要本机跑着一个无密码redis（默认127.0.0.1:6379）
// 跑法：go test ./pkg/sse/ -run TestSSE -v
func TestSSE(t *testing.T) {
	rdb, err := projectredis.New(&goredis.Options{Addr: "127.0.0.1:6379", DB: 0})
	if err != nil {
		t.Skipf("本地redis不可用，跳过: %v", err)
	}
	client := rdb.Rdb()

	svc := New(&Config{
		Rds:        client,
		NodeId:     1,
		ServerName: "test-app",
	})
	defer svc.Close()

	// 1. 广播：无客户端连接时仅publish，各节点订阅协程自然消费
	svc.BroadcastMessage("群发", 1)

	// 2. 单推离线用户：应落入离线队列
	svc.SendToUser(99999, "私聊-用户不在线应落离线队列", 2)
	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	offlineKey := "test-app:sse:offline-msg:99999"
	n, err := client.LLen(ctx, offlineKey).Result()
	if err != nil || n != 1 {
		t.Fatalf("离线消息应落队列1条, got n=%v err=%v", n, err)
	}

	// 3. 验证离线消息内容可通过RPOP取回
	val, err := client.RPop(ctx, offlineKey).Bytes()
	if err != nil {
		t.Fatalf("RPop离线消息失败: %v", err)
	}
	if len(val) == 0 {
		t.Fatal("离线消息内容为空")
	}
	t.Logf("离线消息内容: %s", val)

	// 4. 清理测试key
	client.Del(ctx, offlineKey, "test-app:sse:node:clients:1")
}

// TestSSECrossService 跨服务推送集成测试
// 用两个Service实例模拟"服务A → 服务B"：两个实例共用一个Redis、各自有独立的服务名和节点id，
// 等价于生产环境两台机器上跑着两个服务（真实部署时服务名由 Config.ServerName 决定）。
//
// 前提：目标服务名必须在 Config.RemoteServerNames 白名单内，否则推送会被静默丢弃（deny by default）
// 跑法：go test ./pkg/sse/ -run TestSSECrossService -v
func TestSSECrossService(t *testing.T) {
	rdb, err := projectredis.New(&goredis.Options{Addr: "127.0.0.1:6379", DB: 0})
	if err != nil {
		t.Skipf("本地redis不可用，跳过: %v", err)
	}
	client := rdb.Rdb()
	ctx := context.Background()

	// 服务A：发起推送方，白名单里允许推给 app-b
	svcA := New(&Config{
		Rds:               client,
		NodeId:            1,
		ServerName:        "app-a",
		RemoteServerNames: []string{"app-b"},
	})
	defer svcA.Close()

	// 服务B：接收推送方，真实场景中它就是另一台机器上的服务进程
	// 预埋一条"上次运行残留"的会话记录：New()会在返回前同步清理掉它，
	// 因此之后注册的客户端不会被误清（这也是同步清理要保证的语义）
	client.SAdd(ctx, "app-b:sse:node:clients:2", "1:stale")
	svcB := New(&Config{
		Rds:        client,
		NodeId:     2, // 节点id与A不同，模拟两台机器
		ServerName: "app-b",
	})
	defer svcB.Close()
	if client.Exists(ctx, "app-b:sse:node:clients:2").Val() != 0 {
		t.Fatal("New()应同步完成启动清理，残留会话集合应已被删除")
	}

	// 在服务B上手工注册一个"在线设备"（真实场景是客户端连上B的 /sse 端点后注册）
	const onlineUser = int64(88888)
	remoteClient := &Client{
		userId:      onlineUser,
		UUID:        "b-dev-1",
		messageChan: make(chan string, 10),
		done:        make(chan struct{}),
	}
	svcB.registerClient(remoteClient)

	// 等B的订阅协程就绪：B启动后异步订阅自己的节点频道和广播频道，
	// 订阅没建立前publish的消息会丢（pub/sub不重放），用PUBSUB NUMSUB确认订阅者数量
	waitSubscribed(t, ctx, client, "app-b:sse:cluster:2", "app-b:sse:cluster:-1")

	// 场景1：跨服务单推（目标用户在B在线）
	// 链路：A查"app-b:sse:user:clients:88888"拿到设备 → 查会话key拿到节点id=2 →
	//       publish到"app-b:sse:cluster:2" → B收到 → 投递给本地的b-dev-1
	svcA.SendToUser(onlineUser, "跨服务单推-在线", 1, "app-b")
	select {
	case data := <-remoteClient.messageChan:
		t.Logf("✓ 场景1 跨服务实时单推已到达B的设备: %s", data)
	case <-time.After(2 * time.Second):
		t.Fatal("场景1失败: 2秒内未收到跨服务实时消息")
	}

	// 场景2：跨服务单推（目标用户在B离线）
	// 目标服务查不到在线设备 → 消息落到B的离线队列，B的用户下次连上时自动收到
	const offlineUser = int64(77777)
	svcA.SendToUser(offlineUser, "跨服务单推-离线", 1, "app-b")
	time.Sleep(200 * time.Millisecond)
	offlineKey := "app-b:sse:offline-msg:77777"
	if n := client.LLen(ctx, offlineKey).Val(); n != 1 {
		t.Fatalf("场景2失败: 应落B的离线队列1条, got %d", n)
	}
	t.Log("✓ 场景2 跨服务离线消息已落入 app-b 的离线队列")

	// 场景3：跨服务广播
	// publish到"app-b:sse:cluster:-1"（-1是广播频道）→ B的所有节点收到 → 各自投递本地在线设备
	svcA.BroadcastMessage("跨服务广播", 2, "app-b")
	select {
	case data := <-remoteClient.messageChan:
		t.Logf("✓ 场景3 跨服务广播已到达B的设备: %s", data)
	case <-time.After(2 * time.Second):
		t.Fatal("场景3失败: 2秒内未收到跨服务广播")
	}

	// 场景4：白名单校验
	// 目标服务名不在RemoteServerNames里时整条消息被丢弃（只记日志，无任何Redis写入）
	svcA.SendToUser(offlineUser, "不该送达", 1, "not-in-allowlist")
	time.Sleep(200 * time.Millisecond)
	if client.Exists(ctx, "not-in-allowlist:sse:offline-msg:77777").Val() != 0 {
		t.Fatal("场景4失败: 白名单外的服务名不应产生任何离线消息")
	}
	t.Log("✓ 场景4 白名单外的服务名已被静默丢弃（服务日志可见 allowlist 提示）")

	// 清理测试数据（各服务自身的会话key由Close时自动清理）
	client.Del(ctx, offlineKey)
}

// waitSubscribed 等待指定频道的订阅者就绪（pub/sub消息不重放，订阅建立前的publish会丢失）
func waitSubscribed(t *testing.T, ctx context.Context, client goredis.UniversalClient, channels ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		res, err := client.PubSubNumSub(ctx, channels...).Result()
		if err == nil {
			ready := true
			for _, ch := range channels {
				if res[ch] == 0 {
					ready = false
					break
				}
			}
			if ready {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待频道订阅就绪超时: %v", channels)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
