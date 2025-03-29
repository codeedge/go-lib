package gcache

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/gcache"
	"testing"
	"time"
)

func Test_gcache_01(t *testing.T) {
	// 创建一个缓存对象，
	// 当然也可以便捷地直接使用gcache包方法
	//c := gcache.New()
	c := gcache.NewAndListener(func(key interface{}, value interface{}, reason gcache.RemoveReason) {
		fmt.Printf("key=%v, value=%v, reasion = %v\n", key, value, reason)
	})

	// 设置缓存，不过期
	c.Set("k1", "v1", 0)
	c.Set("k2", "v2", 1*time.Second)

	// 获取缓存
	fmt.Println(c.Get("k1"))

	// 获取缓存大小
	fmt.Println(c.Size())

	// 缓存中是否存在指定键名
	fmt.Println(c.Contains("k1"))

	// 删除并返回被删除的键值
	fmt.Println(c.Remove("k1"))

	// 等待1秒，以便k1:v1自动过期
	time.Sleep(2 * time.Second)

	// 关闭缓存对象，让GC回收资源
	c.Close()
}

func Test_gcache_02(t *testing.T) {
	// 当键名不存在时写入，设置过期时间1000毫秒
	gcache.SetIfNotExist("k1", "v1", 1000*time.Millisecond)

	// 打印当前的键名列表
	fmt.Println(gcache.Keys())

	// 打印当前的键值列表
	fmt.Println(gcache.Values())

	// 获取指定键值，如果不存在时写入，并返回键值
	fmt.Println(gcache.GetOrSet("k2", "v2", 0))

	// 打印当前的键值对
	fmt.Println(gcache.Data())

	// 等待1秒，以便k1:v1自动过期
	time.Sleep(2 * time.Second)

	// 再次打印当前的键值对，发现k1:v1已经过期，只剩下k2:v2
	fmt.Println(gcache.Data())
}

func Test_gcache_03(t *testing.T) {
	c := gcache.NewAndListener(func(key interface{}, value interface{}, reason gcache.RemoveReason) {
		fmt.Printf("key=%v, value=%v, reasion = %v\n", key, value, reason)
	})

	// 当键名不存在时写入，设置过期时间1000毫秒
	c.SetIfNotExist("k1", "v1", 1000*time.Millisecond)

	// 打印当前的键名列表
	fmt.Println(c.Keys())

	// 打印当前的键值列表
	fmt.Println(c.Values())

	// 获取指定键值，如果不存在时写入，并返回键值
	fmt.Println(c.GetOrSet("k2", "v2", 0))

	// 打印当前的键值对
	fmt.Println(c.Data())

	// 等待1秒，以便k1:v1自动过期
	time.Sleep(2 * time.Second)

	// 再次打印当前的键值对，发现k1:v1已经过期，只剩下k2:v2
	fmt.Println(c.Data())
}
