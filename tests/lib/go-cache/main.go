package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
)

type MyStruct struct {
	Name string
}

func main() {
	// 1. 基本缓存操作
	// 1.1 创建一个缓存，默认过期时间为 5 分钟，每 10 分钟清理一次过期项 测试过了过期时间就失效了，第二个参数10分钟不一定是按照设定的来执行的
	c := cache.New(5*time.Minute, 10*time.Minute)

	// 1.2 设置缓存项
	// 使用默认过期时间
	c.Set("key1", "value1", cache.DefaultExpiration)
	// 设置无过期时间
	c.Set("key2", 123, cache.NoExpiration)
	// 自定义过期时间
	c.Set("key3", "customValue", 2*time.Minute)

	// 1.3 获取缓存项
	if value, found := c.Get("key1"); found {
		fmt.Printf("获取 key1 的值: %v\n", value)
	}
	if value, found := c.Get("key2"); found {
		fmt.Printf("获取 key2 的值: %v\n", value)
	}

	var foos string
	// 获取值， 并断言
	if x, found := c.Get("key1"); found {
		foos = x.(string)
		fmt.Println(foos)
	}
	// 对结构体指针进行操作
	var my *MyStruct
	c.Set("foo", &MyStruct{Name: "NameName"}, cache.DefaultExpiration)
	if x, found := c.Get("foo"); found {
		my = x.(*MyStruct)
		// ...
	}
	fmt.Println(my)

	// 1.4 删除缓存项
	c.Delete("key1")

	// 1.5 尝试获取不存在的缓存项
	if _, found := c.Get("key1"); !found {
		fmt.Println("key1 不存在于缓存中")
	}

	// 2. 数值增减操作
	// 2.1 增加缓存项的值
	c.Set("key1", 1, cache.DefaultExpiration)
	if newVal, err := c.IncrementInt("key1", 1); err == nil {
		fmt.Printf("增加 key1 的值后为: %d\n", newVal)
	}

	// 2.2 减少缓存项的值
	if newVal, err := c.DecrementInt("key1", 1); err == nil {
		fmt.Printf("减少 key1 的值后为: %d\n", newVal)
	}

	// 3. 缓存项替换和添加
	// 3.1 替换操作 使用 Replace 方法替换已存在且未过期的缓存项。
	err := c.Replace("key2", "newBar", cache.DefaultExpiration)
	if err != nil {
		println(err.Error())
	}

	// 3.2 添加操作 使用 Add 方法仅在键不存在或已过期时添加缓存项。
	err = c.Add("newKey", "newValue", cache.DefaultExpiration)
	if err != nil {
		println(err.Error())
	}

	// 获取所有未过期的缓存项
	items := c.Items()
	fmt.Println("所有未过期的缓存项:")
	for k, v := range items {
		fmt.Printf("Key: %s, Value: %v\n", k, v.Object)
	}

	// 4. 缓存持久化

	// 序列化缓存到文件
	fileName := "cache.data"
	if err := c.SaveFile(fileName); err != nil {
		fmt.Printf("保存缓存到文件时出错: %v\n", err)
	} else {
		fmt.Println("缓存已保存到文件")
	}

	// 创建一个新的缓存实例
	newCache := cache.New(5*time.Minute, 10*time.Minute)
	// 从文件中加载缓存
	if err := newCache.LoadFile(fileName); err != nil {
		fmt.Printf("从文件加载缓存时出错: %v\n", err)
	} else {
		fmt.Println("缓存已从文件加载")
		// 验证加载的缓存项
		if value, found := newCache.Get("key1"); found {
			fmt.Printf("从新缓存中获取 key1 的值: %v\n", value)
		}
	}

	// 删除文件
	if err := os.Remove(fileName); err != nil {
		fmt.Printf("删除文件时出错: %v\n", err)
	} else {
		fmt.Println("文件已删除")
	}

	// 5.回调函数
	// 使用 OnEvicted 方法设置回调函数，当缓存项被删除时触发。
	c.OnEvicted(func(key string, value interface{}) {
		println("Evicted key:", key)
	})

	c.Delete("key1")

	// 6.并发安全
	// go-cache 是线程安全的，可在多个 goroutine 中安全使用。
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.Set("goroutineKey1", "value1", cache.NoExpiration)
	}()

	go func() {
		defer wg.Done()
		c.Set("goroutineKey2", "value2", cache.NoExpiration)
	}()

	wg.Wait()

}
