/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package singleflight provides a duplicate function call suppression
// mechanism.
// 在一个服务中抑制对下游的多次重复请求。也就是说，如果对于某个key的请求已经存在并且正在进行，
// 则对该key的新的请求会堵塞在这里，等原来的请求结束后，将请求得到的结果同时返回给堵塞中的请求。
// 可用于处理缓存失效风暴（dog-pile effect）
// 一个比较常见的使用场景是 — 我们在使用 Redis 对数据库中的一些热门数据进行了缓存并设置了超时时间，
// 缓存超时的一瞬间可能有非常多的并行请求发现了 Redis 中已经不包含任何缓存所以大量的流量会打到数据库上影响服务的延时和质量。
// 但是 singleflight 就能有效地解决这个问题，它的主要作用就是对于同一个 Key 最终只会进行一次函数调用，
// 在这个上下文中就是只会进行一次数据库查询，查询的结果会写回 Redis 并同步给所有请求对应 Key 的用户：
package singleflight

import "sync"

// call is an in-flight or completed Do call
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
