package gopool

import (
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2/pkg/pool/goroutine"
	"sync"
	"time"
)

// 扩展自定义协程池
type extPool struct {
	p    *goroutine.Pool
	name string
}

func (p *extPool) Submit(f func()) bool {
	err := p.p.Submit(f)
	for err != nil && errors.Is(err, ants.ErrPoolOverload) {
		fmt.Printf("协程池超载稍后重试,%v", p.name)
		time.Sleep(1 * 100 * time.Millisecond)
		err = p.p.Submit(f)
		if err == nil {
			fmt.Printf("协程池超载重试成功,%v", p.name)
			return true
		}
	}
	if err != nil {
		fmt.Printf("提交任务失败: %v %v", p.name, err)
		return false
	}
	return true
}

var sysPool *extPool // 协程池实例
var sysPoolOnce sync.Once

// SysPool 系统协程池
func SysPool(size ...int) *extPool {
	sysPoolOnce.Do(func() {
		// 创建一个新的协程池实例
		sysPool = &extPool{
			p:    CreatePool(size...),
			name: "系统协程池",
		}
	})
	return sysPool
}

var smsPool *extPool // 协程池实例
var smsPoolOnce sync.Once

// SmsPool 系统协程池
func SmsPool(size ...int) *extPool {
	sysPoolOnce.Do(func() {
		// 创建一个新的协程池实例
		sysPool = &extPool{
			p:    CreatePool(size...),
			name: "短信协程池",
		}
	})
	return sysPool
}
