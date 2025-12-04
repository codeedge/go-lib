package gopool

import (
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
)

const (
	maxPoolSize = 2000
)

var (
	pool    *ants.Pool
	onePool sync.Once
)

func Pool(sizes ...int) *ants.Pool {
	size := maxPoolSize
	if len(sizes) > 0 {
		size = sizes[0]
	}
	onePool.Do(func() {
		var options = ants.Options{
			ExpiryDuration:   time.Minute,
			Nonblocking:      false,
			MaxBlockingTasks: 0,
			PreAlloc:         false,
			PanicHandler: func(e interface{}) {
				log.Printf("panic recovered: %v\n 调用栈: %s", e, debug.Stack())
			},
		}
		var err error
		pool, err = ants.NewPool(size, ants.WithOptions(options))
		if err != nil {
			log.Printf("New Pool Error: %v", err)
		}
	})
	return pool
}

func CreatePool(sizes ...int) *ants.Pool {
	size := maxPoolSize
	if len(sizes) > 0 {
		size = sizes[0]
	}
	var options = ants.Options{
		ExpiryDuration:   time.Minute,
		Nonblocking:      false,
		MaxBlockingTasks: 0,
		PreAlloc:         false,
		PanicHandler: func(e interface{}) {
			log.Printf("panic recovered: %v\n 调用栈: %s\n", e, debug.Stack())
		},
	}
	p, err := ants.NewPool(size, ants.WithOptions(options))
	if err != nil {
		log.Printf("New Pool Error: %v", err)
	}
	return p
}
