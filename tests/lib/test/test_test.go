package test

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"testing"
	"time"
)

var smsChan = make(chan int, 100000) // 10万缓冲
// SendSMSHandler 生产者（接口层）
func SendSMSHandler(params int) {
	smsChan <- params // 非阻塞推送
}

func SendSmsProcess() {
	go func() {
		limiter := rate.NewLimiter(rate.Limit(1000), 1) // 每秒1000条
		for req := range smsChan {
			limiter.Wait(context.Background())
			go func() {
				fmt.Printf("1,%d\n", req)
			}()
		}
	}()
}

func Test_1(t *testing.T) {
	go func() {
		for {
			SendSMSHandler(1)
			time.Sleep(time.Second)
		}

	}()
	SendSmsProcess()
	go func() {
		for req := range smsChan {
			go func() {
				fmt.Printf("1,%d\n", req)
			}()
		}
	}()
	select {}
}
