package kafka

import (
	"github.com/codeedge/go-lib/lib/kafka"
	"testing"
	"time"
)

// 需要配置文件才能执行
func Test_kafka_01(t *testing.T) {
	for i := 0; i < 20; i++ {
		kafka.Input(&kafka.Message{
			Topic: "test",
			Body:  []byte("this is a message  " + time.Now().Format("15:04:05")),
		})
		time.Sleep(time.Second * 1)
	}
}
