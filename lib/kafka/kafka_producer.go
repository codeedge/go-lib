package kafka

import (
	"bytes"
	"encoding/json"
	"github.com/codeedge/go-lib/lib/gpool"
	"github.com/codeedge/go-lib/lib/util"
	"strconv"
	"time"
)

// 协程池handlerName
const (
	Kafka_AsyncProducerData  = "kafka.AsyncProducerData"
	Kafka_AsyncProducerDepth = "kafka.AsyncProducerDepth"
)

// 异步生产消息
// topic 主题
// data 数据
func AsyncProducerData(topic string, data interface{}) {
	gpool.AddTaskByHandlerName(Kafka_AsyncProducerData, &gpool.Task{
		Handler: func(v ...interface{}) {
			var bt []byte
			switch v := data.(type) {
			case string:
				bt = []byte(v)
			case []byte:
				bt = v
			default:
				bt, _ = json.Marshal(data)
			}
			Input(&Message{
				Topic: topic,
				Body:  bt,
			})
		},
	})
}

// 异步生产消息  多步骤消息使，消费端调用，消费成功后调用发送下一步的消息
// topic 主题
// data  数据
// uuid  要替换的原消息uuid
// depth 消费的深度，如果存在则把消息的ConsumerDepth从depth-1改为depth
func AsyncProducerDepth(topic string, data interface{}, uuid, date, dateTime string, depth ...int) {
	gpool.AddTaskByHandlerName(Kafka_AsyncProducerDepth, &gpool.Task{
		Handler: func(v ...interface{}) {
			var bt []byte
			switch v := data.(type) {
			case string:
				bt = []byte(v)
			case []byte:
				bt = v
			default:
				bt, _ = json.Marshal(data)
			}
			//深度从2开始才需要处理，第一步无需重发 重新生成uuid
			if len(depth) > 0 && depth[0] > 1 {
				bt = bytes.Replace(bt, []byte(`"cd":`+strconv.Itoa(depth[0]-1)), []byte(`"cd":`+strconv.Itoa(depth[0])), 1)             //修改消费深度
				bt = bytes.Replace(bt, []byte(`"uuid":`+uuid), []byte(`"uuid":`+util.UUID()), 1)                                        //修改uuid
				bt = bytes.Replace(bt, []byte(`"d":"`+date+`"`), []byte(`"d":"`+time.Now().Format("2006-01-02")+`"`), 1)                //修改消息时间
				bt = bytes.Replace(bt, []byte(`"dt":"`+dateTime+`"`), []byte(`"dt":"`+time.Now().Format("2006-01-02 15:04:05")+`"`), 1) //修改消息时间
			}
			Input(&Message{
				Topic: topic,
				Body:  bt,
			})
		},
	})
}
