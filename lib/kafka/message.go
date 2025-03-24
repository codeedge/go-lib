package kafka

type Message struct {
	Topic string
	Key   string
	Body  []byte //消息体
}
