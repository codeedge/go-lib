package rabbitmq

import "fmt"

// PermanentError 定义一个标记接口或类型来识别永久性错误
type PermanentError interface {
	Permanent() bool
	Error() string
}

// PermanentFailure 结构体，用于标记不应重试的错误
type PermanentFailure struct {
	Msg string
}

// 实现 error 接口
func (e *PermanentFailure) Error() string {
	return fmt.Sprintf("permanent failure: %s", e.Msg)
}

// Permanent 实现 PermanentError 接口
func (e *PermanentFailure) Permanent() bool {
	return true
}

// NewPermanentError 辅助函数，方便创建永久性错误
func NewPermanentError(msg string) error {
	return &PermanentFailure{Msg: msg}
}
