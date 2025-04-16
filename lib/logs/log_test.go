package logs

import (
	"testing"
)

func TestInfo(t *testing.T) {
	NewFgLogger("", "", 10, 3, "debug")
	Debug("这是一条Debug日志")
	Info("这是一条Info日志")
	Warn("这是一条Warn日志")
	Error("这是一条Error日志")
	for i := 0; i < 200000; i++ {
		Data("这是一条Data日志")
	}
}
