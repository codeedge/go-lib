/*
高效的生成随机字符串方式
from: Golang 生成随机字符串的八种方式与性能测试 https://xie.infoq.cn/article/f274571178f1bbe6ff8d974f3
*/
package util

import (
	"math/rand"
	"time"
	"unsafe"
)

var letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

func SetLetters(str string) {
	letters = str
}

var src = rand.NewSource(time.Now().UnixNano())

const (
	// 6 bits to represent a letter index
	letterIdBits = 6
	// All 1-bits as many as letterIdBits
	letterIdMask = 1<<letterIdBits - 1
	letterIdMax  = 63 / letterIdBits
)

func RandStr(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdMax
		}
		if idx := int(cache & letterIdMask); idx < len(letters) {
			b[i] = letters[idx]
			i--
		}
		cache >>= letterIdBits
		remain--
	}
	return *(*string)(unsafe.Pointer(&b))
}
