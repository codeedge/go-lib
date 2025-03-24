package util

import (
	"container/list"
	"errors"
	"fmt"
)

// Base34生成邀请码算法
var base []byte
var baseMap map[byte]int

// 邀请码长度 默认是6位
var length int

// 初始化邀请码Key，先调用此再编码解码
// _len 邀请码长度 默认是6
func InitBaseMap(invitationCodeKey string, _len ...int) {
	if len(_len) > 0 {
		length = _len[0]
	} else {
		length = 6
	}

	base = []byte(invitationCodeKey)
	baseMap = make(map[byte]int)
	for i, v := range base {
		baseMap[v] = i
	}
}
func Base34(n uint64) []byte {
	quotient := n
	mod := uint64(0)
	l := list.New()
	for quotient != 0 {
		//fmt.Println("---quotient:", quotient)
		mod = quotient % 34
		quotient = quotient / 34
		l.PushFront(base[int(mod)])
		//res = append(res, base[int(mod)])
		//fmt.Printf("---mod:%d, base:%s\n", mod, string(base[int(mod)]))
	}
	listLen := l.Len()

	if listLen >= length {
		res := make([]byte, 0, listLen)
		for i := l.Front(); i != nil; i = i.Next() {
			res = append(res, i.Value.(byte))
		}
		return res
	} else {
		res := make([]byte, 0, length)
		for i := 0; i < length; i++ {
			if i < length-listLen {
				res = append(res, base[0])
			} else {
				res = append(res, l.Front().Value.(byte))
				l.Remove(l.Front())
			}
		}
		return res
	}

}

func Base34ToNum(str []byte) (uint64, error) {
	if baseMap == nil {
		return 0, errors.New("no init base map")
	}
	if str == nil || len(str) == 0 {
		return 0, errors.New("parameter is nil or empty")
	}
	var res uint64 = 0
	var r uint64 = 0
	for i := len(str) - 1; i >= 0; i-- {
		v, ok := baseMap[str[i]]
		if !ok {
			fmt.Printf("")
			return 0, errors.New("character is not base")
		}
		var b uint64 = 1
		for j := uint64(0); j < r; j++ {
			b *= 34
		}
		res += b * uint64(v)
		r++
	}
	return res, nil
}
