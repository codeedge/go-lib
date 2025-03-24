package util

import (
	"regexp"
)

// 手提电话（不带前缀）最高11位
func IsMobile(str ...string) bool {
	var b bool
	for _, s := range str {
		b, _ = regexp.MatchString("^1[3-9]{1}[0-9]{9}$", s)
		if false == b {
			return b
		}
	}
	return b
}

// 邮箱 最高30位
func IsEmail(str ...string) bool {
	var b bool
	for _, s := range str {
		b, _ = regexp.MatchString("^([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$", s)
		if false == b {
			return b
		}
	}
	return b
}
