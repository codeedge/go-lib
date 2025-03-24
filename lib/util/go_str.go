package util

import (
	"bytes"
)

// 隐藏字符
// param headLength 开头保留长度
// param tailLength 结尾保留长度
// param hide 替换隐藏的字符
func HideStr(str string, headLength, tailLength int, hide string) string {
	rs := []rune(str)
	length := len(rs)

	if headLength+tailLength >= length {
		tailLength = length / 2
		headLength = length - tailLength
	}

	hideLength := length - (headLength + tailLength)
	if hideLength <= 0 {
		hideLength = 1
	}

	hideStr := []rune("")
	hideRune := []rune(hide)
	for i := 0; i < hideLength; i++ {
		hideStr = append(hideStr, hideRune...)
	}

	buffer := bytes.NewBuffer(nil)
	buffer.WriteString(string(rs[0:headLength]))
	buffer.WriteString(string(hideStr))
	buffer.WriteString(string(rs[length-tailLength:]))

	return buffer.String()
}
