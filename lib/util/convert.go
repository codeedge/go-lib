/*
高效的string []byte互相转换方式
结构体互相转换
*/
package util

import (
	"encoding/json"
	"errors"
	"reflect"
	"unsafe"
)

// 摘自 strings.Builder
// 字节数组转换为字符串
func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// 字符串转换为字节数组
func Str2Bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

// 结构体互相转换 比gconv.Struct效率高
// source源结构体或结构体指针或结构体切片
// target目标结构体指针或结构体切片指针
// 不可用jsoniter库,会有数据没有附加回来
func Struct2Pointer(source, target interface{}) error {
	if source == nil {
		return errors.New("source cannot be nil")
	}
	if target == nil {
		return errors.New("target cannot be nil")
	}
	s, t := reflect.TypeOf(source), reflect.TypeOf(target)
	if k := s.Kind(); !(k == reflect.Struct || k == reflect.Slice || k == reflect.Ptr) {
		return errors.New("source must be struct or pointer or slice ")
	}
	if k := t.Kind(); !(k == reflect.Ptr || k == reflect.Slice) {
		return errors.New("target must be a pointer or slice ")
	}
	b, err := json.Marshal(source)
	if err != nil {
		return errors.New("serialization failed")
	}
	err = json.Unmarshal(b, &target)
	if err != nil {
		return errors.New("deserialization failed")
	}
	return nil
}
