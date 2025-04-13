package main

import (
	"context"
	"errors"
)

type Student struct {
	ClassNo int
}

// NewStudent 就是一个 provider，返回一个 Student
func NewStudent() Student {
	return Student{ClassNo: 10}
}

type Class struct {
	ClassNo int
}

// NewClass 就是一个 provider，返回一个 Class
func NewClass(stu Student) Class {
	return Class{ClassNo: stu.ClassNo}
}

type School struct {
	ClassNo int
}

// NewSchool 是一个 provider，返回一个 School
// 与上面 provider 不同的是，它还返回了一个错误信息
func NewSchool(ctx context.Context, class Class) (School, error) {
	if class.ClassNo == 0 {
		return School{}, errors.New("cannot provider school when class is 0")
	}
	return School{ClassNo: class.ClassNo}, nil
}
