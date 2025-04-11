package test

import (
	"fmt"
	"sync"
	"testing"
)

type Request struct {
	batchSize int
	Lock      sync.Mutex
}

func NewRequest(batchSize int) *Request {
	return &Request{
		batchSize: batchSize,
	}
}

func Test_1(t *testing.T) {
	r := NewRequest(100)
	r.process()
	fmt.Println(r.batchSize)
}

func (r *Request) process() {
	rr := r
	rr.batchSize = 1200
}
