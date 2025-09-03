package internal

import (
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"
	"testing"
)

// GO 编程项目如何自定义迭代器？ https://www.zhihu.com/question/589450046/answer/3536826730

func Test_Iter(t *testing.T) {
	m := map[string]string{"1": "a", "2": "b"}
	keys := slices.Collect(maps.Keys(m))
	values := slices.Collect(maps.Values(m))
	fmt.Println(keys, values)
}

func Test_MyRange(t *testing.T) {
	for value := range MyRange(1, 3) {
		fmt.Println(value) // 1 2 3
	}
	// iter.Seq泛型方法迭代器，v 1个参数，返回bool值的函数都可以使用迭代器
	fmt.Println(slices.Collect(MyRange(1, 3))) // [1 2 3]
}

func MyRange(start, end int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for current := start; current <= end; current++ {
			if !yield(current) {
				return
			}
		}
	}
}

func IntSeq(yield func(int) bool) {
	for i := 0; i < 10; i++ {
		if !yield(i) {
			break
		}
	}
}

var s iter.Seq[int] = IntSeq

// 其实上面的s就是下面的这种的泛型
type IntSeq2 func(yield func(int) bool)

var s2 IntSeq2 = IntSeq

func Test_IntSeq(t *testing.T) {
	s(func(v int) bool {
		fmt.Println(v)
		return true
	})

	// for range迭代器其实就是上面s调用方法的语法糖
	for v := range IntSeq {
		fmt.Println(v) // 0 1 2 3 4 5 6 7 8 9
	}
	// 如果要 break，脱糖后就得到了 return false。
	// 这个设计，可谓是为了不增加一个 yield 关键字而煞费苦心。
}

func Test_SyncMap(t *testing.T) {
	var m sync.Map

	m.Store("alice", 11)
	m.Store("bob", 12)
	m.Store("cindy", 13)

	// 1.22
	m.Range(func(key, value any) bool {
		fmt.Println(key, value)
		return true
	})

	// 1.23
	for key, val := range m.Range {
		fmt.Println(key, val)
	}

	// iter.Seq2泛型方法迭代器，k v 2个参数，返回bool值的函数都可以使用迭代器
	mp := maps.Collect(m.Range)
	fmt.Println(mp)
}
