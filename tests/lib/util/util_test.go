package goftp_test

import (
	"github.com/codeedge/go-lib/lib/util"
	"testing"
)

func Test_1(t *testing.T) {
	var s = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	s = util.SliceDel(s, 5)
	t.Log(s)
}
