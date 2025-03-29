package goftp

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/util"
	"github.c
	"testing"
)

func Test_time_01(t *testing.T) {

	fmt.Println(util.IsSameDay(gtime.Now(), gtime.ParseTimeFromContent("2020-03-12")))
	fmt.Println(util.IsSameDay(gtime.Now(), gtime.ParseTimeFromContent("2020-03-13")))
}
