package goftp

import (
	"fmt"
	"github.com/gogf/gf/os/gtime"
	"github.com/kdcer/go-lib/lib/util"
	"testing"
)

func Test_time_01(t *testing.T) {

	fmt.Println(util.IsSameDay(gtime.Now(), gtime.ParseTimeFromContent("2020-03-12")))
	fmt.Println(util.IsSameDay(gtime.Now(), gtime.ParseTimeFromContent("2020-03-13")))
}
