package goftp

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/util"
	"github.com/gogf/gf/v2/os/gtime"
	"testing"
)

func Test_time_01(t *testing.T) {

	fmt.Println(util.IsSameDay(gtime.Now(), gtime.ParseTimeFromContent("2020-03-12")))
	fmt.Println(util.IsSameDay(gtime.Now(), gtime.ParseTimeFromContent("2020-03-13")))
}
