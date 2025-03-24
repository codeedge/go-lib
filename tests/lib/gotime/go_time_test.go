package gotime

import (
	"fmt"
	"github.com/gogf/gf/os/gtime"
	"github.com/kdcer/go-lib/lib/util"
	"testing"
)

func Test_GetPastTimeDescribe(t *testing.T) {
	//now := gtime.Now()
	now := gtime.NewFromStrFormat("2020-10-05 12:00:00", "Y-m-d H:i:s")
	// 1分钟内
	pastTime := gtime.NewFromStrFormat("2020-10-05 11:59:57", "Y-m-d H:i:s")
	fmt.Printf("过去的时间==>>%v, 当前的时间==>>%v, 描述:%v\n", now.Format("Y-m-d H:i:s"), pastTime, util.GetPastTimeDescribe(pastTime))

	// 1小时内
	pastTime = gtime.NewFromStrFormat("2020-10-05 11:01:57", "Y-m-d H:i:s")
	fmt.Printf("过去的时间==>>%v, 当前的时间==>>%v, 描述:%v\n", now.Format("Y-m-d H:i:s"), pastTime, util.GetPastTimeDescribe(pastTime))

	// 1天内
	pastTime = gtime.NewFromStrFormat("2020-10-04 12:01:57", "Y-m-d H:i:s")
	fmt.Printf("过去的时间==>>%v, 当前的时间==>>%v, 描述:%v\n", now.Format("Y-m-d H:i:s"), pastTime, util.GetPastTimeDescribe(pastTime))

	// 2天内
	pastTime = gtime.NewFromStrFormat("2020-10-03 12:01:57", "Y-m-d H:i:s")
	fmt.Printf("过去的时间==>>%v, 当前的时间==>>%v, 描述:%v\n", now.Format("Y-m-d H:i:s"), pastTime, util.GetPastTimeDescribe(pastTime))

	// 1年内
	pastTime = gtime.NewFromStrFormat("2019-10-08 11:01:57", "Y-m-d H:i:s")
	fmt.Printf("过去的时间==>>%v, 当前的时间==>>%v, 描述:%v\n", now.Format("Y-m-d H:i:s"), pastTime, util.GetPastTimeDescribe(pastTime))

	// 1年外
	pastTime = gtime.NewFromStrFormat("2018-10-03 11:01:57", "Y-m-d H:i:s")
	fmt.Printf("过去的时间==>>%v, 当前的时间==>>%v, 描述:%v\n", now.Format("Y-m-d H:i:s"), pastTime, util.GetPastTimeDescribe(pastTime))

	// 1年外
	pastTime = gtime.NewFromStrFormat("2008-10-03 11:01:57", "Y-m-d H:i:s")
	fmt.Printf("过去的时间==>>%v, 当前的时间==>>%v, 描述:%v\n", now.Format("Y-m-d H:i:s"), pastTime, util.GetPastTimeDescribe(pastTime))
}

func Test_001(t *testing.T) {
	//now := gtime.Now()
	time1 := gtime.NewFromStrFormat("2020-10-05 12:00:00", "Y-m-d H:i:s")
	time2 := gtime.NewFromStrFormat("2021-10-05 12:00:00", "Y-m-d H:i:s")

	fmt.Printf("时间1==>>%v, 时间2==>>%v, time1.Day() == time2.Day() ==>>>:%v\n",
		time1.Format("Y-m-d H:i:s"), time2.Format("Y-m-d H:i:s"), time1.Day() == time2.Day())

}
