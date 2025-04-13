package util

import (
	"fmt"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

type SECOND_COUNT int64

var (
	// SECOND_SEC 秒数的值
	SECOND_SEC SECOND_COUNT = 1
	MINUTE_SEC              = SECOND_SEC * 60
	HOUR_SEC                = MINUTE_SEC * 60
	DAY_SEC                 = HOUR_SEC * 24
)

func GetNow() string {
	return gtime.Datetime()
}

// GetTodayLatestTime 获取当天最晚时间
func GetTodayLatestTime() *gtime.Time {
	gtime.Now().Format("Y-m-d 23:59:59")
	timeStr := gtime.Now().Format("Y-m-d 23:59:59")
	tonightTime := gtime.NewFromStrFormat(timeStr, "Y-m-d H:i:s")
	return tonightTime
}

// IsSameDay 判断是否是同一天
func IsSameDay(time1 *gtime.Time, time2 *gtime.Time) bool {
	timeStr1 := time1.Format("Y-m-d")
	timeStr2 := time2.Format("Y-m-d")
	return timeStr1 == timeStr2
}

// GetPastTimeDescribe 获取对过去时间的描述
//
//	时间范围	描述
//	1分钟内		刚刚
//	1小时内		58分钟前
//	1天内		23小时前
//	2天内		昨天
//	1年内		2020-10-03
//	1年外		2年前
func GetPastTimeDescribe(pastTime *gtime.Time) string {
	if g.IsEmpty(pastTime) {
		return ""
	}

	pastTimeSec := pastTime.Unix() // 过去的时间戳(秒)
	nowSec := gtime.Now().Unix()   // 当前的时间戳(秒)
	//nowSec := gtime.NewFromStrFormat("2020-10-05 12:00:00","Y-m-d H:i:s").Unix()	// 测试

	pastSec := nowSec - pastTimeSec // 逝去的秒数
	if pastSec < 0 {
		//TODO 目前不支持对未来时间的描述
		return ""
	}

	if pastSec <= int64(MINUTE_SEC) { // 1分钟内
		return "刚刚"
	} else if pastSec <= int64(HOUR_SEC) { // 1小时内
		return fmt.Sprintf("%v分钟前", pastSec/int64(MINUTE_SEC))
	} else if pastSec <= int64(DAY_SEC) { // 1天内
		return fmt.Sprintf("%v小时前", pastSec/int64(HOUR_SEC))
	} else if pastSec <= int64(DAY_SEC)*2 { // 2天内
		return "昨天"
	} else if pastSec <= int64(DAY_SEC)*365 { // 1年内
		return pastTime.Format("Y-m-d")
	} else { // 1年外
		return fmt.Sprintf("%v年前", pastSec/(int64(DAY_SEC)*365))
	}
}
