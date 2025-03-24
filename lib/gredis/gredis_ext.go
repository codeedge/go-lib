package gredis

import "github.com/gogf/gf/os/glog"

// 设置value, 如果redis里没有该值的话, 就会执行execFunc
func (r *Redigo) SetValueIfNoExistExecFunc(key string, value interface{}, execFunc func(), ex ...int64) (err error) {
	if len(ex) > 0 && ex[0] > 0 {
		_, err = r.SetArgs(key, value, "NX", "EX", ex[0])
	} else {
		_, err = r.SetArgs(key, value, "NX")
	}
	if err != nil {
		glog.Errorf("SetValueIfNoExistExecFunc 执行失败 key=%v, value=%v,ex=%v, err=%v", key, value, ex, err)
		return err
	}
	execFunc()
	return nil
}

// 获取指定数量的key
// @pointCount -1时扫描所有, >0为指定数量
// @eachPageExcFunc 每次扫描完执行该函数
func (r *Redigo) ScanDataAndExecFuc(cursor uint64, likeKey string, pointCount int, eachPageExcFunc func(arrays []string)) (uint64, []string) {
	eachCursor := cursor           // 每次的游标
	eachKeys := make([]string, 0)  // 每次获取到的keys
	totalKeys := make([]string, 0) // 所有的keys

	eachRows := 30
	if pointCount > 0 && pointCount <= eachRows {
		eachRows = pointCount
	}
	for {

		eachCursor, eachKeys, _ = r.Scan(eachCursor, likeKey, eachRows)

		if eachPageExcFunc != nil && len(eachKeys) > 0 {
			//执行该函数
			eachPageExcFunc(eachKeys)
		}

		totalKeys = append(totalKeys, eachKeys...)
		if 0 == eachCursor {
			// 游标为：0, 表示结束
			break
		}

		if pointCount > 0 && len(totalKeys) >= pointCount {
			// 获取了超过20条数据就返回
			break
		}
	}
	return eachCursor, totalKeys
}
