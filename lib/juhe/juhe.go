package juhe

import (
	"encoding/json"
	"fmt"
	"github.com/gogf/gf/frame/g"
	"net/url"
)

/*
聚合数据 官方文档 https://www.juhe.cn/docs
*/

// 定义api接口地址
const (
	SimpleWeather = "http://apis.juhe.cn/simpleWeather/query" //天气预报
	Laohuangli    = "http://v.juhe.cn/laohuangli/d"           //老黄历
	Soup          = "https://apis.juhe.cn/fapig/soup/query"   //每日心灵鸡汤语录
)

type Common struct {
	Reason    string      `json:"reason"`
	ErrorCode int         `json:"error_code"`
	Result    interface{} `json:"result"`
}

// SW 天气预报 要查询的城市名称/id，城市名称如：温州、上海、北京，需要utf8 urlencode
func SW(city string) *Common {
	c := &Common{}
	key := "b015c18feee82f444514c566cac3c90b"
	res, _ := g.Client().Get(fmt.Sprintf(SimpleWeather+"?city=%s&key=%s", url.QueryEscape(city), key))
	data := res.ReadAll()
	json.Unmarshal(data, &c)
	return c
}

// LHL 老黄历 日期，格式2014-09-09
func LHL(date string) *Common {
	c := &Common{}
	key := "c2845e9107a255ba9a0ed95fb48cbd0d"
	url := fmt.Sprintf(Laohuangli+"?date=%s&key=%s", url.QueryEscape(date), key)
	res, _ := g.Client().Get(url)
	data := res.ReadAll()
	json.Unmarshal(data, &c)
	return c
}

// So 每日心灵鸡汤语录
func So() *Common {
	c := &Common{}
	key := "e5796441e442b3b83e9ed2044c2ac6c0"
	url := fmt.Sprintf(Soup+"?key=%s", key)
	res, _ := g.Client().Get(url)
	data := res.ReadAll()
	json.Unmarshal(data, &c)
	return c
}
