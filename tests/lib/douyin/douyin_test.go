package douyin

import (
	"encoding/json"
	"github.com/fastwego/microapp"
	"github.com/fastwego/microapp/apis/auth"
	"github.com/gogf/gf/frame/g"
	"github.com/kdcer/go-lib/lib/douying"
	"net/url"
	"testing"
)

// 初始化 抖音小程序配置
func Test_douyin(t *testing.T) {
	douying.InitMicroapp(microapp.Config{
		AppId:     g.Config().GetString("douyin.appId"),
		AppSecret: g.Config().GetString("douyin.appSecret"),
	})
}

// exp 抖音code登陆 演示
func Test_douyin_login(t *testing.T) {
	douying.InitMicroapp(microapp.Config{
		AppId:     g.Config().GetString("douyin.appId"),
		AppSecret: g.Config().GetString("douyin.appSecret"),
	})

	var tk douying.ResCodeSession
	params := url.Values{}
	params.Add("code", "code")
	resp, _ := auth.Code2Session(douying.MicroappApp, params)
	json.Unmarshal(resp, &tk)
}
