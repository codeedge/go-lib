package apiSign

import (
	"testing"

	"github.com/gogf/gf/net/ghttp"

	"github.com/gogf/gf/frame/g"
	"github.com/kdcer/go-lib/lib/apiSign"
)

func Test_Sign(t *testing.T) {
	var signIgnoreFilterUrl []string //签名拦截忽略路径, 相对完整路径(不支持*)
	var r *ghttp.Request
	err := apiSign.APISign(r, &apiSign.SignParams{
		NormalSign:      "constants.ParamsNormalSign",
		WebSign:         "constants.ParamsWebSign",
		IgnoreFilterUrl: signIgnoreFilterUrl,
		IgnoreParams:    []string{"_start_time"},
		MasterKey:       g.Cfg().GetString("sign.masterKey"),
	})
	//if err != nil {
	//	response.Json(r, response.SignFAIL, err.Error())
	//}
	//r.Middleware.Next()
	t.Log(err)
}
