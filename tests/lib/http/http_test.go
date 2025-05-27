package copier

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/http"
	"github.com/codeedge/go-lib/lib/logs"
	"github.com/go-resty/resty/v2"
	"testing"
)

type OperationRegister struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data *struct {
		Id int `json:"id"`
	} `json:"data"`
	Ext interface{} `json:"ext"`
	Tp  int         `json:"tp"`
}

func Test_Http(t *testing.T) {
	// 注册到运营平台
	client := http.NewSmsClient("url", "POST", http.WithBody(map[string]any{
		"account": "1",
	}), http.WithHeader(map[string]string{
		"X-Custom-Header": "1",
	}))

	respBody, err := http.Do[OperationRegister](client)
	if err != nil {
		logs.Errorf("http.NewSmsClient err: %v", err)
		return
	}

	if respBody.Code > 0 || respBody.Data == nil {
		return
	}
}

// Go 每日一库之 resty https://segmentfault.com/a/1190000040247099
func Test_Resty(t *testing.T) {
	respBody := &OperationRegister{}
	cli := resty.New()
	rsp, err := cli.R().SetResult(respBody).SetHeaders(map[string]string{"X-Custom-Header": "1"}).SetBody(map[string]any{
		"account": "1",
	}).Post("url")
	if err != nil {
		logs.Errorf("resty.New err: %v", err)
		return
	}
	fmt.Println(rsp)
	if respBody.Code > 0 || respBody.Data == nil {
		return
	}
}
