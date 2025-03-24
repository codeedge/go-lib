package douying

import (
	"github.com/fastwego/microapp"
)

type CommonError struct {
	Errcode int    `json:"errcode"`
	Errmsg  string `json:"errmsg"`
	Error   int    `json:"error"`
	Message string `json:"message"`
}

type CodeSession struct {
	SessionKey      string `json:"session_key"`
	OpenID          string `json:"openid"`
	AnonymousOpenid string `json:"anonymous_openid"`
	Unionid         string `json:"unionid"`
}

type ResCodeSession struct {
	CommonError
	CodeSession
}

var MicroappApp *microapp.MicroApp

func InitMicroapp(conf microapp.Config) {
	MicroappApp = microapp.New(conf)
}
