package wechat

import (
	"github.com/silenceper/wechat/v2"
	"github.com/silenceper/wechat/v2/cache"
	"github.com/silenceper/wechat/v2/miniprogram"
	miniConfig "github.com/silenceper/wechat/v2/miniprogram/config"
	"github.com/silenceper/wechat/v2/officialaccount"
	offConfig "github.com/silenceper/wechat/v2/officialaccount/config"
	"github.com/silenceper/wechat/v2/openplatform"
	openConfig "github.com/silenceper/wechat/v2/openplatform/config"
	"github.com/silenceper/wechat/v2/pay"
	payConfig "github.com/silenceper/wechat/v2/pay/config"
	"github.com/silenceper/wechat/v2/work"
	workConfig "github.com/silenceper/wechat/v2/work/config"
)

var wc *wechat.Wechat
var OfficialAccount *officialaccount.OfficialAccount
var MiniProgram *miniprogram.MiniProgram
var Pay *pay.Pay
var OpenPlatform *openplatform.OpenPlatform
var Work *work.Work
var redisCache *cache.Redis

// InitWechat 获取wechat实例
// 在这里已经设置了全局cache，则在具体获取公众号/小程序等操作实例之后无需再设置，设置即覆盖
func InitWechat(opts *cache.RedisOpts) {
	wc = wechat.NewWechat()
	redisCache = cache.NewRedis(opts)
	wc.SetCache(redisCache)
}

// InitOfficialAccount 获取微信公众号实例
func InitOfficialAccount(cfg *offConfig.Config) {
	OfficialAccount = wc.GetOfficialAccount(cfg)
}

// InitMiniProgram 获取小程序的实例
func InitMiniProgram(cfg *miniConfig.Config) {
	MiniProgram = wc.GetMiniProgram(cfg)
}

// InitPay 获取微信支付的实例
func InitPay(cfg *payConfig.Config) {
	Pay = wc.GetPay(cfg)
}

// InitOpenPlatform 获取微信开放平台的实例
func InitOpenPlatform(cfg *openConfig.Config) {
	if cfg.Cache == nil {
		cfg.Cache = redisCache
	}
	OpenPlatform = wc.GetOpenPlatform(cfg)
}

// InitWork 获取企业微信的实例
func InitWork(cfg *workConfig.Config) {
	Work = wc.GetWork(cfg)
}
