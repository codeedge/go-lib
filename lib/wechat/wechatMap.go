package wechat

import (
	"github.com/gogf/gf/frame/g"
	"github.com/gogf/gf/os/gmlock"
	"github.com/silenceper/wechat/v2"
	"github.com/silenceper/wechat/v2/cache"
	"github.com/silenceper/wechat/v2/miniprogram"
	miniConfig "github.com/silenceper/wechat/v2/miniprogram/config"
	"github.com/silenceper/wechat/v2/officialaccount"
	offConfig "github.com/silenceper/wechat/v2/officialaccount/config"
	"github.com/silenceper/wechat/v2/openplatform"
	openConfig "github.com/silenceper/wechat/v2/openplatform/config"
	"github.com/silenceper/wechat/v2/work"
	workConfig "github.com/silenceper/wechat/v2/work/config"
)

var infoMap = make(map[string]*Info, 0)

func GetInfo(appId string, f func(appId string) interface{}) *Info {
	gmlock.Lock(appId)
	defer gmlock.Unlock(appId)
	if p, ok := infoMap[appId]; ok {
		return p
	} else {
		info, err := NewInfo(appId, f)
		if err != nil {
			panic(err)
		}
		infoMap[appId] = info
		return info
	}
}

type Info struct {
	wc              *wechat.Wechat
	OfficialAccount *officialaccount.OfficialAccount
	MiniProgram     *miniprogram.MiniProgram
	OpenPlatform    *openplatform.OpenPlatform
	Work            *work.Work
	redisCache      *cache.Redis
}

type WechatInfos struct {
	Id             int64  `json:"id"             ` //
	Name           string `json:"name"           ` // 公众号名
	AppId          string `json:"appId"          ` //
	AppSecret      string `json:"appSecret"      ` //
	Scope          string `json:"scope"          ` //
	Token          string `json:"token"          ` //
	EncodingAesKey string `json:"encodingAesKey" ` //
}

func NewInfo(appId string, f func(appId string) interface{}) (info *Info, err error) {
	wechatInfo := f(appId).(*WechatInfos)
	info = &Info{}
	info.InitWechat(&cache.RedisOpts{
		Host:        g.Config().GetString("redis.addr"),
		Password:    g.Config().GetString("redis.password"),
		Database:    0,
		MaxActive:   2000,
		MaxIdle:     10,
		IdleTimeout: 300,
	})
	info.InitOfficialAccount(&offConfig.Config{
		AppID:          wechatInfo.AppId,
		AppSecret:      wechatInfo.AppSecret,
		Token:          wechatInfo.Token,
		EncodingAESKey: wechatInfo.EncodingAesKey,
	})
	info.InitMiniProgram(&miniConfig.Config{
		AppID:     wechatInfo.AppId,
		AppSecret: wechatInfo.AppSecret,
	})
	info.InitOpenPlatform(&openConfig.Config{
		AppID:          wechatInfo.AppId,
		AppSecret:      wechatInfo.AppSecret,
		Token:          wechatInfo.Token,
		EncodingAESKey: wechatInfo.EncodingAesKey,
	})
	return info, nil
}

// InitWechat 获取wechat实例
// 在这里已经设置了全局cache，则在具体获取公众号/小程序等操作实例之后无需再设置，设置即覆盖
func (i *Info) InitWechat(opts *cache.RedisOpts) {
	i.wc = wechat.NewWechat()
	i.redisCache = cache.NewRedis(opts)
	i.wc.SetCache(i.redisCache)
}

// InitOfficialAccount 获取微信公众号实例
func (i *Info) InitOfficialAccount(cfg *offConfig.Config) {
	i.OfficialAccount = i.wc.GetOfficialAccount(cfg)
}

// InitMiniProgram 获取小程序的实例
func (i *Info) InitMiniProgram(cfg *miniConfig.Config) {
	i.MiniProgram = i.wc.GetMiniProgram(cfg)
}

// InitOpenPlatform 获取微信开放平台的实例
func (i *Info) InitOpenPlatform(cfg *openConfig.Config) {
	if cfg.Cache == nil {
		cfg.Cache = redisCache
	}
	i.OpenPlatform = i.wc.GetOpenPlatform(cfg)
}

// InitWork 获取企业微信的实例
func (i *Info) InitWork(cfg *workConfig.Config) {
	i.Work = i.wc.GetWork(cfg)
}
