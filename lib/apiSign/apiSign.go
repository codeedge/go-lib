package apiSign

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/gogf/gf/v2/crypto/gmd5"
	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
)

// SignParams 接口签名参数
// 高级权限可以设置盐值配置到数据库同步到缓存，每个app平台的不同版本号对应不同的盐值，根据版本号设置不同的盐值，可以控制失效某个版本的app
type SignParams struct {
	NormalSign      string   // 参数标准签名盐值
	WebSign         string   // 参数Web签名盐值   web易破解 设置独立盐值
	IgnoreFilterUrl []string // 签名拦截忽略路径, 相对完整路径(不支持*)
	IgnoreParams    []string // 忽略不参与签名的公共参数
	Whitelist       []string // 白名单 多个逗号隔开
	MasterKey       string   // 万能密钥 不为空才有效 直接跳过验证
}

type appPlatform = string // app平台

const (
	android appPlatform = "1"
	iOS     appPlatform = "2"
	web     appPlatform = "3"
	applet  appPlatform = "4"
)

var signErr = errors.New("签名错误")

// APISign 接口签名
// 加密方式：MD5(盐值+|+排序普通参数[k=v&]+然后添加时间戳[k=v]+|+盐值) kv使用=和&连接，时间戳后面的&不用，盐值和参数之间用|隔开。
// 比如：salt|appChannelId=10012&appProductCode=video_code&id=1&siteType=10003&unixTime=1583550103|salt
// 设置开关-秘钥-参数排序-时间戳-时间是否过期-时区设置--web项目独立盐-上传等接口部分参数忽略-忽略部分接口
// header中有 1,appInfo 格式{appCode}-{appPlatform}-{versionNumber} 说明：(appCode：项目编号,appType:平台 1-Android、2-iOS、3-web、4-小程序,versionNumber：版本号),目前版本只验证appType获取不同的盐值，不对平台和版本号进行处理，例：vp-4-1.0.1 (视频项目小程序1.0.1版本)
//
//	2,unixTime Unix时间戳
//	3,sign 客户端加密结果
func APISign(r *ghttp.Request, signParams *SignParams) (err error) {
	// 静态页面不拦截
	if r.IsFileRequest() {
		return nil
	}
	// 跳过白名单
	if len(signParams.Whitelist) > 0 {
		for _, v := range signParams.Whitelist {
			if v == r.GetClientIp() {
				return nil
			}
		}
	}
	// 忽略路径直接跳过
	for _, signIgnorePath := range signParams.IgnoreFilterUrl {
		if signIgnorePath == r.URL.Path || strings.HasPrefix(r.URL.Path, signIgnorePath) {
			fmt.Println("忽略sign拦截链接url=", r.URL.String())
			return nil
		}
	}
	unixTime := r.Header.Get("unixTime")
	sign := r.Header.Get("sign")
	// 万能密钥 不为空才有效 直接跳过验证
	if len(signParams.MasterKey) > 0 && sign == signParams.MasterKey {
		return nil
	}
	appInfo := r.Header.Get("appInfo")
	if unixTime == "" || sign == "" || appInfo == "" {
		return signErr
	}
	appInfoSlice := strings.Split(appInfo, "-")
	if len(appInfoSlice) < 3 {
		return signErr
	}
	platform := appInfoSlice[1]
	salt := signParams.NormalSign
	// 如果是web和小程序 使用web独立的盐
	if platform == web || platform == applet {
		salt = signParams.WebSign
	}
	mp := r.GetRequestMap()

	// 移除忽略参数
	for _, v := range signParams.IgnoreParams {
		delete(mp, v)
	}

	keys := make([]string, 0)
	for k := range mp {
		keys = append(keys, k)
	}
	//fmt.Println(keys)
	// 排序
	sort.Strings(keys)
	//fmt.Println(keys)
	// 拼接加密字符串
	kvString := bytes.NewBufferString(salt)
	kvString.WriteString("|")
	for _, v := range keys {
		kvString.WriteString(v)
		kvString.WriteString("=")
		kvString.WriteString(gconv.String(mp[v]))
		kvString.WriteString("&")
	}
	// 参数之后添加时间
	kvString.WriteString("unixTime")
	kvString.WriteString("=")
	kvString.WriteString(unixTime)
	// 最后添加盐
	kvString.WriteString("|")
	kvString.WriteString(salt)
	//fmt.Println(kvString.String())
	//fmt.Println(gurl.Decode(kvString.String()))// gf会自动decode 不用重复处理
	// 生成服务器sign
	signServer, err := gmd5.Encrypt(kvString.String())
	if err != nil {
		fmt.Println("参数签名 gmd5.Encrypt 错误", err)
		return signErr
	}
	//fmt.Printlnf("服务器签名：%s", signServer)
	// 签名不一致 直接返回
	if signServer != sign {
		fmt.Sprintf("签名错误：MD5原始字符串=%s,服务端签名结果=%s,客户端签名=%s,appInfo=%s", kvString.String(), signServer, sign, appInfo)
		return signErr
	}
	// 判断时间
	interval := gtime.Timestamp() - gconv.Int64(unixTime)
	// 时间取绝对值 和服务器时间偏移60秒以上不合法
	interval = int64(math.Abs(float64(interval)))
	if interval > 60 {
		fmt.Sprintf("签名超时：MD5原始字符串=%s,服务端签名结果=%s,客户端签名=%s,时间差=%d,appInfo=%s", kvString.String(), signServer, sign, interval, appInfo)
		return signErr
	}
	return nil
}
