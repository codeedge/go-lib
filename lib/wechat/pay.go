package wechat

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/silenceper/wechat/v2/util"

	"github.com/gogf/gf/os/glog"
	"golang.org/x/crypto/pkcs12"
)

var _tlsConfig *tls.Config
var (
	wechatURL           = "https://api.mch.weixin.qq.com/mmpaymkttransfers/sendredpack" //请求发红包的地址
	formatDate          = "20060102"
	xmlStr              = "xml"
	redPacketRequestStr = "RedPacketRequest"
)

type PayService struct {
	*RedPacketConfig
}

type RedPacketConfig struct {
	WeixinPayKey            string // 微信密钥
	WeixinMchID             string // 微信商户号
	WeixinAppID             string // 公众账号ID
	WeixinClientCertPemPath string // 客户端证书存放绝对路径
	WeixinClientKeyPemPath  string // 客户端私匙存放绝对路径
	WeixinRootCaPath        string // 服务端证书存放绝对路径
	ClientIP                string // 调用接口的机器ip地址
}

// RedPacketRequest 发红包的请求实体
type RedPacketRequest struct {
	ActName     string `xml:"act_name"`     //必填，活动名称
	ClientIP    string `xml:"client_ip"`    //必填，调用接口的机器ip地址
	MchBillno   string `xml:"mch_billno"`   //必填，商户订单号
	MchID       string `xml:"mch_id"`       //必填，微信支付分配的商户号
	NonceStr    string `xml:"nonce_str"`    //必填,随机字符串，不超过32位
	ReOpenid    string `xml:"re_openid"`    //必填，接收红包者用户，用户在wxappid下的openid
	Remark      string `xml:"remark"`       //必填，备注信息
	SendName    string `xml:"send_name"`    //必填，红包发送者名称
	TotalAmount int    `xml:"total_amount"` //必填，付款金额，单位为分
	TotalNum    int    `xml:"total_num"`    //必填，红包发放人数
	Wishing     string `xml:"wishing"`      //必填，红包祝福语
	Wxappid     string `xml:"wxappid"`      //必填，微信公众号id
	Sign        string `xml:"sign"`         //必填，签名
	SceneID     string `xml:"scene_id"`     //非必填，红包使用场景 红包金额大于200或者小于1元时，请求参数scene_id必传，参数说明见下文。  https://pay.weixin.qq.com/wiki/doc/api/tools/cash_coupon.php?chapter=13_4&index=3
	//RiskInfo 	string		`xml:"risk_info"`    //非必填，用户操作的时间戳
	//ConsumeMchId string	`xml:"consume_mch_id"` //非必填，资金授权商户号
}

// Response 接口返回
type Response struct {
	ReturnCode  string `xml:"return_code"`
	ReturnMsg   string `xml:"return_msg"`
	ResultCode  string `xml:"result_code,omitempty"`
	ErrCode     string `xml:"err_code,omitempty"`
	ErrCodeDes  string `xml:"err_code_des,omitempty"`
	AppID       string `xml:"wxappid,omitempty"`
	MchBillno   string `xml:"mch_billno,omitempty"`
	MchID       string `xml:"mch_id,omitempty"`
	ReOpenid    string `xml:"re_openid,omitempty"`
	TotalAmount string `xml:"total_amount,omitempty"`
	SendListid  string `xml:"send_listid,omitempty"`
}

var payService *PayService

func InitPayService(cfg *RedPacketConfig) {
	payService = &PayService{
		RedPacketConfig: cfg,
	}
}

func GetPayService() *PayService {
	return payService
}

// SendRedPack 发微信红包
func (srv *PayService) SendRedPack(redPacketEntity *RedPacketRequest) (rsp *Response, err error) {
	nonceStr := util.RandomStr(32) //随机字符串

	//订单号,随机生成
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	mchBillno := srv.WeixinMchID + time.Now().Format(formatDate) + strconv.FormatInt(time.Now().Unix(), 10)[4:] + strconv.Itoa(r.Intn(8999)+1000)
	redPacketEntity.ClientIP = srv.ClientIP
	redPacketEntity.NonceStr = nonceStr
	redPacketEntity.MchBillno = mchBillno
	redPacketEntity.TotalNum = 1
	redPacketEntity.MchID = srv.WeixinMchID
	redPacketEntity.Wxappid = srv.WeixinAppID
	// 生成签名
	sign := strings.ToUpper(srv.signature(*redPacketEntity)) //签名
	redPacketEntity.Sign = sign
	data, err := xml.MarshalIndent(redPacketEntity, "", "   ")
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	sendData := strings.Replace(string(data), redPacketRequestStr, xmlStr, -1)
	res, err := srv.securePost(wechatURL, []byte(sendData))
	if err != nil {
		glog.Error(err)
		return
	}
	err = xml.Unmarshal(res, &rsp)
	if err != nil {
		glog.Error(err)
		return
	}
	fmt.Println(rsp)
	return rsp, err
}

// http发送请求
func (srv *PayService) securePost(url string, xmlContent []byte) ([]byte, error) {
	tlsConfig, err := srv.getTLSConfig()
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: tr}
	resp, err := client.Post(url, "text/xml", bytes.NewBuffer(xmlContent))
	if err != nil {
		fmt.Println(err)
		glog.Error(err)
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

// 加载微信发红包需要的证书
func (srv *PayService) getTLSConfig() (*tls.Config, error) {
	if _tlsConfig != nil {
		return _tlsConfig, nil
	}
	// load cert
	cert, err := tls.LoadX509KeyPair(srv.WeixinClientCertPemPath, srv.WeixinClientKeyPemPath)
	if err != nil {
		fmt.Println("load wechat keys fail", err)
		return nil, err
	}
	// load root ca
	caData, err := ioutil.ReadFile(srv.WeixinRootCaPath)
	if err != nil {
		fmt.Println("read wechat ca fail", err)
		return nil, err
	}
	blocks, _ := pkcs12.ToPEM(caData, "1229445702")
	var pemData []byte
	for _, block := range blocks {
		pemData = append(pemData, pem.EncodeToMemory(block)...)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pemData)

	// pair, _ := tls.X509KeyPair(pemData, pemData)

	_tlsConfig = &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
	}
	return _tlsConfig, nil
}

// 签名算法
func (srv *PayService) signature(sendParamEntity interface{}) string {
	str := getFieldString(sendParamEntity)
	if str != "" {
		str = fmt.Sprintf("%s&%s=%s", str, "key", srv.WeixinPayKey)
		fmt.Println(str)
		md5Ctx2 := md5.New()
		md5Ctx2.Write([]byte(str))
		str = hex.EncodeToString(md5Ctx2.Sum(nil))
		return str
	}
	return ""
}

// 获取结构体字段及值的拼接值
func getFieldString(sendParamEntity interface{}) string {
	m := reflect.TypeOf(sendParamEntity)
	v := reflect.ValueOf(sendParamEntity)
	var tagName string
	numField := m.NumField()
	w := make([]string, numField)
	numFieldCount := 0
	for i := 0; i < numField; i++ {
		fieldName := m.Field(i).Name
		tags := strings.Split(string(m.Field(i).Tag), "\"")
		if len(tags) > 1 {
			tagName = tags[1]
		} else {
			tagName = m.Field(i).Name
		}

		fieldValue := v.FieldByName(fieldName).Interface()

		if fieldValue != "" {
			s := fmt.Sprintf("%s=%v", tagName, fieldValue)
			w[numFieldCount] = s
			numFieldCount++
		}
	}
	if numFieldCount == 0 {
		return ""
	}
	w = w[:numFieldCount]
	sort.Strings(w)
	return strings.Join(w, "&")
}
