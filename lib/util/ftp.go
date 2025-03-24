package util

//
//import (
//	"crypto/tls"
//	"github.com/gogf/gf/frame/g"
//	"github.com/gogf/gf/os/glog"
//	"github.com/jlaffaye/ftp"
//	"os"
//	"time"
//)
//
//var conn *ftp.ServerConn
//
//func initFtp() {
//	var err error
//	//获取配置文件
//	host := g.Cfg().GetString("ftp.host")
//	username := g.Cfg().GetString("ftp.username")
//	password := g.Cfg().GetString("ftp.password")
//	timeout := time.Duration(g.Cfg().GetInt("ftp.timeout"))
//	passive := g.Cfg().GetBool("ftp.passive")
//	isTLS := g.Cfg().GetBool("ftp.isTLS", true) //是否启动TLS, 默认true
//
//	options := []ftp.DialOption{
//		ftp.DialWithTimeout(timeout * time.Second),
//		ftp.DialWithDisabledEPSV(passive),
//	}
//	if isTLS {
//		// TLS client authentication
//		config := &tls.Config{
//			InsecureSkipVerify: true,
//			ClientAuth:         tls.RequestClientCert,
//		}
//		options = append(options, ftp.DialWithTLS(config))
//	}
//
//	conn, err = ftp.Dial(host, options...)
//
//	//conn, err = ftp.Dial(host, ftp.DialWithTimeout(timeout*time.Second), ftp.DialWithDisabledEPSV(passive))
//
//	if err != nil {
//		glog.File("file--{Ymd}.log").Error("ftp连接失败，error：%s", err.Error())
//		return
//	}
//
//	err = conn.Login(username, password)
//
//	if err != nil {
//		glog.File("file--{Ymd}.log").Error("ftp登录失败，error：%s", err.Error())
//		return
//	}
//}
//
////上传
////imagePath		本地文件路径
////staticPath	静态服务器地址
//func Upload(imagePath string, staticPath string) bool {
//	initFtp()
//	defer func() {
//		err := conn.Quit()
//		if err != nil {
//			glog.File("file--{Ymd}.log").Error("ftp关闭失败，error：%s", err.Error())
//		}
//	}()
//
//	f, err := os.Open(imagePath)
//	err = conn.Stor(staticPath, f)
//	if err != nil {
//		glog.File("file--{Ymd}.log").Error("ftp文件上传失败，error：%s", err.Error())
//		return false
//	}
//
//	return true
//}
//
////下载
////src	远端路径
////dist	本地路径
//func Download(src string, dist string) error {
//
//	return nil
//}
