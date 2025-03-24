package util

import (
	"crypto/tls"
	"github.com/gogf/gf/frame/g"
	"github.com/gogf/gf/os/glog"
	"gopkg.in/dutchcoders/goftp.v1"
	"os"
	"strings"
)

func initFtp() (ftp *goftp.FTP, err error) {

	//获取配置文件
	host := g.Cfg().GetString("ftp.host")
	username := g.Cfg().GetString("ftp.username")
	password := g.Cfg().GetString("ftp.password")
	//timeout := time.Duration(g.Cfg().GetInt("ftp.timeout"))
	//passive := g.Cfg().GetBool("ftp.passive")
	isTLS := g.Cfg().GetBool("ftp.isTLS", true) //是否启动TLS, 默认true

	if ftp, err = goftp.Connect(host); err != nil {
		glog.File("file--{Ymd}.log").Error("ftp连接失败，error：%s", err.Error())
		return
	}

	if isTLS {
		// TLS client authentication
		config := &tls.Config{
			InsecureSkipVerify: true,
			ClientAuth:         tls.RequestClientCert,
		}

		if err = ftp.AuthTLS(config); err != nil {
			glog.File("file--{Ymd}.log").Error("ftp AuthTLS失败，error：%s", err.Error())
			return
		}
	}

	if err = ftp.Login(username, password); err != nil {
		glog.File("file--{Ymd}.log").Error("ftp登陆失败，error：%s", err.Error())
		return
	}

	return ftp, err
}

// 上传
// imagePath		本地文件路径
// staticPath	静态服务器地址
func Upload(imagePath string, staticPath string) bool {
	ftp, err := initFtp()
	if err != nil {
		glog.Errorf("初始化Ftp失败，error：%s", err.Error())
		return false
	}

	f, err := os.Open(imagePath)

	defer func() {
		_ = f.Close()
		closeFtp(ftp)
	}()

	if err != nil {
		glog.Errorf("os.Open imagePath=%s，error：%s", imagePath, err.Error())
		return false
	}

	MkDir(staticPath)
	err = ftp.Stor(staticPath, f)

	if err != nil {
		glog.File("file--{Ymd}.log").Error("ftp文件上传失败，error：%s", err.Error())
		return false
	}

	return true
}

// 创建文件夹
func MkDir(staticPath string) bool {
	ftp, err := initFtp()
	defer func() {
		closeFtp(ftp)
	}()
	if err != nil {
		glog.Errorf("初始化Ftp失败，error：%s", err.Error())
		return false
	}

	_staticPath := staticPath[0:strings.LastIndex(staticPath, "/")]
	_staticPaths := strings.Split(_staticPath, "/")
	_createPath := ""
	for i := 0; i < len(_staticPaths); i++ {
		_createPath += _staticPaths[i]

		if !Exist(_createPath) {
			err := ftp.Mkd(_createPath + "/")
			if err != nil && err.Error() != "550 Can't create directory: No such file or directory\r\n" {
				glog.Errorf("创建远端目录失败_createPath=[%s], error：%s", _createPath, err.Error())
				return false
			}
		} else {
			//glog.Info("存在对应的文件夹 path=", _createPath)
		}

		if i < len(_staticPaths)-1 {
			_createPath += "/"
		}
	}
	return true
}

// 下载
// src	远端路径
// dist	本地路径
func Download(src string, dist string) error {

	return nil
}

// 判断目录是否存在,不存在返回false
func Exist(path string) bool {
	ftp, err := initFtp()
	defer func() {
		closeFtp(ftp)
	}()
	if err != nil {
		glog.Errorf("初始化Ftp失败，error：%s", err.Error())
		return false
	}
	err1 := ftp.Cwd(path)
	//文件不存在
	if err1 != nil {
		return false
	} else {
		return true
	}
}

// 关闭连接
func closeFtp(conn *goftp.FTP) {
	err := conn.Quit()
	if err != nil {
		glog.Errorf("ftp关闭失败，error：%s", err.Error())
	}
}
