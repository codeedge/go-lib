package goftp_test

import (
	"crypto/tls"
	"fmt"
	"gopkg.in/dutchcoders/goftp.v1"
	"os"
	"testing"
)

func Test_util_ftp_1(t *testing.T) {
	var err error
	var ftp *goftp.FTP

	//192.168.2.196
	//	//admin
	//	//admin888
	// For debug messages: goftp.ConnectDbg("ftp.server.com:21")
	//if ftp, err = goftp.Connect("125.65.44.181:21"); err != nil {
	if ftp, err = goftp.Connect("192.168.2.196:21"); err != nil {
		panic(err)
	}

	defer ftp.Close()
	//fmt.Println("Successfully connected to", server)

	// TLS client authentication
	config := &tls.Config{
		InsecureSkipVerify: true,
		ClientAuth:         tls.RequestClientCert,
	}

	if err = ftp.AuthTLS(config); err != nil {
		panic(err)
	}

	// Username / password authentication
	//if err = ftp.Login("static", "4Sn7W7KN"); err != nil {
	if err = ftp.Login("admin", "admin888"); err != nil {
		panic(err)
	}

	if err = ftp.Cwd("/"); err != nil {
		panic(err)
	}

	var curpath string
	if curpath, err = ftp.Pwd(); err != nil {
		panic(err)
	}

	fmt.Printf("Current path: %s", curpath)

	// Get directory listing
	var files []string
	if files, err = ftp.List(""); err != nil {
		panic(err)
	}
	fmt.Println("Directory listing:", files)

	// Upload a file
	var file *os.File
	//if file, err = os.Open("D:/路飞.jpg"); err != nil {
	if file, err = os.Open("D:/黑暗物质.His.Dark.Materials.S01E01.中英字幕.WEBrip.720P-人人影视.mp4"); err != nil {
		panic(err)
	}

	//if err := ftp.Stor("路飞.jpg", file); err != nil {
	if err := ftp.Stor("黑暗物质.His.Dark.Materials.S01E01.中英字幕.WEBrip.720P-人人影视.mp4", file); err != nil {
		panic(err)
	}

	//// Download each file into local memory, and calculate it's sha256 hash
	//err = ftp.Walk("/", func(path string, info os.FileMode, err error) error {
	//	_, err = ftp.Retr(path, func(r io.Reader) error {
	//		var hasher = sha256.New()
	//		if _, err = io.Copy(hasher, r); err != nil {
	//			return err
	//		}
	//
	//		hash := fmt.Sprintf("%s %x", path, hex.EncodeToString(hasher.Sum(nil)))
	//		fmt.Println(hash)
	//
	//		return err
	//	})
	//
	//	return nil
	//})
}
