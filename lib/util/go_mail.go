package util

import (
	"crypto/tls"
	"encoding/json"
	"github.com/go-gomail/gomail"
	"github.com/gogf/gf/os/glog"
	"net/smtp"
)

// 邮箱配置
type Email struct {
	Auth     smtp.Auth
	Identity string `json:"identity"`
	Username string `json:"username"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	From     string `json:"from"`
	To       string
	Subject  string
	HTML     string // Html message (optional)
}

func NewEMail(config string) *Email {
	e := new(Email)
	err := json.Unmarshal([]byte(config), e)
	if err != nil {
		return nil
	}
	return e
}

func (e *Email) Send() error {
	m := gomail.NewMessage()
	m.SetAddressHeader("From", e.From, "")
	m.SetAddressHeader("To", e.To, "")
	m.SetHeader("Subject", e.Subject)
	m.SetBody("text/html", e.HTML)

	d := gomail.NewDialer(e.Host, e.Port, e.From, e.Password)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	err := d.DialAndSend(m)
	if err != nil {
		glog.Errorf("***%s\n", err.Error())
	}
	return err
}
