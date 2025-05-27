package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SharedClient 新增共享HTTP客户端（提升性能）
var sharedClient = &http.Client{
	Timeout: 60 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:    100,
		IdleConnTimeout: 90 * time.Second,
	},
}

// Client 短信客户端结构体
type Client struct {
	url         string
	method      string
	body        io.Reader
	headers     map[string]string
	contentType string
}

func NewSmsClient(url, method string, opts ...func(*Client)) *Client {
	client := &Client{
		url:    url,
		method: method,
	}

	for _, opt := range opts {
		opt(client)
	}
	return client
}

func WithBody(body any) func(*Client) {
	return func(c *Client) {
		if r, ok := body.(io.Reader); ok {
			c.body = r
		} else {
			bt, _ := json.Marshal(body)
			c.body = bytes.NewReader(bt)
		}
	}
}

func WithHeader(headers map[string]string) func(*Client) {
	return func(c *Client) {
		c.headers = headers
	}
}

func WithContentType(contentType string) func(*Client) {
	return func(c *Client) {
		c.contentType = contentType
	}
}

func (c *Client) Do() (respBody []byte, err error) {
	httpReq, err := http.NewRequest(c.method, c.url, c.body)
	if err != nil {
		fmt.Printf("http.NewRequest err: %v\n", err)
		return nil, err
	}
	header := make(http.Header)
	contentType := "application/json"
	if len(c.contentType) > 0 {
		contentType = c.contentType
	}
	header.Set("Content-Type", contentType)
	if len(c.headers) > 0 {
		// 构造请求头
		for k, v := range c.headers {
			header.Set(k, v)
		}
	}
	httpReq.Header = header

	// 使用共享客户端发送请求
	resp, err := sharedClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 新增响应体读取逻辑
	respBody, err = io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 {
		fmt.Printf("http.DO err: %v,c:%v,,statusCode:%d\n", err, c, resp.StatusCode)
		return nil, fmt.Errorf("http.DO err: %v", err)
	}

	return respBody, nil
}

func Do[T any](c *Client) (*T, error) {
	res, err := c.Do()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New("no data")
	}
	var t T
	if err := json.Unmarshal(res, &t); err != nil {
		return nil, err
	}
	return &t, nil
}
