package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
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

// Client 客户端结构体
type Client struct {
	url         string
	method      string
	body        io.Reader
	headers     map[string]string
	contentType string
	debug       bool // 新增：控制是否打印请求详情
}

func NewClient(url string, opts ...func(*Client)) *Client {
	client := &Client{
		url: url,
	}

	for _, opt := range opts {
		opt(client)
	}
	return client
}

// WithQuery 添加URL查询参数 GET请求用
func WithQuery(param map[string]string) func(*Client) {
	return func(c *Client) {
		c.method = "GET"
		// 构建URL编码的查询参数
		var queryParams []string
		for k, v := range param {
			// URL编码键值对
			encoded := url.QueryEscape(k) + "=" + url.QueryEscape(v)
			queryParams = append(queryParams, encoded)
		}

		// 拼接URL参数
		if len(queryParams) > 0 {
			separator := "?"
			if strings.Contains(c.url, "?") {
				separator = "&"
			}
			c.url += separator + strings.Join(queryParams, "&")
		}
	}
}

// WithFormUrlEncoded 设置请求体 POST "application/x-www-form-urlencoded" 请求用
func WithFormUrlEncoded(param map[string]string) func(*Client) {
	return func(c *Client) {
		c.method = "POST"
		c.contentType = "application/x-www-form-urlencoded"
		formData := url.Values{}
		for k, v := range param {
			formData.Set(k, v)
		}
		c.body = strings.NewReader(formData.Encode())
	}
}

// WithBody 设置请求体 POST 请求用
func WithBody(body any) func(*Client) {
	return func(c *Client) {
		c.method = "POST"
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

func WithMethodPost() func(*Client) {
	return func(c *Client) {
		c.method = "POST"
	}
}

func WithMethodGet() func(*Client) {
	return func(c *Client) {
		c.method = "GET"
	}
}

func WithMethod(method string) func(*Client) {
	return func(c *Client) {
		c.method = method
	}
}

// WithDebug 开启调试模式，打印完整的请求信息
func WithDebug() func(*Client) {
	return func(c *Client) {
		c.debug = true
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

	if c.debug {
		// 使用 DumpRequest 打印 (true 表示同时打印 Body)
		dump, _ := httputil.DumpRequest(httpReq, true)
		fmt.Printf("--- Request Dump ---\n%s\n--------------------\n", string(dump))
	}

	// 使用共享客户端发送请求
	resp, err := sharedClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 新增响应体读取逻辑
	respBody, err = io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 {
		fmt.Printf("http.DO err: %v,url:%v,statusCode:%d\n", err, c.url, resp.StatusCode)
		return nil, fmt.Errorf("http.DO err: %v,resp.StatusCode:%d", err, resp.StatusCode)
	}

	fmt.Printf("http.Do info: url=%s,resp=%v\n", c.url, string(respBody))

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
