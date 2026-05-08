package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// 测试默认超时（请求在60秒内完成，不触发超时）
func TestDefaultTimeout_NotExceed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"ok"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, WithMethodGet())
	_, err := client.Do()

	if err != nil {
		t.Fatalf("预期成功，但返回错误: %v", err)
	}
}

// 测试自定义超时未触发（请求在超时时间内完成）
func TestCustomTimeout_NotExceed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"ok"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, WithMethodGet(), WithTimeout(2*time.Second))
	_, err := client.Do()

	if err != nil {
		t.Fatalf("预期成功，但返回错误: %v", err)
	}
}

// 测试自定义超时触发（请求超过设置的超时时间）
func TestCustomTimeout_Exceed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, WithMethodGet(), WithTimeout(2*time.Second))
	_, err := client.Do()

	if err == nil {
		t.Errorf("预期超时错误，但请求成功")
	} else {
		t.Logf("超时错误（符合预期）: %v", err)
	}
}
