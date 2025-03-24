package apiSign

import (
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/gogf/guuid"

	"github.com/gogf/gf/net/ghttp"

	"github.com/kdcer/go-lib/lib/alioss"

	"github.com/gogf/gf/frame/g"
)

func Test_OSS(t *testing.T) {
	alioss.New(&alioss.OssConfig{
		Endpoint:        g.Config().GetString("oss.endpoint"),
		AccessKeyId:     g.Config().GetString("oss.accessKeyId"),
		AccessKeySecret: g.Config().GetString("oss.accessKeySecret"),
		BucketName:      g.Config().GetString("oss.bucketName"),
	})
	var r *ghttp.Request
	file := r.GetUploadFile("file")
	ext := path.Ext(file.Filename)
	fileName := guuid.New().String() + ext
	objectKey := fmt.Sprintf("%s/file/%s/%s", g.Config().GetString("oss.prefix"), time.Now().Format("2006-01"), fileName)
	f, err := file.Open()
	if err != nil {
		t.Fatal(err)
	}
	var extList = []string{".doc", ".docx", ".mp4", ".mov", ".mkv", ".wmv", ".avi", ".jpg"}
	tag := false
	for _, v := range extList {
		if strings.ToLower(ext) == strings.ToLower(v) {
			tag = true
			break
		}
	}
	if !tag {
		t.Fatal("仅支持doc、docx、mp4、mov、mkv、wmv和avi格式")
	}
	url, err := alioss.OssClient.Put(objectKey, f, fileName)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(url)
	// 设置超时链接测试,当文件或者bucket的ACL权限设置为私有的使用可以通过签名设置临时访问权限，如果ACL是公共读权限则没有意义，直接就可以访问。
	url2, err := alioss.OssClient.Bucket.SignURL(objectKey, oss.HTTPGet, 30)
	t.Log(url2, err)
	// 上传同时设置为私有
	options := []oss.Option{
		oss.ObjectACL(oss.ACLPrivate),
	}
	url, err = alioss.OssClient.Put(objectKey, f, fileName, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(url, err)
}
