package goftp

import (
	"fmt"
	"github.com/kdcer/go-lib/lib/util"
	"testing"
)

func Test_ip_01(t *testing.T) {
	ip, err := util.ExternalIP()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(ip.String())
}
