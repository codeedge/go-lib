package casbin

import (
	"fmt"
	"testing"

	"github.com/kdcer/go-lib/lib/casbin"
)

func Test_Casbin(t *testing.T) {
	casbin.Init("mysql", "root:123456@tcp(127.0.0.1:3306)/goblog?charset=utf8", "./rbac_models.conf")

	casbin.Enforcer().ClearPolicy()
	casbin.Enforcer().SavePolicy()
	casbin.Enforcer().AddPolicy("nothing", "domain1", "/index", "get")
	list := casbin.Enforcer().GetPolicy()
	fmt.Println(list)
}
