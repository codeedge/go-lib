package casbin

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/database"
	"github.com/codeedge/go-lib/lib/logs"
	"testing"

	"github.com/codeedge/go-lib/lib/casbin"
)

func Test_Casbin(t *testing.T) {
	logs.NewFgLogger("", "", 10, 3, "info")
	// 连接MySQL
	db, err := database.Init(false, "root:123456@tcp(127.0.0.1:13306)/test?charset=utf8", nil)
	if err != nil {
		panic("数据库连接失败：" + err.Error())
	}

	casbin.Init(&casbin.Config{
		Key:           "",
		Path:          "./rbac_models.conf",
		DB:            db,
		RedisAddr:     "127.0.0.1:6379",
		RedisPassword: "123456",
	})

	//casbin.Init(&casbin.Config{
	//	Key:           "app",
	//	Path:          "./rbac_models.conf",
	//	DB:            db,
	//	RedisAddr:     "127.0.0.1:6379",
	//	RedisPassword: "123456",
	//	Enforcer:      nil,
	//})

	//从DB加载策略，上面这种参数会在执行casbin.NewEnforcer时自动调用，无需重复调用，但是封装后每次使用前需要调用一次，否则无法加载最新的数据
	e := casbin.Enforcer()
	e.LoadPolicy()

	// 添加一个p行，重复添加返回false,nil
	// p	admin	data1	read
	if ok, _ := e.AddPolicy("admin", "data1", "read"); !ok {
		fmt.Println("Policy已经存在")
	} else {
		fmt.Println("增加成功")
	}

	if ok, _ := e.UpdatePolicy([]string{"admin", "data1", "read"}, []string{"admin", "data1", "write"}); !ok {
		fmt.Println("Policy不存在")
	} else {
		fmt.Println("修改成功")
	}

	if ok, _ := e.RemovePolicy("admin", "data1", "read"); !ok {
		fmt.Println("Policy不存在")
	} else {
		fmt.Println("删除成功")
	}

	list, err := e.GetPolicy()
	if err != nil {
		panic(err)
	}
	for _, vlist := range list {
		for _, v := range vlist {
			fmt.Printf("value: %s, ", v)
		}
	}
	fmt.Println()
	res, err := e.GetFilteredPolicy(0, "admin") // 指定参数过滤，第一个参数对应的是字段v0
	if err != nil {
		panic(err)
	}
	fmt.Println(res)

	//判断策略中是否存在
	if ok, _ := e.Enforce("admin", "data1", "write"); ok {
		fmt.Println("恭喜您,权限验证通过")
	} else {
		fmt.Println("很遗憾,权限验证没有通过")
	}

	// 添加一个g行，重复添加返回false,nil
	// g	admin	data2_admin
	added, err := e.AddGroupingPolicy("admin", "data2_admin")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(added)
	// 批量添加g行
	// g	admin	data3_admin
	// g	admin2	data2_admin
	e.AddGroupingPolicies([][]string{
		[]string{
			"admin",
			"data3_admin",
		},
		[]string{
			"admin2",
			"data2_admin",
		},
	})

	if ok, _ := e.AddPolicy("data3_admin", "data2", "read"); !ok {
		fmt.Println("Policy已经存在")
	} else {
		fmt.Println("增加成功")
	}

	//判断策略中是否存在
	if ok, _ := e.Enforce("admin", "data2", "read"); ok {
		fmt.Println("恭喜您,权限验证通过")
	} else {
		fmt.Println("很遗憾,权限验证没有通过")
	}

	// GetGroupingPolicy和GetNamedGroupingPolicy("g")是一样的，获取所有的g行
	gs, err := e.GetGroupingPolicy()
	if err != nil {
		panic(err)
	}
	fmt.Println(gs)
	gs2, err := e.GetNamedGroupingPolicy("g")
	if err != nil {
		panic(err)
	}
	fmt.Println(gs2)
	// GetFilteredGroupingPolicy和GetFilteredNamedGroupingPolicy("g"是一样的，筛选g行 ，如果涉及到多个g的比如有g2可以使用named方法指定g2来获取g2的数据
	// index为0说明后面的参数组是从v0开始的，依次为v0,v1...
	g3, err := e.GetFilteredGroupingPolicy(0, "1")
	if err != nil {
		panic(err)
	}
	fmt.Println(g3)
	namedGroupingPolicy, err := e.GetFilteredNamedGroupingPolicy("g", 0, "1")
	if err != nil {
		panic(err)
	}
	fmt.Println(namedGroupingPolicy)
	// 这样过滤出domain1域下的g行，中间的角色用空字符串代表跳过此条件
	//namedGroupingPolicy2, err := e.GetFilteredGroupingPolicy(0, "1", "", "domain1")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(namedGroupingPolicy2)

	e.ClearPolicy()
	err = e.SavePolicy()
	if err != nil {
		panic(err)
	}
}
