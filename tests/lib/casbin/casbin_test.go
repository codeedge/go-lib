package casbin

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/database"
	"github.com/codeedge/go-lib/lib/logs"
	"github.com/gogf/gf/v2/util/gconv"
	"testing"

	"github.com/codeedge/go-lib/lib/casbin"
)

func Test_Casbin(t *testing.T) {
	logs.NewFgLogger("", "", 10, 3, "info")
	// 连接MySQL
	db, err := database.Init(false, "root:root@tcp(192.168.1.201:3306)/feige_sms_op?charset=utf8", nil)
	if err != nil {
		panic("数据库连接失败：" + err.Error())
	}

	casbin.Init(&casbin.Config{
		Key:           "op",
		Path:          "./rbac_models.conf",
		DB:            db,
		RedisAddr:     "192.168.1.201:6379",
		RedisPassword: "root",
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

	if ok, e := e.UpdatePolicy([]string{"admin", "data1", "read"}, []string{"admin", "data1", "write"}); !ok {
		fmt.Println("Policy不存在", e)
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

	// conf文件配置了  || r.sub == "root"则root拥有所有权限
	if ok, _ := e.Enforce("root", "data1", "write"); ok {
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

func Test_Casbin2(t *testing.T) {
	logs.NewFgLogger("", "", 10, 3, "info")
	// 连接MySQL
	db, err := database.Init(false, "root:root@tcp(192.168.1.201:3306)/feige_sms_op?charset=utf8", nil)
	if err != nil {
		panic("数据库连接失败：" + err.Error())
	}

	casbin.Init(&casbin.Config{
		Key:           "op2",
		Path:          "./rbac_domain_models.conf",
		DB:            db,
		RedisAddr:     "192.168.1.201:6379",
		RedisPassword: "root",
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

	// 获取所有g行，返回的结构为 [[1 r:1 d:0] [2 r:1 d:0] [1 r:-1 d:-1] [1 r:5 d:14] [r:1 r:2 d:14] ... ]
	a1, _ := e.GetGroupingPolicy()
	fmt.Println("a1", a1)
	// ForUser命名的函数代表的是用户或角色，不只是针对用户。
	// 只能返回用户或角色所有域对应的权限，不能返回用户所在角色的权限 返回：[[1 d:0 button btn-finance2] [1 d:1 button btn-finance2]]
	a3, _ := e.GetPermissionsForUser("1")
	fmt.Println("a3", a3)
	// 只能返回用户或角色所有域对应的权限，不能返回用户所在角色的权限 返回： [[r:7 d:0 button btn-team-manage] [r:7 d:14 button btn-team-manage]]
	a31, _ := e.GetPermissionsForUser("r:7")
	fmt.Println("a31", a31)
	// 和 GetPermissionsForUser只有一个参数 的区别是过滤某个域下的数据。只能返回用户或角色某域下对应的权限，不能返回继承的权限 这里返回的是用户"1"的权限：[[1 d:0 button btn-finance2]]
	a4, _ := e.GetPermissionsForUser("1", "d:0")
	fmt.Println("a4", a4)
	// 只能返回用户或角色某域下对应的权限，不能返回继承的权限 这里返回的是角色"r:5"的权限：[[r:5 d:0 button btn-finance]]
	a41, _ := e.GetPermissionsForUser("r:5", "d:0")
	fmt.Println("a41", a41)
	// 和 GetPermissionsForUser 函数加上域参数的结果竟然不一样！ 除了返回用户或者角色的权限，还返回了继承上一级的权限，这里是用户"1"继承了角色"r:5"的权限 [[r:5 d:0 button btn-finance] [1 d:0 button btn-finance2]]
	a2 := e.GetPermissionsForUserInDomain("1", "d:0")
	fmt.Println("a2", a2)
	// 和 GetPermissionsForUser 函数加上域参数的结果竟然不一样！  除了返回用户或者角色的权限，还返回了继承上一级的权限，这里是角色"r:5"继承了角色"r:6"的权限 [[r:6 d:0 /platform-api/admin/enterpriseEdit POST] [r:5 d:0 button btn-finance]]
	a20 := e.GetPermissionsForUserInDomain("r:5", "d:0")
	fmt.Println("a20", a20)
	// 返回了用户或角色在不同域下的所有权限，但是没有返回继承的隐含权限。[[1 d:0 button btn-finance2] [1 d:1 button btn-finance2]] 官网没有使用域的示例返回了继承的隐含权限，使用域的情况要用下面带域的参数才会全部返回
	a21, err := e.GetImplicitPermissionsForUser("1")
	fmt.Println("a21", a21, err)
	// 返回了用户或角色在不同域下的所有权限，但是没有返回继承的隐含权限。[[r:5 d:0 button btn-finance] [r:5 d:14 button btn-finance]] 官网没有使用域的示例返回了继承的隐含权限，使用域的情况要用下面带域的参数才会全部返回
	a211, err := e.GetImplicitPermissionsForUser("r:5")
	fmt.Println("a211", a211, err)
	// 这个会返回"d:0"域下用户或角色和其多级继承的所有权限 [[1 d:0 button btn-finance2] [r:5 d:0 button btn-finance] [r:6 d:0 /platform-api/admin/enterpriseEdit POST] [r:7 d:0 button btn-team-manage] [r:8 d:0 /platform-api/admin/enterpriseDetail GET] [r:8 d:0 button btn-enterprise-change]]
	a22, err := e.GetImplicitPermissionsForUser("1", "d:0")
	fmt.Println("a22", a22, err)
	// 这个会返回"d:0"域下用户或角色和其多级继承的所有权限 [[r:5 d:0 button btn-finance] [r:6 d:0 /platform-api/admin/enterpriseEdit POST] [r:7 d:0 button btn-team-manage] [r:8 d:0 /platform-api/admin/enterpriseDetail GET] [r:8 d:0 button btn-enterprise-change]]
	a23, err := e.GetImplicitPermissionsForUser("r:5", "d:0")
	fmt.Println("a23", a23, err)
	// GetPolicy内部调用的是GetNamedPolicy("p"),GetNamedPolicy("p")内部调用的是 e.model.GetPolicy("p", ptype)，注意这里的ptype就是参数"p"，但是内部还要用一个写死的"p"来调用，Group系列方法也是类似的，待研究(猜测可能是因为支持多个"p，g"；比如"p2"，"g2"；需要一个固定的p来描述这些"p2"、"p3"是属于"p".参考： https://casbin.org/docs/zh-CN/syntax-for-models#多个班级类型 )。
	a5, _ := e.GetPolicy()
	fmt.Println("a5", a5)
	a6, _ := e.GetNamedPolicy("p")
	fmt.Println("a6", a6)

	e.ClearPolicy()
	err = e.SavePolicy()
	if err != nil {
		panic(err)
	}
}

func Test_Casbin3(t *testing.T) {
	logs.NewFgLogger("", "", 10, 3, "info")
	// 连接MySQL
	db, err := database.Init(false, "root:root@tcp(192.168.1.201:3306)/feige_sms_op?charset=utf8", nil)
	if err != nil {
		panic("数据库连接失败：" + err.Error())
	}

	casbin.Init(&casbin.Config{
		Key:           "op2",
		Path:          "./rbac_domain_models.conf",
		DB:            db,
		RedisAddr:     "192.168.1.201:6379",
		RedisPassword: "root",
	})

	//从DB加载策略，上面这种参数会在执行casbin.NewEnforcer时自动调用，无需重复调用，但是封装后每次使用前需要调用一次，否则无法加载最新的数据
	e := casbin.Enforcer()
	e.LoadPolicy()

	adminId := 1
	// 测试返回role
	// 此方法在有域的情况下返回nil，说明无法获取用户所有域下的角色
	a, err := casbin.Enforcer().GetRolesForUser(gconv.String(adminId))
	fmt.Println(a, err)
	// 下面2个方法在不同域下结果一致，都能获取对应域下的角色
	b, err := casbin.Enforcer().GetRolesForUser(gconv.String(adminId), "d:0")
	fmt.Println(b, err)
	c, err := casbin.Enforcer().GetRolesForUser(gconv.String(adminId), "d:14")
	fmt.Println(c, err)
	d := casbin.Enforcer().GetRolesForUserInDomain(gconv.String(adminId), "d:0")
	fmt.Println(d, err)
	e1 := casbin.Enforcer().GetRolesForUserInDomain(gconv.String(adminId), "d:14")
	fmt.Println(e1, err)
	// 此方法在有域的情况下返回nil，说明无法获取用户所有域下的角色和隐含角色
	f, err := casbin.Enforcer().GetImplicitRolesForUser(gconv.String(adminId))
	fmt.Println(f, err)
	// 下面2个方法都能获取用户域下角色和隐含角色
	g1, err := casbin.Enforcer().GetImplicitRolesForUser(gconv.String(adminId), "d:0")
	fmt.Println(g1, err)
	h, err := casbin.Enforcer().GetImplicitRolesForUser(gconv.String(adminId), "d:14")
	fmt.Println(h, err)
	// 获取用户对应的不同域下的角色使用这个函数，如果需要继续获取某个企业下所有的角色，使用GetImplicitRolesForUser方法
	i, err := casbin.Enforcer().GetFilteredGroupingPolicy(0, "1")
	fmt.Println(i, err)

	r1, err := casbin.Enforcer().GetUsersForRole("r:1", "d:0")
	fmt.Println(r1, err)
	r2 := casbin.Enforcer().GetUsersForRoleInDomain("r:5", "d:0")
	fmt.Println(r2)
	//e.ClearPolicy()
	//err = e.SavePolicy()
	//if err != nil {
	//	panic(err)
	//}
}
