package copier

import (
	"fmt"
	"github.com/jinzhu/copier"
	"testing"
	"time"
)

/*
copier是一个结构复制库，支持结构体、切片、map等的数据复制。

golang-copier复制库的使用 https://blog.csdn.net/weixin_49132888/article/details/142852515
什么是vo dto bo po ?
在一个系统中，我们会按照
controller(数参数校验) => service(具体业务逻辑) => dao(数据库的接口)
三层架构的设计进行编码
其中
controller层入参会接收vo 返回 dto 供service调用
service层入参接收 dto 返回 Bo 供Dao持久层调用
dao层入参接收po 将po直接存入数据库
所以就经常需要对这些对象进行转换操作
如果收到转换就会比较麻烦
这里推荐一个开源的复制库jinzhu/copier

github.com/jinzhu/copier的使用
GitHub地址：https://github.com/jinzhu/copier

官方文档：https://pkg.go.dev/github.com/jinzhu/copier

Copier提供了一系列功能

从相同名称的字段复制到字段
从具有相同名称的方法复制到字段
从字段复制到具有相同名称的方法
从切片复制到切片
从结构体复制到切片
从 map 复制到 map
强制复制带有标记的字段
忽略带有标记的字段
深度复制
当处理复杂的数据结构或嵌套结构时，手动复制数据可能会是一个繁琐且容易出错的任务

Copier 在这种情况下非常有用

使用方法
Copy方法src表示数据的源，即需要拷贝的对象
target表示要将拷贝出来的数据复制到的目标对象

copier.Copy(&target, &src)

*/

type User struct {
	Name string
	Age  int
}

type Person struct {
	Name string
	Age  int
	Role string
}

// 结构体复制
// 定义了两个结构体
// 其中Person 有一个字段Role是User没有的
// 现在要将User的字段复制到Person上
// 打印 copier.Person{Name:"姓名", Age:18, Role:""}
func Test_Struct(t *testing.T) {
	user := User{Name: "姓名", Age: 18}
	person := Person{}
	copier.Copy(&person, &user)
	fmt.Printf("%#v\n", person)
}

// 切片复制
// 定义一个[]User类型的切片
// 里边放置两个数据
// 现在要将[]User切片复制到[]Person类型的切片上
// 打印 []copier.Person{copier.Person{Name:"姓名1", Age:18, Role:""}, copier.Person{Name:"姓名2", Age:21, Role:""}}
func Test_Slice(t *testing.T) {
	users := []User{
		{Name: "姓名1", Age: 18},
		{Name: "姓名2", Age: 21},
	}
	var persons []Person
	copier.Copy(&persons, &users)
	fmt.Printf("%#v\n", persons)
}

// 常用结构体标签
// copier:"EmployeeNum" 用于解决源字段和目标字段不一致的问题
// 下面的例子中
// 想实现将User的Name字段转换为Person的PersonName字段
// 现在存在字段名称不一致的问题
// 这就需要在源结构体指明目标结构体的字段
// 打印 copier.Person{Name2:"姓名"}
func Test_Copier(t *testing.T) {
	type User struct {
		Name1 string `copier:"Name"`
	}

	type Person struct {
		Name2 string `copier:"Name"`
	}

	user := User{
		Name1: "姓名",
	}
	persons := Person{}
	copier.Copy(&persons, &user)
	fmt.Printf("%#v\n", persons)
}

// 常用结构体标签
// copier:"-" 忽略这个字段不被复制
// 打印 copier.Person{Name:""}
func Test_CopierIgnore(t *testing.T) {
	type User struct {
		Name string
	}

	type Person struct {
		Name string `copier:"-"`
	}
	user := User{
		Name: "姓名",
	}
	persons := Person{}
	copier.Copy(&persons, &user)
	fmt.Printf("%#v\n", persons)
}

// 常用结构体标签
// copier:"must" 强制必须复制该字段，如果没有复制则报异常退出程序
// 由于Person.Sex字段没有被赋值
// 程序会报错抛出panic结束运行
// 打印 panic: Field Sex has must tag but was not copied [recovered]
//
//	panic: Field Sex has must tag but was not cop
//	panic: Field Sex has must tag but was not copied
func Test_CopierMust(t *testing.T) {
	type User struct {
		Name string
	}

	type Person struct {
		Sex string `copier:"must"`
	}
	user := User{
		Name: "姓名",
	}
	persons := Person{}
	copier.Copy(&persons, &user)

	fmt.Printf("%#v\n", persons)
}

// 常用结构体标签
// copier:"must, nopanic" 强制必须复制该字段, 如果没有复制，则会抛出错误，但不会结束程序
// 注意：“must, nopanic” nopanic前要带空格 否则err就是nil
// 打印 &errors.errorString{s:"copier field name tag must be start upper case"}
func Test_CopierMustNopanic(t *testing.T) {
	type User struct {
		Name string
	}

	type Person struct {
		Sex uint `copier:"must, nopanic"` // 此处指定该字段必须被赋值，如果没有则抛出错误
	}
	user := User{}
	persons := Person{}
	err := copier.Copy(&persons, &user)

	fmt.Printf("%#v\n", err)
}

// 自定义
// 比如设置时间格式
// 打印 copier.Person{Time:"2025-05-10 22:11:51", Name:"tom"} <nil>
func Test_CopyWithOption(t *testing.T) {
	type User struct {
		Time *time.Time
		Name string
	}

	type Person struct {
		Time string
		Name string
	}
	now := time.Now()
	user := User{Time: &now, Name: "tom"}
	persons := Person{}
	err := copier.CopyWithOption(&persons, &user, copier.Option{
		IgnoreEmpty: true,
		Converters: []copier.TypeConverter{
			{
				// 这样写同时支持time.Time 和 *time.Time SrcType和Fn转换的类型要写一样的 建议全部写成结构体
				// 测试指针不能满足所有场景 如果这里写&time.Time{}，Fn写srcTime.(*time.Time)当Copy的字段为结构体时不走Fn函数
				SrcType: time.Time{},
				DstType: "",
				Fn: func(srcTime any) (dst any, err error) {
					// 测试只有时间类型会经过此函数，所有可以直接转换类型
					src, _ := srcTime.(time.Time)
					return src.Format(time.DateTime), nil
					//if src, ok := srcTime.(time.Time); ok {
					//	return src.Format(time.DateTime), nil
					//}
					//return nil, errors.New("time err")
				},
			},
		},
	})
	fmt.Printf("%#v %v\n", persons, err)
}
