package gostr

import (
	"fmt"
	"github.com/codeedge/go-lib/lib/util"
	"testing"
)

func Test_gstr_01(t *testing.T) {
	str1 := "喜马拉雅山"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 5, 5, "*")
	fmt.Println(str1)

	str1 = "喜马拉雅"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)

	str1 = "喜马拉"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)

	str1 = "喜马"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)

	str1 = "喜"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)
}

func Test_gstr_02(t *testing.T) {
	str1 := "hello123喜马拉雅山"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 5, 5, "*")
	fmt.Println(str1)

	str1 = "hello123喜马拉雅"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)

	str1 = "喜马拉hello123"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)

	str1 = "喜hello123马"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)

	str1 = "喜hello123"
	fmt.Print(str1 + "===>>>")
	str1 = util.HideStr(str1, 1, 1, "*")
	fmt.Println(str1)
}
