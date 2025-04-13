//go:build wireinject
// +build wireinject

package main

import (
	"github.com/codeedge/go-lib/tests/wire/internal/config"
	"github.com/codeedge/go-lib/tests/wire/internal/db"
	"github.com/google/wire"
)

//func InitApp() (*App, error) {
//	panic(wire.Build(config.Provider, db.Provider, NewApp)) // 调用wire.Build方法传入所有的依赖对象以及构建最终对象的函数得到目标对象
//}

func InitApp() (*App, func(), error) { // 声明第二个返回值
	panic(wire.Build(config.Provider, db.Provider, NewApp)) // 调用wire.Build方法传入所有的依赖对象以及构建最终对象的函数得到目标对象
}
