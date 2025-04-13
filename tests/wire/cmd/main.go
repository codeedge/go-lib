package main

// golang依赖注入工具wire指南 https://segmentfault.com/a/1190000039185137

import (
	"github.com/codeedge/go-lib/tests/wire/internal/db"
	"log"
)

type App struct { // 最终需要的对象
	dao db.Dao // 依赖Dao接口
}

func NewApp(dao db.Dao) *App { // 依赖Dao接口
	return &App{dao: dao}
}

func main() {
	app, cleanup, err := InitApp() // 使用wire生成的injector方法获取app对象
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()                   // 延迟调用cleanup关闭资源
	version, err := app.dao.Version() // 调用Dao接口方法
	if err != nil {
		log.Fatal(err)
	}
	log.Println(version)
}
