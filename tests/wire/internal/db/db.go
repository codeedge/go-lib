package db

import (
	"database/sql"
	"github.com/codeedge/go-lib/tests/wire/internal/config"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/wire"
)

var Provider = wire.NewSet(New, NewDao, wire.Bind(new(Dao), new(*dao)))

func New(cfg *config.Config) (db *sql.DB, cleanup func(), err error) { // 声明第二个返回值
	db, err = sql.Open("mysql", cfg.Database.Dsn)
	if err != nil {
		return
	}
	if err = db.Ping(); err != nil {
		return
	}
	cleanup = func() { // cleanup函数中关闭数据库连接
		db.Close()
	}
	return db, cleanup, nil
}
