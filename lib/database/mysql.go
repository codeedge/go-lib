package database

import (
	"context"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"gorm.io/plugin/dbresolver"
)

var (
	mysqlDB *gorm.DB
)

// GetDB 获取数据库
func GetDB(ctx context.Context) *gorm.DB {
	return mysqlDB.WithContext(ctx)
}

// Init 初始化数据库
func Init(separation bool, masterDSN string, slaveDSN []string) (*gorm.DB, error) {
	var err error
	if separation {
		err = connectRWDB(masterDSN, slaveDSN)
	}

	d, openErr := gorm.Open(mysql.Open(masterDSN), &gorm.Config{
		Logger: NewGormLogger(),
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 单数表名
		},
		SkipDefaultTransaction:                   true, // 跳过默认事物（大约 30%+ 性能提升）
		DisableForeignKeyConstraintWhenMigrating: true, // 禁用自动创建外键约束
	})

	if openErr != nil {
		err = openErr
	} else {
		db, dbErr := d.DB()
		if err != nil {
			err = dbErr
		} else {
			db.SetMaxOpenConns(100)               // 设置数据库的最大打开连接数
			db.SetMaxIdleConns(100)               // 设置最大空闲连接数
			db.SetConnMaxIdleTime(time.Hour)      // 设置连接空闲的最大时间
			db.SetConnMaxLifetime(12 * time.Hour) // 设置空闲连接最大存活时间

			mysqlDB = d
		}
	}

	return mysqlDB, err
}

func connectRWDB(masterDSN string, slaveDSN []string) error {
	d, err := gorm.Open(mysql.Open(masterDSN), &gorm.Config{
		Logger: NewGormLogger(),
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 单数表名
		},
		SkipDefaultTransaction:                   true, // 跳过默认事物（大约 30%+ 性能提升）
		DisableForeignKeyConstraintWhenMigrating: true, // 禁用自动创建外键约束
	})

	if err != nil {
		return err
	}

	var replicas []gorm.Dialector
	for _, s := range slaveDSN {
		cfg := mysql.Config{
			DSN: s,
		}
		replicas = append(replicas, mysql.New(cfg))
	}

	err = d.Use(
		dbresolver.Register(dbresolver.Config{
			Sources: []gorm.Dialector{mysql.New(mysql.Config{
				DSN: masterDSN,
			})},
			Replicas: replicas,
			Policy:   dbresolver.RandomPolicy{},
		}).
			SetMaxIdleConns(100).
			SetMaxOpenConns(100).
			SetConnMaxIdleTime(time.Hour).
			SetConnMaxLifetime(time.Hour),
	)
	if err != nil {
		return err
	}

	mysqlDB = d

	return nil
}
