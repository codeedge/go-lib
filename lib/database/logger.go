package database

import (
	"context"
	"errors"
	"github.com/codeedge/go-lib/lib/logs"
	"time"

	"gorm.io/gorm/logger"
)

type GormLogger struct {
	level                     logger.LogLevel
	IgnoreRecordNotFoundError bool
	SlowThreshold             time.Duration
}

func NewGormLogger() *GormLogger {
	return &GormLogger{
		level:                     logger.Info,
		IgnoreRecordNotFoundError: true,
		SlowThreshold:             3 * time.Second,
	}
}

var _ logger.Interface = (*GormLogger)(nil)

func (l *GormLogger) LogMode(level logger.LogLevel) logger.Interface {
	l.level = level
	return l
}

func (l *GormLogger) Info(ctx context.Context, s string, data ...interface{}) {
	if l.level < logger.Info {
		return
	}
	logs.Infof(s)
}

func (l *GormLogger) Warn(ctx context.Context, s string, i ...interface{}) {
	if l.level < logger.Warn {
		return
	}
	logs.Warnf(s)
}

func (l *GormLogger) Error(ctx context.Context, s string, i ...interface{}) {
	if l.level < logger.Error {
		return
	}
	logs.Errorf(s)
}

func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	// 获取运行时间
	elapsed := time.Since(begin)
	// 获取 SQL 语句和返回条数
	sql, rows := fc()
	// args := []any{"sql", sql, "rows", rows, "elapsed", elapsed.Milliseconds()}
	switch {
	// Gorm 错误时打印
	case err != nil && l.level >= logger.Error && (!errors.Is(err, logger.ErrRecordNotFound) || !l.IgnoreRecordNotFoundError):
		logs.Errorf("发生错误：%v。SQL(耗时：%v，行数：%v) %v", err.Error(), elapsed, rows, sql)
	// 慢SQL语句
	case l.SlowThreshold != 0 && elapsed > l.SlowThreshold:
		logs.Infof("慢SQL(耗时：%v，行数：%v) %v", elapsed, rows, sql)
	// 打印SQL语句
	case l.level == logger.Info:
		logs.Infof("SQL(耗时：%v，行数：%v) %v", elapsed, rows, sql)
	}
}
