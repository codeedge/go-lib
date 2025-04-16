package logs

import (
	"fmt"

	"go.uber.org/zap"
)

func GetLogger() *zap.Logger {
	if fgLogger == nil || fgLogger.l == nil {
		panic("日志组件未初始化")
	}
	return fgLogger.l
}

func GetFileLogger() *zap.Logger {
	if fgLogger == nil || fgLogger.l == nil {
		panic("文件日志组件未初始化")
	}
	return fgLogger.fl
}

func SetLevel(level string) {
	if fgLogger == nil || fgLogger.al == nil {
		panic("日志组件未初始化")
	}
	fgLogger.al.SetLevel(fgLevel(level))
}

func Sync() error {
	err := fgLogger.l.Sync()
	if err != nil {
		return err
	}

	err = fgLogger.fl.Sync()
	if err != nil {
		return err
	}

	return nil
}

func Debug(msg string) {
	GetLogger().Debug(msg)
}

func Info(msg string) {
	GetLogger().Info(msg)
}

func Warn(msg string) {
	GetLogger().Warn(msg)
}

func Error(msg string) {
	GetLogger().Error(msg)
}

func Err(err error) {
	GetLogger().Error(err.Error())
}

func Data(msg string) {
	GetFileLogger().Info(msg)
}

func Dataf(format string, v ...interface{}) {
	GetFileLogger().Info(fmt.Sprintf(format, v...))
}

func Debugf(format string, v ...interface{}) {
	GetLogger().Debug(fmt.Sprintf(format, v...))
}

func Infof(format string, v ...interface{}) {
	GetLogger().Info(fmt.Sprintf(format, v...))
}

func Warnf(format string, v ...interface{}) {
	GetLogger().Warn(fmt.Sprintf(format, v...))
}

func Errorf(format string, v ...interface{}) {
	GetLogger().Error(fmt.Sprintf(format, v...))
}
