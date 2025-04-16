package logs

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type FgLogger struct {
	fl *zap.Logger // 写日志到文件和控制台
	l  *zap.Logger // 写日志到控制台
	al *zap.AtomicLevel
}

var (
	fgLogger *FgLogger
	oneLog   sync.Once
)

func NewFgLogger(path, filename string, maxSize, maxAge int, level string) {
	oneLog.Do(func() {
		// 设置日志等级
		al := zap.NewAtomicLevelAt(fgLevel(level))

		// 只写控制台
		consoleEncoder := fgConsoleEncoder(zapcore.CapitalColorLevelEncoder)
		consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), al)
		consoleLogger := zap.New(consoleCore)

		// 写文件同时写控制台
		fileEncoder := fgFileEncoder()
		writeSyncer := fgFileWriter(path, filename, maxSize, maxAge)
		fileCore := zapcore.NewCore(fileEncoder, writeSyncer, zapcore.InfoLevel)

		fConsoleEncoder := fgConsoleEncoder(fgEncodeLevel)
		fConsoleCore := zapcore.NewCore(fConsoleEncoder, zapcore.AddSync(os.Stdout), zapcore.InfoLevel)

		fileAndConsoleCore := zapcore.NewTee(fileCore, fConsoleCore)
		fileLogger := zap.New(fileAndConsoleCore)

		fgLogger = &FgLogger{fl: fileLogger, l: consoleLogger, al: &al}
	})
}

func fgLevel(level string) zapcore.Level {
	l := InfoLevel
	switch strings.ToLower(level) {
	case "debug":
		l = DebugLevel
		break
	case "info":
		l = InfoLevel
		break
	case "warn":
		l = WarnLevel
		break
	case "error":
		l = ErrorLevel
		break
	}

	return l
}

// 写入控制台的日志格式
func fgConsoleEncoder(encodeLevel zapcore.LevelEncoder) zapcore.Encoder {
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:          "msg",
		LevelKey:            "level",
		TimeKey:             "ts",
		NameKey:             "",
		CallerKey:           "",
		FunctionKey:         "",
		StacktraceKey:       "",
		SkipLineEnding:      false,
		LineEnding:          zapcore.DefaultLineEnding, // 每行后面的分隔符
		EncodeLevel:         encodeLevel,
		EncodeTime:          fgEncodeTime, // 设置日志时间格式
		EncodeDuration:      zapcore.SecondsDurationEncoder,
		EncodeCaller:        zapcore.ShortCallerEncoder,
		EncodeName:          nil,
		NewReflectedEncoder: nil,
		ConsoleSeparator:    "--", // 每个字段之间的分隔符
	}

	return zapcore.NewConsoleEncoder(encoderConfig)
}

// 写入文件的日志格式
func fgFileEncoder() zapcore.Encoder {
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:          "msg",
		LevelKey:            "level",
		TimeKey:             "ts",
		NameKey:             "",
		CallerKey:           "",
		FunctionKey:         "",
		StacktraceKey:       "",
		SkipLineEnding:      false,
		LineEnding:          zapcore.DefaultLineEnding, // 每行后面的分隔符
		EncodeLevel:         fgEncodeLevel,             // 设置日志等级文字格式
		EncodeTime:          fgEncodeTime,              // 设置日志时间格式
		EncodeDuration:      zapcore.SecondsDurationEncoder,
		EncodeCaller:        zapcore.ShortCallerEncoder,
		EncodeName:          nil,
		NewReflectedEncoder: nil,
		ConsoleSeparator:    "--", // 每个字段之间的分隔符
	}

	return zapcore.NewConsoleEncoder(encoderConfig)
}

// 自定义日志级别文字显示
func fgEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	if level == zapcore.InfoLevel {
		enc.AppendString("DATA")
	} else {
		enc.AppendString(level.CapitalString())
	}
}

// 设置日志记录中时间的格式
func fgEncodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

// 日志写入文件
func fgFileWriter(filepath, filename string, maxSize, maxAge int) zapcore.WriteSyncer {
	if filepath == "" {
		filepath = "./log/"
	}

	if filename == "" {
		filename = "data.log"
	}

	filename = fmt.Sprintf("%s/%s", filepath, filename)

	if maxSize <= 0 {
		maxSize = 100
	}

	if maxAge <= 0 {
		maxAge = 3
	}

	lumberJackLogger := &lumberjack.Logger{
		Filename:   filename, // 日志文件的位置
		MaxSize:    maxSize,  // 在进行切割之前，日志文件的最大大小（以MB为单位）
		MaxBackups: 100,      // 保留旧文件的最大个数
		MaxAge:     maxAge,   // 保留旧文件的最大天数
		Compress:   false,    // 是否压缩归档旧文件
		LocalTime:  true,
	}

	return zapcore.AddSync(lumberJackLogger)
}
