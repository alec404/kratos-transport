package websocket

import (
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
)

const logKey = "[websocket]"

// LogDebug 输出调试日志。
func LogDebug(args ...any) {
	log.Debugf("%s %s", logKey, fmt.Sprint(args...))
}

// LogInfo 输出信息日志。
func LogInfo(args ...any) {
	log.Infof("%s %s", logKey, fmt.Sprint(args...))
}

// LogWarn 输出警告日志。
func LogWarn(args ...any) {
	log.Warnf("%s %s", logKey, fmt.Sprint(args...))
}

// LogError 输出错误日志。
func LogError(args ...any) {
	log.Errorf("%s %s", logKey, fmt.Sprint(args...))
}

// LogFatal 输出致命日志。
func LogFatal(args ...any) {
	log.Fatalf("%s %s", logKey, fmt.Sprint(args...))
}

// LogDebugf 按格式输出调试日志。
func LogDebugf(format string, args ...any) {
	log.Debugf("%s %s", logKey, fmt.Sprintf(format, args...))
}

// LogInfof 按格式输出信息日志。
func LogInfof(format string, args ...any) {
	log.Infof("%s %s", logKey, fmt.Sprintf(format, args...))
}

// LogWarnf 按格式输出警告日志。
func LogWarnf(format string, args ...any) {
	log.Warnf("%s %s", logKey, fmt.Sprintf(format, args...))
}

// LogErrorf 按格式输出错误日志。
func LogErrorf(format string, args ...any) {
	log.Errorf("%s %s", logKey, fmt.Sprintf(format, args...))
}

// LogFatalf 按格式输出致命日志。
func LogFatalf(format string, args ...any) {
	log.Fatalf("%s %s", logKey, fmt.Sprintf(format, args...))
}
