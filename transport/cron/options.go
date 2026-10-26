package cron

import (
	"net/url"
	"time"

	robfigcron "github.com/robfig/cron/v3"
)

// ServerOption 表示 cron 服务配置项。
type ServerOption func(*Server)

// WithEndpoint 设置用于服务发现的 endpoint。
// cron 本身不监听网络端口，默认返回空 endpoint；如需注册中心可手动指定。
func WithEndpoint(endpoint *url.URL) ServerOption {
	return func(s *Server) {
		s.endpoint = endpoint
	}
}

// WithGracefullyShutdown 设置是否等待运行中的任务执行完成后再返回 Stop。
func WithGracefullyShutdown(enable bool) ServerOption {
	return func(s *Server) {
		s.gracefullyShutdown = enable
	}
}

// WithParser 覆盖默认 cron 表达式解析器。
func WithParser(parser robfigcron.Parser) ServerOption {
	return func(s *Server) {
		s.parser = parser
	}
}

// WithLocation 设置 cron 任务调度时区。
func WithLocation(location *time.Location) ServerOption {
	return func(s *Server) {
		if location != nil {
			s.cronOptions = append(s.cronOptions, robfigcron.WithLocation(location))
		}
	}
}

// WithChain 设置任务执行链，可用于 recover、skip 等扩展。
func WithChain(wrappers ...robfigcron.JobWrapper) ServerOption {
	return func(s *Server) {
		if len(wrappers) > 0 {
			s.cronOptions = append(s.cronOptions, robfigcron.WithChain(wrappers...))
		}
	}
}
