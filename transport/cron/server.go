package cron

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	kratosTransport "github.com/go-kratos/kratos/v2/transport"
	robfigcron "github.com/robfig/cron/v3"
)

var (
	_ kratosTransport.Server     = (*Server)(nil)
	_ kratosTransport.Endpointer = (*Server)(nil)
)

// Server 是基于 robfig/cron 的 kratos transport.Server 实现。
type Server struct {
	scheduler *robfigcron.Cron
	parser    robfigcron.Parser
	endpoint  *url.URL

	gracefullyShutdown bool
	cronOptions        []robfigcron.Option

	stateMu sync.Mutex
	jobMu   sync.RWMutex

	running    bool
	entrySpecs map[robfigcron.EntryID]string
}

// NewServer 创建一个 cron Server。
// 默认支持：
//   - 6 位秒级表达式：*/10 * * * * *
//   - 5 位标准表达式：*/5 * * * *
//   - descriptor：@every 10s / @daily
func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		parser: robfigcron.NewParser(
			robfigcron.SecondOptional |
				robfigcron.Minute |
				robfigcron.Hour |
				robfigcron.Dom |
				robfigcron.Month |
				robfigcron.Dow |
				robfigcron.Descriptor,
		),
		gracefullyShutdown: true,
		entrySpecs:         make(map[robfigcron.EntryID]string),
	}

	srv.init(opts...)
	srv.scheduler = robfigcron.New(append([]robfigcron.Option{robfigcron.WithParser(srv.parser)}, srv.cronOptions...)...)
	return srv
}

func (s *Server) init(opts ...ServerOption) {
	for _, o := range opts {
		o(s)
	}
}

// Name 返回 transport 名称。
func (s *Server) Name() string {
	return KindCron
}

// Start 启动 cron 调度器。
func (s *Server) Start(_ context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.running {
		return nil
	}

	s.scheduler.Start()
	s.running = true

	LogInfof("server started with %d cron job(s)", s.GetJobCount())
	return nil
}

// Stop 停止 cron 调度器。
func (s *Server) Stop(ctx context.Context) error {
	s.stateMu.Lock()
	if !s.running {
		s.stateMu.Unlock()
		return nil
	}

	s.running = false
	waitCtx := s.scheduler.Stop()
	graceful := s.gracefullyShutdown
	s.stateMu.Unlock()

	LogInfo("server stopping...")

	if !graceful {
		LogInfo("server stopped without waiting running jobs")
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-waitCtx.Done():
		LogInfo("server stopped")
		return nil
	case <-ctx.Done():
		LogWarnf("server shutdown canceled: %v", ctx.Err())
		return ctx.Err()
	}
}

// Endpoint 返回用于服务发现注册的 endpoint。
// cron 本身没有监听地址；如未通过 WithEndpoint 指定，则返回空 URL。
func (s *Server) Endpoint() (*url.URL, error) {
	if s.endpoint == nil {
		return &url.URL{}, nil
	}
	return s.endpoint, nil
}

// StartTimerJob 注册一个定时任务。
// 可以在 Start 前或 Start 后调用；任务会在调度器启动后按 spec 执行。
func (s *Server) StartTimerJob(spec string, cmd func()) (robfigcron.EntryID, error) {
	if strings.TrimSpace(spec) == "" {
		return 0, errors.New("cron spec is required")
	}
	if cmd == nil {
		return 0, errors.New("cron job is required")
	}

	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	entryID, err := s.scheduler.AddJob(spec, s.wrapJob(spec, robfigcron.FuncJob(cmd)))
	if err != nil {
		return 0, err
	}

	s.entrySpecs[entryID] = spec
	LogInfof("cron job registered: id=%d spec=%q", entryID, spec)
	return entryID, nil
}

// StopTimerJob 根据任务 ID 停止单个任务。
func (s *Server) StopTimerJob(entryID robfigcron.EntryID) {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	s.scheduler.Remove(entryID)
	if spec, ok := s.entrySpecs[entryID]; ok {
		delete(s.entrySpecs, entryID)
		LogInfof("cron job removed: id=%d spec=%q", entryID, spec)
	}
}

// StopAllJobs 停止全部定时任务。
func (s *Server) StopAllJobs() {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	count := len(s.entrySpecs)
	for entryID := range s.entrySpecs {
		s.scheduler.Remove(entryID)
		delete(s.entrySpecs, entryID)
	}

	LogInfof("all cron jobs removed, total=%d", count)
}

// GetJobCount 返回当前已注册的任务数量。
func (s *Server) GetJobCount() int {
	s.jobMu.RLock()
	defer s.jobMu.RUnlock()
	return len(s.entrySpecs)
}

func (s *Server) wrapJob(spec string, job robfigcron.Job) robfigcron.Job {
	return robfigcron.FuncJob(func() {
		defer func() {
			if r := recover(); r != nil {
				LogErrorf("cron job panic recovered: spec=%q panic=%v", spec, r)
			}
		}()

		job.Run()
	})
}

// JobSpec 返回指定 entryID 对应的 cron 表达式。
func (s *Server) JobSpec(entryID robfigcron.EntryID) (string, bool) {
	s.jobMu.RLock()
	defer s.jobMu.RUnlock()

	spec, ok := s.entrySpecs[entryID]
	return spec, ok
}

// String 返回服务的可读描述。
func (s *Server) String() string {
	endpoint := ""
	if s.endpoint != nil {
		endpoint = s.endpoint.String()
	}
	return fmt.Sprintf("cron_server(running=%t,jobs=%d,endpoint=%q)", s.running, s.GetJobCount(), endpoint)
}
