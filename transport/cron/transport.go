package cron

import (
	"github.com/go-kratos/kratos/v2/selector"
	kratosTransport "github.com/go-kratos/kratos/v2/transport"
)

const (
	// KindCron 表示 cron transport 类型。
	KindCron = "cron"
)

var _ kratosTransport.Transporter = (*Transport)(nil)

// Transport 是 cron 任务在 kratos 上下文中的 transport 描述。
type Transport struct {
	endpoint    string
	operation   string
	reqHeader   headerCarrier
	replyHeader headerCarrier
	nodeFilters []selector.NodeFilter
}

// Kind 返回 transport 类型。
func (tr *Transport) Kind() kratosTransport.Kind {
	return KindCron
}

// Endpoint 返回 transport endpoint。
func (tr *Transport) Endpoint() string {
	return tr.endpoint
}

// Operation 返回 transport operation。
func (tr *Transport) Operation() string {
	return tr.operation
}

// RequestHeader 返回请求头。
func (tr *Transport) RequestHeader() kratosTransport.Header {
	return tr.reqHeader
}

// ReplyHeader 返回响应头。
func (tr *Transport) ReplyHeader() kratosTransport.Header {
	return tr.replyHeader
}

// NodeFilters 返回客户端 selector filters。
func (tr *Transport) NodeFilters() []selector.NodeFilter {
	return tr.nodeFilters
}

type headerCarrier struct{}

// Get 返回 key 对应的值。
func (hc headerCarrier) Get(_ string) string {
	return ""
}

// Set 设置 key 对应的值。
func (hc headerCarrier) Set(_ string, _ string) {}

// Keys 返回所有 key。
func (hc headerCarrier) Keys() []string {
	return nil
}

// Add 追加 key/value。
func (hc headerCarrier) Add(_ string, _ string) {}

// Values 返回 key 对应的所有值。
func (hc headerCarrier) Values(_ string) []string {
	return nil
}
