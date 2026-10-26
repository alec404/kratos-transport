package websocket

import "time"

// businessHeartbeatPingPayload 是业务层心跳 ping 的固定帧内容。
// 当前协议约定：
//   - server -> client: []byte{0xB0, 0x04}
//   - client -> server: []byte{}
//
// 注意：这里是“完整 binary data frame”的保留值，不是业务 payload 的一部分。
var businessHeartbeatPingPayload = []byte{0xB0, 0x04}

const defaultBusinessHeartbeatCheckInterval = 5 * time.Second

type businessHeartbeatConfig struct {
	enabled       bool
	interval      time.Duration
	ackTimeout    time.Duration
	checkInterval time.Duration
}

func (c businessHeartbeatConfig) isEnabled() bool {
	return c.enabled && c.interval > 0 && c.ackTimeout > 0
}

func (c businessHeartbeatConfig) normalizedCheckInterval() time.Duration {
	if c.checkInterval > 0 {
		return c.checkInterval
	}
	return defaultBusinessHeartbeatCheckInterval
}

// isBusinessHeartbeatPingPayload 判断整个 binary frame 是否就是保留的心跳 ping 帧。
func isBusinessHeartbeatPingPayload(data []byte) bool {
	return len(data) == len(businessHeartbeatPingPayload) &&
		data[0] == businessHeartbeatPingPayload[0] &&
		data[1] == businessHeartbeatPingPayload[1]
}
