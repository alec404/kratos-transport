package websocket

import "sync"

// SessionObserver 用于观察 session 新增和删除事件。
type SessionObserver interface {
	// OnSessionAdded 在 session 被加入管理器后触发。
	OnSessionAdded(session *Session)
	// OnSessionRemoved 在 session 被移出管理器后触发。
	OnSessionRemoved(session *Session)
}

// SessionManager 负责管理当前所有在线 websocket session。
type SessionManager struct {
	sessions sync.Map
	observer SessionObserver
}

// NewSessionManager 创建一个 session 管理器。
func NewSessionManager(observer SessionObserver) *SessionManager {
	return &SessionManager{
		sessions: sync.Map{},
		observer: observer,
	}
}

// RegisterObserver 注册 session 观察者。
func (sm *SessionManager) RegisterObserver(observer SessionObserver) {
	sm.observer = observer
}

// Clean 关闭并清理所有 session。
func (sm *SessionManager) Clean() {
	var sessions []*Session
	sm.sessions.Range(func(_, val any) bool {
		sessions = append(sessions, val.(*Session))
		return true
	})

	for _, session := range sessions {
		session.Close()
	}

	sm.sessions.Clear()
}

// Count 返回当前在线 session 数量。
func (sm *SessionManager) Count() int {
	count := 0
	sm.sessions.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// GetSession 根据 sessionID 查询 session。
func (sm *SessionManager) GetSession(sessionID SessionID) *Session {
	if val, ok := sm.sessions.Load(sessionID); ok {
		return val.(*Session)
	}
	return nil
}

// RangeSessions 遍历所有 session。
// 当 fn 返回 false 时，提前终止遍历。
func (sm *SessionManager) RangeSessions(fn func(SessionID, *Session) bool) {
	var sessions []*Session
	sm.sessions.Range(func(_, val any) bool {
		sessions = append(sessions, val.(*Session))
		return true
	})

	for _, session := range sessions {
		if !fn(session.SessionID(), session) {
			return
		}
	}
}

// AddSession 添加一个 session 到管理器。
func (sm *SessionManager) AddSession(session *Session) {
	if session == nil {
		return
	}

	sm.sessions.Store(session.SessionID(), session)
	if sm.observer != nil {
		sm.observer.OnSessionAdded(session)
	}
}

// RemoveSession 从管理器移除一个 session。
func (sm *SessionManager) RemoveSession(session *Session) {
	if session == nil {
		return
	}

	sm.sessions.Delete(session.SessionID())
	if sm.observer != nil {
		sm.observer.OnSessionRemoved(session)
	}
}

// count 是对 Count 的兼容封装，便于保持与参考实现的内部接口一致。
func (sm *SessionManager) count() int {
	return sm.Count()
}

// getSession 是对 GetSession 的兼容封装。
func (sm *SessionManager) getSession(sessionID SessionID) *Session {
	return sm.GetSession(sessionID)
}

// rangeSessions 是对 RangeSessions 的兼容封装。
func (sm *SessionManager) rangeSessions(fn func(SessionID, *Session) bool) {
	sm.RangeSessions(fn)
}

// addSession 是对 AddSession 的兼容封装。
func (sm *SessionManager) addSession(session *Session) {
	sm.AddSession(session)
}

// removeSession 是对 RemoveSession 的兼容封装。
func (sm *SessionManager) removeSession(session *Session) {
	sm.RemoveSession(session)
}
