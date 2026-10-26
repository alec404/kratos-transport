package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/alec404/kratos-transport/transport/websocket"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
)

var testServer *websocket.Server

const (
	// MessageTypeChat 表示聊天消息类型。
	MessageTypeChat = iota + 1
	// MessageTypeSystem 表示系统提示消息类型。
	MessageTypeSystem
)

// ChatMessage 是示例中使用的聊天消息结构。
type ChatMessage struct {
	Sender    string `json:"sender"`
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
	VIPOnly   bool   `json:"vipOnly,omitempty"`
}

// SystemMessage 表示服务端发给前端的提示消息。
type SystemMessage struct {
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
}

// DemoTokenClaims 是示例 token 中保存的简化用户身份信息。
type DemoTokenClaims struct {
	UserID   string `json:"userId"`
	UserName string `json:"userName"`
	VIP      bool   `json:"vip"`
	ExpireAt int64  `json:"expireAt"`
}

// LoginRequest 表示示例页面登录接口请求。
type LoginRequest struct {
	UserName string `json:"userName"`
	VIP      bool   `json:"vip"`
}

// ViewerResponse 表示当前登录身份。
type ViewerResponse struct {
	LoggedIn bool   `json:"loggedIn"`
	UserID   string `json:"userId,omitempty"`
	UserName string `json:"userName,omitempty"`
	VIP      bool   `json:"vip,omitempty"`
	Role     string `json:"role,omitempty"`
}

type SessionUser struct {
	UserID   string
	UserName string
	VIP      bool
}

const (
	authCookieName      = "demo_ws_token"
	demoTokenSecret     = "kratos-transport-websocket-example-secret"
	demoTokenExpireTime = 2 * time.Hour
)

func main() {
	// 启动一个简单静态文件服务，便于直接在浏览器中打开示例页面。
	go func() {
		webDir := resolveWebDir()
		log.Infof("static web listening on: http://127.0.0.1:8080 (dir=%s)", webDir)
		if err := http.ListenAndServe(":8080", newDemoHTTPHandler(webDir)); err != nil {
			log.Error(err)
		}
	}()

	wsSrv := websocket.NewServer(
		websocket.WithAddress(":8800"),
		websocket.WithPath("/ws"),
		websocket.WithHandshakeHandler(handleHandshakeAuth),
		websocket.WithConnectHandler(handleConnect),
		websocket.WithDisconnectHandler(handleDisconnect),
		websocket.WithCodec("json"),
		websocket.WithPayloadType(websocket.PayloadTypeText),
		websocket.WithBusinessHeartbeat(30*time.Second, 15*time.Second),
		websocket.WithBusinessHeartbeatCheckInterval(5*time.Second),
	)
	if wsSrv == nil {
		log.Fatal("websocket server init failed")
	}

	testServer = wsSrv
	websocket.RegisterServerMessageHandler(wsSrv, MessageTypeChat, handleChatMessage)

	app := kratos.New(
		kratos.Name("websocket-example"),
		kratos.Server(wsSrv),
	)
	if err := app.Run(); err != nil {
		log.Error(err)
	}
}

// handleConnect 打印连接与断开事件。
func handleConnect(ctx context.Context, session *websocket.Session) error {
	user := userFromMetadata(session.Metadata())
	fmt.Printf("[%s] connected user=%s vip=%v [%+v]\n", session.SessionID(), user.UserName, user.VIP, session.Queries())
	return sendSystemMessage(session.SessionID(), welcomeText(user))
}

// handleDisconnect 打印断开事件。
func handleDisconnect(ctx context.Context, session *websocket.Session) {
	user := userFromMetadata(session.Metadata())
	fmt.Printf("[%s] disconnect user=%s vip=%v\n", session.SessionID(), user.UserName, user.VIP)
}

// handleChatMessage 收到消息后广播给所有在线连接。
func handleChatMessage(ctx context.Context, session *websocket.Session, message *ChatMessage) error {
	if testServer == nil {
		return errors.New("websocket server is nil")
	}
	if session == nil {
		return errors.New("session not found")
	}

	user := userFromMetadata(session.Metadata())
	text := strings.TrimSpace(message.Message)
	if text == "" {
		return sendSystemMessage(session.SessionID(), "消息不能为空")
	}

	message.Sender = displayUserName(user)
	message.Timestamp = time.Now().Format(time.RFC3339)
	message.Message = text

	fmt.Printf("[%s] user=%s vip=%v payload=%+v\n", session.SessionID(), user.UserName, user.VIP, message)

	if strings.HasPrefix(text, "/vip ") {
		if !user.VIP {
			return sendSystemMessage(session.SessionID(), "你当前是普通用户，不能发送 VIP 专属消息。请先切换为 VIP 身份。")
		}

		vipText := strings.TrimSpace(strings.TrimPrefix(text, "/vip"))
		if vipText == "" {
			return sendSystemMessage(session.SessionID(), "VIP 消息格式：/vip 你的内容")
		}

		message.Message = vipText
		message.VIPOnly = true
		broadcastToVIP(*message)
		return nil
	}

	message.VIPOnly = false
	testServer.Broadcast(MessageTypeChat, *message)
	return nil
}

func newDemoHTTPHandler(webDir string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/login", handleLogin)
	mux.HandleFunc("/api/logout", handleLogout)
	mux.HandleFunc("/api/me", handleCurrentViewer)
	mux.Handle("/", http.FileServer(http.Dir(webDir)))
	return mux
}

func handleLogin(res http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeJSON(res, http.StatusMethodNotAllowed, map[string]any{"message": "method not allowed"})
		return
	}

	var request LoginRequest
	if err := json.NewDecoder(req.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(res, http.StatusBadRequest, map[string]any{"message": "invalid login payload"})
		return
	}

	request.UserName = strings.TrimSpace(request.UserName)
	if request.UserName == "" {
		if request.VIP {
			request.UserName = "vip-demo"
		} else {
			request.UserName = "guest-demo"
		}
	}

	claims := &DemoTokenClaims{
		UserID:   strings.ToLower(strings.ReplaceAll(request.UserName, " ", "-")),
		UserName: request.UserName,
		VIP:      request.VIP,
		ExpireAt: time.Now().Add(demoTokenExpireTime).Unix(),
	}

	token, err := issueDemoToken(claims)
	if err != nil {
		writeJSON(res, http.StatusInternalServerError, map[string]any{"message": "issue token failed"})
		return
	}

	http.SetCookie(res, &http.Cookie{
		Name:     authCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Unix(claims.ExpireAt, 0),
	})

	writeJSON(res, http.StatusOK, ViewerResponse{
		LoggedIn: true,
		UserID:   claims.UserID,
		UserName: claims.UserName,
		VIP:      claims.VIP,
		Role:     roleName(claims.VIP),
	})
}

func handleLogout(res http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeJSON(res, http.StatusMethodNotAllowed, map[string]any{"message": "method not allowed"})
		return
	}

	http.SetCookie(res, &http.Cookie{
		Name:     authCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})

	writeJSON(res, http.StatusOK, map[string]any{"message": "logout success"})
}

func handleCurrentViewer(res http.ResponseWriter, req *http.Request) {
	claims, err := readClaimsFromRequest(req)
	if err != nil {
		writeJSON(res, http.StatusOK, ViewerResponse{LoggedIn: false})
		return
	}

	writeJSON(res, http.StatusOK, ViewerResponse{
		LoggedIn: true,
		UserID:   claims.UserID,
		UserName: claims.UserName,
		VIP:      claims.VIP,
		Role:     roleName(claims.VIP),
	})
}

func handleHandshakeAuth(ctx context.Context, req *http.Request, metadata websocket.SessionMetadata) error {
	claims, err := readClaimsFromRequest(req)
	if err != nil {
		return errors.New("未登录或 token 已失效，请先在示例页面选择登录身份")
	}

	metadata["user_id"] = claims.UserID
	metadata["user_name"] = claims.UserName
	metadata["vip"] = claims.VIP
	metadata["role"] = roleName(claims.VIP)
	return nil
}

func readClaimsFromRequest(req *http.Request) (*DemoTokenClaims, error) {
	if req == nil {
		return nil, errors.New("request is nil")
	}

	cookie, err := req.Cookie(authCookieName)
	if err != nil {
		return nil, err
	}
	return parseDemoToken(cookie.Value)
}

func issueDemoToken(claims *DemoTokenClaims) (string, error) {
	if claims == nil {
		return "", errors.New("claims is nil")
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	payloadText := base64.RawURLEncoding.EncodeToString(payload)
	signatureText := signDemoToken(payloadText)
	return payloadText + "." + signatureText, nil
}

func parseDemoToken(token string) (*DemoTokenClaims, error) {
	if token == "" {
		return nil, errors.New("token is empty")
	}

	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, errors.New("token format is invalid")
	}

	payloadText := parts[0]
	signatureText := parts[1]
	if !hmac.Equal([]byte(signatureText), []byte(signDemoToken(payloadText))) {
		return nil, errors.New("token signature is invalid")
	}

	payload, err := base64.RawURLEncoding.DecodeString(payloadText)
	if err != nil {
		return nil, err
	}

	var claims DemoTokenClaims
	if err = json.Unmarshal(payload, &claims); err != nil {
		return nil, err
	}

	if claims.UserID == "" || claims.UserName == "" {
		return nil, errors.New("token claims is invalid")
	}
	if claims.ExpireAt <= time.Now().Unix() {
		return nil, errors.New("token is expired")
	}

	return &claims, nil
}

func signDemoToken(payloadText string) string {
	mac := hmac.New(sha256.New, []byte(demoTokenSecret))
	_, _ = mac.Write([]byte(payloadText))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func userFromMetadata(metadata websocket.SessionMetadata) SessionUser {
	if metadata == nil {
		return SessionUser{}
	}

	return SessionUser{
		UserID:   metadataString(metadata, "user_id"),
		UserName: metadataString(metadata, "user_name"),
		VIP:      metadataBool(metadata, "vip"),
	}
}

func roleName(vip bool) string {
	if vip {
		return "vip"
	}
	return "guest"
}

func displayUserName(user SessionUser) string {
	if user.UserName == "" {
		return "anonymous"
	}
	if user.VIP {
		return "[VIP] " + user.UserName
	}
	return user.UserName
}

func welcomeText(user SessionUser) string {
	if user.VIP {
		return fmt.Sprintf("欢迎你，%s。当前身份是 VIP，可发送公共消息，也可使用 /vip 内容 给 VIP 用户发专属消息。", user.UserName)
	}
	return fmt.Sprintf("欢迎你，%s。当前身份是普通用户，只能发送公共消息。", user.UserName)
}

func sendSystemMessage(sessionID websocket.SessionID, message string) error {
	if testServer == nil {
		return errors.New("websocket server is nil")
	}

	return testServer.SendMessage(sessionID, MessageTypeSystem, &SystemMessage{
		Timestamp: time.Now().Format(time.RFC3339),
		Message:   message,
	})
}

func broadcastToVIP(message ChatMessage) {
	if testServer == nil {
		return
	}

	testServer.RangeSessions(func(sessionID websocket.SessionID, session *websocket.Session) bool {
		if session == nil {
			return true
		}
		if userFromMetadata(session.Metadata()).VIP {
			_ = testServer.SendMessage(sessionID, MessageTypeChat, message)
		}
		return true
	})
}

func metadataString(metadata websocket.SessionMetadata, key string) string {
	if metadata == nil {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

func metadataBool(metadata websocket.SessionMetadata, key string) bool {
	if metadata == nil {
		return false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return false
	}
	flag, ok := value.(bool)
	return ok && flag
}

func writeJSON(res http.ResponseWriter, statusCode int, payload any) {
	res.Header().Set("Content-Type", "application/json; charset=utf-8")
	res.WriteHeader(statusCode)
	_ = json.NewEncoder(res).Encode(payload)
}

// resolveWebDir 返回示例页面目录。
// 这里使用源码文件所在目录做基准，避免在仓库根目录执行 `go run ./_example/server/websocket` 时出现 404。
func resolveWebDir() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "./web"
	}
	return filepath.Join(filepath.Dir(filename), "web")
}
