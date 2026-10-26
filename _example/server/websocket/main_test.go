package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveWebDirServesIndex(t *testing.T) {
	handler := http.FileServer(http.Dir(resolveWebDir()))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d, body=%s", resp.Code, resp.Body.String())
	}
}

func TestDemoTokenRoundTrip(t *testing.T) {
	token, err := issueDemoToken(&DemoTokenClaims{
		UserID:   "vip-demo",
		UserName: "vip-demo",
		VIP:      true,
		ExpireAt: 4102444800,
	})
	if err != nil {
		t.Fatalf("issue token failed: %v", err)
	}

	claims, err := parseDemoToken(token)
	if err != nil {
		t.Fatalf("parse token failed: %v", err)
	}

	if claims.UserID != "vip-demo" || claims.UserName != "vip-demo" || !claims.VIP {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestDemoLoginAndCurrentViewer(t *testing.T) {
	handler := newDemoHTTPHandler(resolveWebDir())

	loginBody, _ := json.Marshal(LoginRequest{
		UserName: "vip-demo",
		VIP:      true,
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewReader(loginBody))
	loginResp := httptest.NewRecorder()
	handler.ServeHTTP(loginResp, loginReq)

	if loginResp.Code != http.StatusOK {
		t.Fatalf("expected login status 200, got %d body=%s", loginResp.Code, loginResp.Body.String())
	}

	result := loginResp.Result()
	defer result.Body.Close()

	meReq := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	for _, cookie := range result.Cookies() {
		meReq.AddCookie(cookie)
	}
	meResp := httptest.NewRecorder()
	handler.ServeHTTP(meResp, meReq)

	if meResp.Code != http.StatusOK {
		t.Fatalf("expected me status 200, got %d body=%s", meResp.Code, meResp.Body.String())
	}

	var viewer ViewerResponse
	if err := json.Unmarshal(meResp.Body.Bytes(), &viewer); err != nil {
		t.Fatalf("decode viewer failed: %v", err)
	}

	if !viewer.LoggedIn || !viewer.VIP || viewer.UserName != "vip-demo" {
		t.Fatalf("unexpected viewer info: %+v", viewer)
	}
}
