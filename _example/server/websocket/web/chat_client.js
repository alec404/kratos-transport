const messages = document.getElementById("messages");
const form = document.getElementById("msgForm");
const inputBox = document.getElementById("inputBox");
const statusBar = document.getElementById("statusBar");
const loginGuestBtn = document.getElementById("loginGuestBtn");
const loginVipBtn = document.getElementById("loginVipBtn");
const logoutBtn = document.getElementById("logoutBtn");

const MESSAGE_TYPE_CHAT = 1;
const MESSAGE_TYPE_SYSTEM = 2;
const HEARTBEAT_PING = [0xB0, 0x04];

let ws;
let currentViewer = { loggedIn: false };

function showMessage(message, extraClass = "") {
    const msgDiv = document.createElement("div");
    msgDiv.classList.add("msgCtn");
    if (extraClass) {
        msgDiv.classList.add(extraClass);
    }
    msgDiv.innerHTML = message;
    messages.appendChild(msgDiv);
    messages.scrollTop = messages.scrollHeight;
}

function showSystemMessage(message) {
    showMessage(message, "msgSystem");
}

function updateViewer(viewer) {
    currentViewer = viewer || { loggedIn: false };
    if (currentViewer.loggedIn) {
        statusBar.textContent = currentViewer.vip
            ? `当前身份：VIP 用户 ${currentViewer.userName}`
            : `当前身份：普通用户 ${currentViewer.userName}`;
    } else {
        statusBar.textContent = "当前未登录，请先选择身份。";
    }
}

form.addEventListener("submit", (event) => {
    event.preventDefault();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        showSystemMessage("WebSocket 尚未连接，请先登录并等待连接建立。");
        return;
    }

    const message = inputBox.value;
    if (!message) {
        return;
    }
    sendChatMessage(message);
    inputBox.value = "";
});

function sendChatMessage(message) {
    const packet = {
        message: message,
        sender: "browser",
        timestamp: new Date().toISOString(),
    };
    sendMessage(1, packet);
}

function sendMessage(id, payload) {
    ws.send(JSON.stringify({
        type: id,
        payload: JSON.stringify(payload),
    }));
}

function isHeartbeatPing(buffer) {
    const uint8 = new Uint8Array(buffer);
    return uint8.length === HEARTBEAT_PING.length &&
        uint8[0] === HEARTBEAT_PING[0] &&
        uint8[1] === HEARTBEAT_PING[1];
}

function handleMessage(messageType, messagePayload) {
    const packet = JSON.parse(messagePayload);

    if (messageType === MESSAGE_TYPE_SYSTEM) {
        showSystemMessage(`[${packet.timestamp || ""}] ${packet.message}`);
        return;
    }

    if (messageType !== MESSAGE_TYPE_CHAT) {
        showSystemMessage(`收到未知消息类型：${messageType}`);
        return;
    }

    const prefix = packet.vipOnly ? "[VIP频道] " : "";
    const className = packet.vipOnly ? "msgVip" : "";
    showMessage(`${prefix}[${packet.timestamp || ""}] ${packet.sender || "anonymous"}: ${packet.message}`, className);
}

async function requestJSON(url, options = {}) {
    const response = await fetch(url, {
        credentials: "include",
        headers: {
            "Content-Type": "application/json",
            ...(options.headers || {}),
        },
        ...options,
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        throw new Error(data.message || `HTTP ${response.status}`);
    }
    return data;
}

function createWebsocketClient() {
    if (!currentViewer.loggedIn) {
        showSystemMessage("请先登录，然后再建立 WebSocket 连接。");
        return;
    }

    if (ws) {
        ws.onerror = ws.onopen = ws.onclose = null;
        ws.close();
    }

    ws = new WebSocket(`ws://${window.location.hostname}:8800/ws`);
    ws.binaryType = "arraybuffer";

    ws.onerror = function () {
        showSystemMessage("WebSocket 连接失败，请确认当前 Cookie 已登录。");
    };

    ws.onopen = () => {
        showSystemMessage("WebSocket 连接已建立。服务端会发送 2B binary 心跳，客户端自动回复 0B binary pong。");
    };

    ws.onclose = () => {
        showSystemMessage("WebSocket 连接已关闭。");
        ws = null;
    };

    ws.onmessage = function (event) {
        if (event.data instanceof ArrayBuffer && isHeartbeatPing(event.data)) {
            ws.send(new Uint8Array(0));
            return;
        }

        if (typeof event.data !== "string") {
            showSystemMessage("收到无法识别的 WebSocket 帧。");
            return;
        }

        const packet = JSON.parse(event.data);
        handleMessage(packet.type, packet.payload);
    };
}

async function loadCurrentViewer() {
    const viewer = await requestJSON("/api/me").catch(() => ({ loggedIn: false }));
    updateViewer(viewer);
    return viewer;
}

async function login(vip) {
    const viewer = await requestJSON("/api/login", {
        method: "POST",
        body: JSON.stringify({
            userName: vip ? "vip-demo" : "guest-demo",
            vip: vip,
        }),
    });

    updateViewer(viewer);
    showSystemMessage(`登录成功，当前身份：${viewer.vip ? "VIP 用户" : "普通用户"} ${viewer.userName}`);
    createWebsocketClient();
}

async function logout() {
    await requestJSON("/api/logout", {
        method: "POST",
    });

    updateViewer({ loggedIn: false });
    if (ws) {
        ws.close();
    }
    showSystemMessage("已退出登录。");
}

loginGuestBtn.addEventListener("click", () => login(false));
loginVipBtn.addEventListener("click", () => login(true));
logoutBtn.addEventListener("click", () => logout());

(async function bootstrap() {
    const viewer = await loadCurrentViewer();
    if (viewer.loggedIn) {
        createWebsocketClient();
    } else {
        showSystemMessage("请先登录后再建立 WebSocket 连接。");
    }
})();
