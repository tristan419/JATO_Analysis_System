# Presence + WebSocket 实时在线协作 PRD

> 目标：在不引入额外基础设施的前提下，为 JATO 平台增加实时在线感知与数据变更推送能力。

---

## 1. 背景与范围

### 1.1 当前状态

JATO 平台所有数据交互均为请求-响应模式（REST API）。多用户同时操作同一页面时：

- 无法感知其他用户的在线状态
- 数据变更后需手动刷新才能看到他人修改
- 多人编辑同一行时存在静默覆盖风险

### 1.2 目标

分两个阶段：

| 阶段 | 功能 | 交付物 |
|------|------|--------|
| Phase 1 — Presence | 在线感知：谁在线、在看哪个页面、最后活跃时间 | REST + 心跳 |
| Phase 2 — WebSocket | 实时推送：在线上线通知、数据变更广播、编辑锁提示 | WebSocket |

### 1.3 不做的事

- **不实现 OT/CRDT 协同编辑** — 复杂度远超当前需求，保留乐观锁方案
- **不引入 Redis/RabbitMQ/Kafka** — 全部使用 FastAPI 原生能力
- **不改造现有数据库** — Presence 数据纯内存存储

---

## 2. Phase 1 — Presence 在线感知

### 2.1 架构

```
Browser ── POST /v1/presence/heartbeat ──→ FastAPI (内存 dict)
                 每 30 秒                        │
                                                 ├─ 清理过期 session
                                                 └─ 返回当前在线列表
```

### 2.2 后端 API

#### `POST /v1/presence/heartbeat`

请求：
```json
{
  "session_id": "uuid-from-localstorage",
  "user_name": "Tristan",
  "current_page": "engineering",
  "page_entity_id": "project-42"
}
```

响应：
```json
{
  "online": 5,
  "users": [
    {"user_name": "Tristan", "current_page": "engineering", "last_seen_ago_s": 2},
    {"user_name": "Lisa",    "current_page": "market-scan",  "last_seen_ago_s": 15}
  ]
}
```

#### `GET /v1/presence/online`

轻量查询，不更新心跳：
```json
{
  "online": 3,
  "same_page": 2,
  "users": [...]
}
```

### 2.3 后端实现

**文件**: `06_AppPlatform/backend/app/services/presence_service.py`

```python
import time
import threading
from dataclasses import dataclass, field

@dataclass
class Session:
    session_id: str
    user_name: str
    current_page: str
    page_entity_id: str | None
    last_seen: float = field(default_factory=time.time)

class PresenceStore:
    """In-memory session tracker. Thread-safe via lock."""
    
    def __init__(self, ttl_seconds: int = 120):
        self._sessions: dict[str, Session] = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
    
    def heartbeat(self, session_id: str, user_name: str, 
                  current_page: str, page_entity_id: str | None = None) -> dict:
        with self._lock:
            self._cleanup()
            session = Session(session_id, user_name, current_page, page_entity_id)
            self._sessions[session_id] = session
            return self._snapshot()
    
    def get_online(self, current_page: str | None = None) -> dict:
        with self._lock:
            self._cleanup()
            return self._snapshot(current_page)
    
    def _cleanup(self):
        now = time.time()
        expired = [sid for sid, s in self._sessions.items() 
                   if now - s.last_seen > self._ttl]
        for sid in expired:
            del self._sessions[sid]
    
    def _snapshot(self, current_page: str | None = None) -> dict:
        now = time.time()
        users = [
            {"user_name": s.user_name, "current_page": s.current_page,
             "last_seen_ago_s": int(now - s.last_seen)}
            for s in self._sessions.values()
        ]
        same_page = sum(1 for u in users if u["current_page"] == current_page) if current_page else 0
        return {"online": len(users), "same_page": same_page, "users": users}

# Singleton — survives between requests, dies on process restart
presence_store = PresenceStore(ttl_seconds=120)
```

**路由**: `06_AppPlatform/backend/app/api/routes/presence.py`

```python
from fastapi import APIRouter
from app.services.presence_service import presence_store

router = APIRouter(prefix="/v1/presence")

@router.post("/heartbeat")
def heartbeat(payload: dict):
    return presence_store.heartbeat(
        session_id=payload["session_id"],
        user_name=payload.get("user_name", "anonymous"),
        current_page=payload.get("current_page", "unknown"),
        page_entity_id=payload.get("page_entity_id"),
    )

@router.get("/online")
def online(page: str | None = None):
    return presence_store.get_online(current_page=page)
```

### 2.4 前端实现

**文件**: `06_AppPlatform/frontend/src/hooks/usePresence.ts`

```ts
// 首次加载生成 session_id 存 localStorage
// 每 30s 发 POST /v1/presence/heartbeat
// 返回 { online, samePage, users } 
// 页面卸载时清理
```

**UI**: 全局右下角或顶部导航栏显示：

```
🟢 4 人在线  ·  2 人正在看本页
```

### 2.5 生命周期

```
页面打开 → 生成 session_id → 立即 heartbeat → 每 30s 循环
页面关闭 → beforeunload 不发（TTL 自然过期）
120s 无心跳 → 自动清理
```

---

## 3. Phase 2 — WebSocket 实时推送

### 3.1 架构

```
Browser A ── WebSocket ──┐
Browser B ── WebSocket ──┼── FastAPI (in-memory pub/sub) 
Browser C ── WebSocket ──┘         │
                                   ├─ 用户上线/下线广播
                                   ├─ "正在编辑" 锁定通知
                                   └─ 数据变更通知
```

### 3.2 WebSocket 端点

`ws://127.0.0.1:8000/v1/ws?session_id=xxx&user_name=Tristan`

**消息协议** (JSON):

```json
// 服务端 → 客户端
{"type": "presence_join",     "user": "Lisa", "page": "engineering"}
{"type": "presence_leave",    "user": "Lisa"}
{"type": "edit_lock",         "entity": "variant-42", "user": "Tristan"}
{"type": "edit_release",      "entity": "variant-42"}
{"type": "data_changed",      "resource": "variant", "id": "variant-42", "by": "Lisa"}
{"type": "data_changed",      "resource": "project",  "id": "project-42", "action": "import_complete"}

// 客户端 → 服务端
{"type": "subscribe",         "topics": ["project-42"]}
{"type": "edit_start",        "entity": "variant-42"}
{"type": "edit_end",          "entity": "variant-42"}
{"type": "ping"}
```

### 3.3 后端实现

**文件**: `06_AppPlatform/backend/app/services/ws_service.py`

```python
import asyncio
import json
import time
from fastapi import WebSocket

class ConnectionManager:
    """Manages active WebSocket connections with topic-based pub/sub."""
    
    def __init__(self):
        # {session_id: {"ws": WebSocket, "user_name": str, "topics": set, "last_ping": float}}
        self._connections: dict[str, dict] = {}
        self._topic_subscribers: dict[str, set[str]] = {}  # topic → session_ids
    
    async def connect(self, ws: WebSocket, session_id: str, user_name: str):
        await ws.accept()
        self._connections[session_id] = {
            "ws": ws, "user_name": user_name,
            "topics": set(), "last_ping": time.time(),
        }
        await self._broadcast_presence("presence_join", session_id, user_name)
    
    async def disconnect(self, session_id: str):
        if session_id not in self._connections:
            return
        user_name = self._connections[session_id]["user_name"]
        # Unsubscribe from all topics
        for topic in self._connections[session_id]["topics"]:
            self._topic_subscribers.get(topic, set()).discard(session_id)
        del self._connections[session_id]
        await self._broadcast_presence("presence_leave", session_id, user_name)
    
    async def subscribe(self, session_id: str, topics: list[str]):
        conn = self._connections.get(session_id)
        if not conn: return
        for t in topics:
            conn["topics"].add(t)
            self._topic_subscribers.setdefault(t, set()).add(session_id)
    
    async def publish(self, topic: str, message: dict):
        """Push to all subscribers of a topic."""
        subs = self._topic_subscribers.get(topic, set())
        dead = []
        for sid in subs:
            conn = self._connections.get(sid)
            if conn is None: dead.append(sid); continue
            try:
                await conn["ws"].send_json(message)
            except Exception:
                dead.append(sid)
        for sid in dead:
            subs.discard(sid)
    
    async def _broadcast_presence(self, event_type: str, sid: str, name: str):
        msg = {"type": event_type, "user": name, "timestamp": time.time()}
        # Send to all connections except sender
        for other_sid, conn in self._connections.items():
            if other_sid == sid: continue
            try:
                await conn["ws"].send_json(msg)
            except Exception:
                pass

manager = ConnectionManager()
```

### 3.4 数据变更钩子

在现有 REST API 的写操作完成后，推送通知到 WebSocket 订阅者：

```python
# 在 PATCH /v1/engineering/projects/{id}/variants/{vid} 的最后：
from app.services.ws_service import manager

await manager.publish(
    topic=f"project-{project_id}",
    message={
        "type": "data_changed",
        "resource": "variant",
        "id": variant_id,
        "by": request.state.user_name,
        "timestamp": time.time(),
    }
)
```

### 3.5 前端实现

**文件**: `06_AppPlatform/frontend/src/hooks/useWebSocket.ts`

```ts
function useWebSocket(sessionId: string, userName: string) {
  const [presence, setPresence] = useState<User[]>([]);
  const [editLocks, setEditLocks] = useState<Map<string, string>>(new Map());
  const wsRef = useRef<WebSocket | null>(null);
  
  useEffect(() => {
    const ws = new WebSocket(`ws://${host}/v1/ws?session_id=${sessionId}&user_name=${userName}`);
    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      switch (msg.type) {
        case "presence_join":  setPresence(prev => [...prev, {name: msg.user}]); break;
        case "presence_leave": setPresence(prev => prev.filter(u => u.name !== msg.user)); break;
        case "edit_lock":      setEditLocks(prev => new Map(prev).set(msg.entity, msg.user)); break;
        case "edit_release":   setEditLocks(prev => { const m = new Map(prev); m.delete(msg.entity); return m; }); break;
        case "data_changed":   onDataChanged?.(msg); break;
      }
    };
    // Ping every 25s to keep alive
    const ping = setInterval(() => ws.send(JSON.stringify({type: "ping"})), 25000);
    return () => { clearInterval(ping); ws.close(); };
  }, [sessionId, userName]);
  
  return { presence, editLocks };
}
```

### 3.6 降级策略

```
尝试 WebSocket 连接
  ├─ 成功 → WS 实时推送
  └─ 失败 → 回退到 POST /v1/presence/heartbeat 30s 轮询
              + GET /v1/presence/online 定时拉取
```

前端自动检测：`new WebSocket(url)` 失败 → 切换到 polling 模式。

---

## 4. 已知缺陷 & 缓解方案

### 4.1 进程重启丢数据 ⚠️ 中风险

**问题**：`PresenceStore` 是进程内 `dict`。uvicorn 重启（部署、crash）后所有在线状态清零。多 worker 模式下（`--workers 4`）各 worker 独立维护，互相看不到。

**缓解**：当前部署为单 worker（未指定 `--workers`），短期无影响。长期显式约束 `--workers 1`，或切换 Redis `SETEX`。

### 4.2 多 Tab 虚高 ⚠️ 中风险

**问题**：同一用户 5 个 Tab = 5 个连接 = 5 个"在线用户"。刷新页面也会短暂翻倍。

**缓解**：在线统计按 `user_name` 去重，只显示 "4 用户在线" 而非 "4 连接"。

### 4.3 僵尸连接泄漏 ⚠️ 高风险

**问题**：客户端网络断开（合盖、切 Wi-Fi），TCP 连接可能残留数小时，`_connections` dict 无限增长。

**缓解**：服务端每 30s 主动 ping → 10s 无 pong 则断开并清理。在 `ConnectionManager` 中加入 `_cleanup_task` 后台协程。

### 4.4 编辑锁竞态 ⚠️ 中风险

**问题**：A 和 B 同时点"编辑"，各自前端乐观确认。WS 广播延迟 ~100ms，双方都以为拿到了锁。

**缓解**：锁仲裁不放在 WS，放在 REST endpoint：
```python
POST /v1/engineering/variants/{id}/lock
# DB 原子操作: UPDATE SET locked_by=$user WHERE locked_by IS NULL
# 先到先得，WS 只推送结果通知
```

### 4.5 离线消息丢失 ✅ 可接受

**问题**：离线期间 `data_changed` 消息直接丢弃，重连后不回溯。

**缓解**：页面重新激活时（`visibilitychange`）自动 fetch 最新数据。编辑提交时乐观锁 `version` 字段兜底。

### 4.6 写入端点污染 ⚠️ 低风险

**问题**：每个 PATCH/POST/DELETE handler 末尾都要手动加 `await manager.publish(...)`，容易遗漏。

**缓解**：优先为核心页面（Engineering、Review）添加。后续可用 FastAPI middleware 自动拦截 `2xx` 写操作统一推送。

### 4.7 Token 泄露 ⚠️ 低风险

**问题**：WebSocket 不支持自定义 header（浏览器限制），token 在 query param 明文 → 暴露在 nginx/uvicorn 日志。

**缓解**：首条消息认证：连接建立后 5s 内发送 `{"type":"auth","token":"xxx"}`，超时未认证则断开。token 不出现在 URL。

### 4.8 推送打断编辑 ⚠️ 低风险

**问题**：用户正在编辑表单时收到 `data_changed`，自动刷新表格会打断编辑。

**缓解**：收到推送时不强制刷新，仅在行旁显示 "🔄 数据已更新" 提示。用户空闲时再刷新。

---

## 5. 安全

| 层面 | 方案 |
|------|------|
| 认证 | WebSocket 连接时验证 query param `token`，与 REST API 共用同一 token |
| 授权 | WebSocket 消息不走业务逻辑，只做通知；真正的写操作仍走 REST + role 校验 |
| 限流 | 单 IP 最多 3 个 WebSocket 连接；心跳超 30s 未响应的连接主动断开 |

---

## 5. 部署影响

| 维度 | 影响 |
|------|------|
| **新依赖** | 无（FastAPI 原生 `WebSocket` + Python `asyncio`） |
| **内存** | 100 并发连接 ≈ 50MB（每个连接 ≈ 0.5MB） |
| **CPU** | 心跳处理 ≈ 0.1% CPU |
| **数据库** | 无改动 |
| **nginx** | 需要配置 WebSocket 代理（`proxy_set_header Upgrade $http_upgrade`） |
| **CI/CD** | `systemd` 的 uvicorn 已支持 WebSocket，无需改启动命令 |

### nginx 配置（腾讯云）

```nginx
location /v1/ws {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 86400s;  # 24h，不自动断开
}
```

---

## 6. 测试计划

### Phase 1 — Presence

| 测试 | 方法 |
|------|------|
| heartbeat 创建 session | `curl -X POST /v1/presence/heartbeat` → 返回 `online: 1` |
| TTL 过期清理 | 心跳停止 130s 后 `GET /v1/presence/online` → `online: 0` |
| same_page 统计 | 两个 session 同 page → `same_page: 2` |

### Phase 2 — WebSocket

| 测试 | 方法 |
|------|------|
| 连接建立 | `wscat -c ws://127.0.0.1:8000/v1/ws?session_id=t1` |
| 上线广播 | A 连接后 B 连接 → A 收到 `presence_join` |
| 断线广播 | B 断开 → A 收到 `presence_leave` |
| 话题推送 | A subscribe `project-42`，B publish → A 收到 |
| 降级回退 | 关掉 WS → 前端自动切 polling |

---

## 7. 文件清单

```
新增文件：
  backend/app/services/presence_service.py      (~50 行)
  backend/app/services/ws_service.py            (~80 行)
  backend/app/api/routes/presence.py            (~25 行)
  backend/app/api/routes/ws.py                  (~40 行)
  frontend/src/hooks/usePresence.ts             (~60 行)
  frontend/src/hooks/useWebSocket.ts            (~80 行)
  frontend/src/components/OnlineUsersBar.tsx    (~50 行)

修改文件：
  backend/app/main.py                           (+2 行，注册 router)
  frontend/src/App.tsx                          (+5 行，Provider 挂载)
  frontend/src/pages/EngineeringPage.tsx        (+10 行，编辑锁提示)
  nginx 配置                                    (+7 行，WS 代理)
```

---

## 8. 实施顺序

```
Phase 1 (2h)：
  1. presence_service.py + routes/presence.py
  2. usePresence.ts
  3. OnlineUsersBar.tsx → 挂到全局导航栏

Phase 2 (4h)：
  1. ws_service.py + routes/ws.py  
  2. useWebSocket.ts
  3. 降级逻辑（WS → polling fallback）
  4. 数据变更钩子接入 Engineering PATCH endpoint
  5. 编辑锁 UI 接入 EngineeringPage
```
