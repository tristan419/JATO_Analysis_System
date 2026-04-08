# Fullstack 本地一键启动与联调

## 1. 一键启动（重启模式）

在项目根目录执行：

```bash
bash 03_Scripts/fullstack_dev.sh start
```

脚本会自动：

1. 先停止已存在的 FastAPI 后端和 Vite 前端
2. 再重新启动 FastAPI 后端和 Vite 前端
3. 自动探测端口冲突并切换空闲端口
4. 输出运行地址与日志路径

如果你只是想查看状态或只做烟测，可以继续使用 `status` / `test` / `stop` / `restart`。

## 2. 常用命令

```bash
# 查看状态
bash 03_Scripts/fullstack_dev.sh status

# 联调烟测
bash 03_Scripts/fullstack_dev.sh test

# 重启
bash 03_Scripts/fullstack_dev.sh restart

# 停止
bash 03_Scripts/fullstack_dev.sh stop
```

## 3. 权限相关（已启用）

默认启用 token + role：

- Header `X-Auth-Token`
- Header `X-User-Role`（viewer/editor/admin）
- Header `X-User-Name`

脚本默认环境变量：

- `APP_AUTH_ENABLED=true`
- `APP_AUTH_TOKEN=change-me`
- `APP_USER_ROLE=admin`
- `APP_USER_NAME=local-dev`

可按需覆盖：

```bash
APP_AUTH_TOKEN=my-token APP_USER_ROLE=viewer bash 03_Scripts/fullstack_dev.sh start
```

## 4. 联调检查清单

1. 健康检查
   - `GET /healthz` 返回 `{ "status": "ok" }`
2. 元数据
   - `GET /v1/metadata/columns` 可返回列名列表
3. 看板概览
   - `POST /v1/analysis/overview` 返回 route + kpis + monthSeries/yearSeries
4. 明细
   - `POST /v1/analysis/detail` 返回 page/pageSize/total/items
5. CRUD
   - `GET /v1/crud/items?page=1&page_size=20` 返回分页结构

## 5. 前端联调建议

1. 打开脚本输出的前端地址
2. 在页面顶部 `Access Control` 面板保存 token/role/name
3. 先跑 Dashboard 的 `Load Overview`
4. 再跑 `Load Detail` 和 `Export Detail CSV`
5. 到 CRUD 页面验证搜索、排序、分页、创建、删除

## 6. 常见问题

1. 端口被占用
   - 脚本会自动切换到下一个空闲端口
2. 401 Unauthorized
   - 检查前端 Access Control 的 token 与后端 `APP_AUTH_TOKEN` 是否一致
3. 403 Forbidden
   - 检查 role 是否满足接口要求（CRUD 写操作需要 editor 及以上）
4. 无法查看日志
   - 后端日志：`06_AppPlatform/.runtime/logs/backend.log`
   - 前端日志：`06_AppPlatform/.runtime/logs/frontend.log`
