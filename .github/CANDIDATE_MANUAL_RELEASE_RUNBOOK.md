# 固定 Active / Candidate V2 人工操作手册

这份手册是 JATO/Ojeur 腾讯云代码发布的用户操作入口。忘记流程时，先让
Codex 读取本文件并报告当前状态，再决定是否执行下一步。

> 本手册只描述应用代码发布。JATO 月更数据的上传、Review、批准和 Publish
> 是另一套授权，不能由任何代码发布操作代替。

## 1. 不变原则

```text
main 上的不可变 release
          |
          | 用户手动启动 prepare-candidate
          v
Candidate：8001 + 127.0.0.1:18002 人工预览
          + candidate.ojeur.cloud 固定国内测试地址
          |
          +-- 不满意：discard-candidate，www Active 不变
          |
          +-- 满意：用户明确授权 update-active
                         |
                         v
                   Active：8000，仍承接 www 公网
                         |
                         +-- 如需主动回退：rollback-active

www Active -- 用户另行启动 sync-www-active-to-intl --> intl
```

- Active 固定为端口 `8000`，始终承接 www 正式公网流量。
- Candidate 固定为端口 `8001`，只用于腾讯云真实环境的人工页面测试。
- Candidate 预览固定监听 `127.0.0.1:18002`；独立的受认证 HTTPS 网关可以把
  `candidate.ojeur.cloud` 转发到该 loopback 入口，但不得开放 18002 本身。
- Active 与 Candidate 的角色和端口不互换；Nginx 正式公网上游始终是 `8000`。
- 合并代码到 `main` 不会自动部署。只有用户从 `main` 手动 dispatch
  `prepare-candidate`，才会构建并部署 Candidate。
- Candidate 可以反复被新的 `main` release 替换。Candidate 不合格时，Active
  可以长期停留在旧稳定版本。
- Candidate 通过测试也不会自动更新 Active；必须由用户对精确 release 明确授权
  `update-active`。
- Active 使用的必须是 Candidate 已测试的同一个不可变 release，不重新构建、
  重新上传或重新组装。
- intl 完全独立。V2 不 dispatch、等待或回退 intl 的同步/部署；既有只读 observer
  即使被独立触发，也不参与 www 发布的成败判定。

## 2. V2 只有四个操作

| 操作 | 作用 | 不允许发生的事 |
| --- | --- | --- |
| `prepare-candidate` | 从手动 dispatch 的当前 `main` 构建、校验并启动固定 Candidate，等待人工测试。 | 不更新 Active，不改变 www 路由，不同步 intl。 |
| `discard-candidate` | 停止 Candidate、撤销预览并清理未被保护指针引用的 Candidate 文件。 | 不更新或重启 Active，不改变 www，不同步 intl。 |
| `update-active` | 将固定 Active 指向人工验收过的同一 release，重启 8000 并验证公网。 | 不取“最新 main”代替已验收 release，不重新构建，不同步 intl。 |
| `rollback-active` | 将固定 Active 回到用户明确指定、且仍受 current/previous 指针保护的 release，重启 8000 并验证公网。 | 不隐式猜测 previous，不临时重建旧包，不改变 Candidate 或 intl。 |

不得为日常发布增加第五个操作。每次用户授权只覆盖所选的一项，成功后是否继续
下一项必须重新确认。

## 3. 固定运行拓扑

| 角色 | 服务 | 资源限制 | 网络入口 | 后台任务 |
| --- | --- | --- | --- | --- |
| Active | `jato-fullstack-backend@8000.service` | `MemoryHigh=6G`、`MemoryMax=8G` | www 公网固定代理到 8000 | 仅 Active 可以运行月更 worker、scheduler 等单实例任务 |
| Candidate | `jato-fullstack-backend@8001.service` | `MemoryHigh=3G`、`MemoryMax=4G` | 经固定 Candidate HTTPS 网关或本机 SSH 隧道访问 18002 | 必须全部禁用 |
| Candidate Preview | `jato-candidate-preview.service` | 独立受限 | 只监听 `127.0.0.1:18002`；公网网关只反代此端口 | 不执行后台任务 |

运行槽始终只有固定的 8000/8001。`prepare-candidate` 可以使用一个无监听、无指针、
完成后即销毁的 3G/4G transient dependency-build scope；它只是资源受限的依赖构建
进程，不是第三个运行槽，也不形成新的 Candidate 身份。

Candidate 可以读取真实生产环境的数据，以发现本地数据无法复现的问题，但必须使用
独立的 PostgreSQL 只读账号：

- Candidate 与 Active 不得共用数据库账号。
- Candidate 账号只允许连接、schema usage 和查询，不得拥有表写权限、sequence
  更新权限或数据库/schema create 权限。
- Candidate 连接强制只读 transaction，并设置有界 statement/lock timeout。
- Candidate 必须显式禁用 JATO 月更、scheduler、Hermes 和预热等后台任务。
- JATO 月更及依赖本地生产数据目录或 PostgreSQL 写权限的操作必须拒绝执行。Candidate
  目前不是针对所有外部系统副作用的通用沙箱；人工测试不得点击 Airflow 等外部管理写
  操作。若未来要求覆盖这类接口，应单独设计网络/权限隔离，不能把它伪装成本 V2 已有保证。

页面人工验收应以登录、查询、筛选、Dashboard、Market Scan、接口兼容、真实性能和
错误报告为主。若确实需要验证生产写入，不应临时给 Candidate 写权限，而应另用隔离的
数据库副本。

## 4. 从 GitHub 手动运行

统一入口：GitHub Actions 中的 `production-release` workflow。运行时必须选择
`main`，再选择以下四种 `release_mode` 之一。

### 4.0 服务器一次性前提

首次 V2 Candidate 前，需要一次性只读盘点并由用户另行授权安装基础合同：

- Nginx 必须有效加载固定 `8000/current` 的 Active 配置；安装前后 www 的 legacy
  commit/frontend identity 必须完全不变。
- 旧 `jato-bluegreen-boot-reconcile` 不得再在开机时改写正式路由。
- Candidate 必须已有 root-owned 0600 的独立 PostgreSQL SELECT-only env。
- `candidate.ojeur.cloud` 使用 DNSPod A 记录直达上海腾讯云；必须使用独立 TLS 证书和
  独立 Nginx vhost，并在所有页面/API 前启用 Basic Auth。密码文件不得进入仓库，建议
  使用 `root:www-data`、`0640` 的 `/etc/nginx/jato-candidate.htpasswd`。
- Candidate 公网 vhost 只能反代 `127.0.0.1:18002`，不得包含 8000、Active include、
  www/intl 域名或任何 Active fallback。Candidate 不存在时应返回 5xx。
- 一次性安装必须分两阶段，不能直接启用引用尚不存在证书的 HTTPS 模板：先建立 DNS，
  为 Candidate 准备临时 HTTP-only server block，让现有 Certbot Nginx authenticator 签发
  独立证书；再生成 Basic Auth、渲染最终模板、执行 `nginx -t`，通过后才原子替换临时
  配置并 reload。任一步失败都保留现有 www vhost，不重启 8000/8001。
- 四个 slot/release/shared 目录、固定 active-slot=8000 和单一生产锁路径必须可信。
- Candidate/Preview systemd 与 Nginx 合同视为基础设施合同；普通代码发布不自动覆盖
  线上漂移。合同需要升级时先单独审查和安装，不在 Candidate 启动中猜测覆盖。
- 生产调用面只允许 `fixed-v2`；`legacy-v1` 直接入口必须拒绝。旧源码在阶段 B 删除前
  只能作为不可达历史代码保留。

legacy Active 第一次进入 V2 采用用户明确选择的最简 `B/B`：先按普通流程准备并人工测试
Candidate B；只有用户再次明确批准 `update-active` 后，现有控制器才把固定 8000 切到同一
构件，并登记 `active.current == active.previous == B`。不建立 A/A helper、第五种操作、
checkpoint 或 recovery 系统，也不把没有 Candidate 只读合同的旧代码假装成 Candidate。

首次切换会先证明 legacy current 精确指向旧根目录、previous 为空、8000 unit/env 可读取、
固定 Nginx 仍只指向 8000，并记录旧 unit/env、systemd 身份和公网 build identity。普通可捕获
失败会恢复这些运行 preimage 并重新验证 legacy 公网；成功后旧 legacy 不再是自动回退点。
因此第一次成功后的 B/B 没有 distinct rollback，直到下一次 C 更新形成 C/B 后，才可使用
普通 `rollback-active` 回到 B。主机断电、内核崩溃或 SIGKILL 造成的多文件中间态不由日常
控制器自动恢复，必须先人工盘点；这项取舍用于避免重新建设事故恢复平台。

### 4.1 准备 Candidate

选择 `prepare-candidate`。正常流程会：

1. 对当前 dispatch 的完整 `main` SHA 构建一次 frontend artifact。
2. 生成完整不可变 release，并校验 archive 大小、SHA-256 和 manifest。
3. 使用增量传输上传变化块；只有服务器没有任何可验证传输基准时，才可由用户明确
   勾选一次 `bootstrap_full_upload`。
4. 更新 Candidate 指针并重启固定 8001。
5. 验证 Candidate SHA、`/healthz`、3G/4G、只读数据库权限、后台任务禁用和
   18002 预览。
6. 输出本次精确的 commit SHA、archive SHA-256、manifest SHA-256 和操作报告。

成功仅表示 Candidate 可供人工测试，不表示已发布到 www。
若上一次进程被强制终止，导致 Candidate 指针、环境文件、8001 或 18002 身份不一致，
下一次 prepare 会在改指针或重启前拒绝并报告 `candidate_runtime_inconsistent`（或精确的
runtime identity mismatch）。先运行现有 `discard-candidate` 清空该测试槽，再重新
prepare；不为这种情况建立 recovery/checkpoint 状态机。

### 4.2 废弃 Candidate

选择 `discard-candidate`，勾选 `confirm_control_operation`，目标 SHA 输入必须留空。
该操作应停止 8001 和 18002，清除 Candidate 指针，并只删除不再被任何保护指针引用
的 release。Active、www、数据库内容和 JATO 数据保持不变。

### 4.3 更新 Active

只有人工页面测试完成后才选择 `update-active`。必须：

1. 从成功的 Candidate 操作报告复制完整 commit SHA、archive SHA-256 和 manifest
   SHA-256，不凭记忆输入。
2. 将三项值分别填入 `target_commit_sha`、`target_archive_sha256` 和
   `target_manifest_sha256`。
3. 勾选 `confirm_control_operation`，由用户明确批准这一精确 release。

服务器会再次证明三项身份与当前 Candidate 完全一致，检查数据库 revision 和正在执行
的 JATO 写任务，然后记录旧 Active 指针、原子更新 `active.current`，使用 Active
专属环境重启固定 8000，并验证：

- 内部健康与预期运行 SHA；
- 2 个后端 worker；
- Active 为 6G/8G；
- 仅 Active 的后台任务状态；
- www 公网健康与 frontend identity。

安装、重启或验证过程中若失败，同一操作会恢复更新前的 Active 指针和运行配置，重启
旧版本并验证公网。Candidate 未获授权时，Active 根本不会发生变化。

第一次 legacy→B/B 会在同一 `update-active` 中安装固定 8000 unit 与 compatibility link；
这不是独立 bootstrap，也不会自动发生。该次成功后页面或命令若请求 rollback，控制器会
明确返回 `rollback_unavailable`，不会把 B/B 的无操作伪装成已回退。

`update-active` 成功后 Candidate 仍可保留供短期核对；需要释放测试环境时，再单独运行
`discard-candidate`。

### 4.4 主动回退 Active

只有目标仍等于受保护的 `active.current` 或 `active.previous`，才运行
`rollback-active`。必须从只读回退预检复制完整 commit SHA、archive SHA-256 和 manifest
SHA-256，分别填入三个 target 输入，勾选 `confirm_control_operation`；不得留空或只凭
“最近版本”猜测。

首次 B/B 没有 distinct previous，因此不能主动回退。下一次 C 更新形成 `C/B` 后，
以 `C/B` 回退到 B 为例，系统使用内核原子交换一次进入 `B/C`，不会出现任一版本失去
指针保护的中间态。控制器捕获到的重启/验证失败会再次原子交换
回 `C/B` 并验证 C；若进程被强制终止，磁盘状态也只会是 `C/B` 或 `B/C`，下一次对同一
B 的显式重试只会验证或继续启动 B，不会自动切回 C。只有用户再次明确提交 C 的完整
三元组时，才允许从 `B/C` 反向交换为 `C/B`。若目标不再受 current/previous 保护，
操作必须拒绝，不能根据 Git 历史临时生成替代版本。

## 5. Candidate 页面访问方式

`https://www.ojeur.cloud` 永远是 Active，不能用它判断 Candidate。固定测试地址是：

```text
https://candidate.ojeur.cloud
```

浏览器先通过独立的 Basic Auth，再在 Candidate 页面使用 JATO 账号密码登录。www 的
localStorage 登录态不会跨域带入 Candidate；第一版不改变 Active OAuth 配置，因此不要
用 Candidate 测试 Google/飞书 OAuth 回调。

该 URL 本身不绑定某个 commit。每次从更新后的 `main` 成功运行 `prepare-candidate`，
控制器会更新 8001 的 Candidate 指针和 18002 的预览身份；刷新同一个 URL 就会看到最新
Candidate。这个动作不会更新 8000、www 或 intl。Candidate 被废弃或启动失败时，固定 URL
必须返回 5xx，绝不能回退显示 Active。

固定网关不可用时，SSH 隧道仍作为运维备用入口。在本机终端运行：

```bash
ssh -N -p "${SSH_PORT:-22}" \
  -L 18002:127.0.0.1:18002 \
  "$SSH_USER@$SSH_HOST"
```

保持该终端运行，然后打开：

```text
http://127.0.0.1:18002
```

验收时至少检查：

- 页面显示 `CANDIDATE` 标识及预期完整 SHA；
- `/candidate-preview.json`、`/healthz` 和 `/readyz` 指向 8001 的同一 release；
- 登录、主要页面、API、筛选与性能符合预期；
- 月更和其他写入口明确拒绝，而不是悄悄执行；
- www Active 在整个测试期间仍为原 SHA 且健康。

本机 18002、Candidate 域名与 www 都是不同 origin，应分别登录。没有一个健康且身份
一致的 Candidate 时，18002 和固定公网地址都必须拒绝，不能回退显示 Active。

## 6. intl 是后续独立动作

`update-active` 成功即表示 www 更新成功。需要更新 intl 时，用户在之后合适的时间另行
手动运行既有 `sync-www-active-to-intl` workflow：

- 它只读取当时的 current www Active；
- 它同步 Active 内嵌的同一个 immutable frontend artifact；
- 它不接受手填 Candidate 或历史 SHA；
- 它不修改 Candidate、Active 后端、数据库或 JATO 数据；
- intl 同步失败只作为 intl 问题处理，不回滚已经成功的 www Active。

因此 www 与 intl 可以短时间处于不同版本。是否再次同步 intl 由用户单独决定，V2
四操作不等待该结果。

## 7. 四指针与 release 保留

服务器只用四个权威指针保护当前和上一版本：

```text
/opt/jato/slots/8000/current   # active.current
/opt/jato/slots/8000/previous  # active.previous
/opt/jato/slots/8001/current   # candidate.current
/opt/jato/slots/8001/previous  # candidate.previous
```

不可变 release 存放于：

```text
/opt/jato/releases/<commit>/<archive-sha256>/
```

同一 release 可以同时被多个指针引用，因此通常只有 2 至 3 个唯一版本。清理必须保护
四个指针及当前操作正在使用的 release；只删除未被引用的 release、失败 staging 和
传输缓存。`discard-candidate` 后，如果该 release 同时是 `active.current`，它仍受
Active 指针保护，不能被删除。

archive cache 只识别 `<cache>/<40-hex>/<64-hex>.tar.gz` 以及对应 `.partial`、`.sha256`
文件。保护集合只来自上述四指针；删除某个未保护身份前必须非阻塞取得其既有永久
`.lock`。lock 正忙、缺失或路径不安全时只保留并诊断，绝不 unlink `.lock`。四个操作
成功后均 best-effort 清理未保护的 archive/partial/sha 文件，清理失败不反向改变已成功
的指针或服务操作。

`update-active` 对同一目标重试时不得轮换 previous；`rollback-active` 成功后保留
`current=回退目标`、`previous=回退前版本`。同一目标重试只收敛当前状态，不会 toggle；
反向切换必须再次提供 previous 的完整三元组并获得用户授权。

## 8. 操作前后应看到的证据

每次让 Codex 执行前，先要求它只读报告：

- 当前 www Active 完整 SHA、8000 健康、2 workers 和 6G/8G；
- 当前 Candidate 完整 SHA或为空、8001/18002 状态和 3G/4G；
- Candidate 数据库只读证明与后台任务禁用状态；
- www Nginx 仍固定指向 8000；
- 是否有另一项发布操作持有单一部署锁；
- 本次将使用的 commit/archive/manifest 三元组。

操作完成后，结构化报告应区分：

- 已通过检查；
- 失败的阶段及 expected/actual；
- 尚未执行的检查；
- Active、Candidate、www 流量、数据库和 JATO 数据是否发生变化。

若证据与预期不一致，停止当前操作并报告一次完整原因，不猜测指针、不重放旧任务，
也不把“HTTP 200”单独当成版本正确的证明。

## 9. 可直接发给 Codex 的口令

每条口令只授权一项 V2 操作：

| 用户口令 | 对应操作 |
| --- | --- |
| `部署当前 main 到 Candidate，只供人工测试，不更新 Active` | `prepare-candidate` |
| `废弃当前 Candidate，Active 不动` | `discard-candidate` |
| `批准已测试的 Candidate 三元组更新 www Active` | `update-active` |
| `将 www Active 回退到只读预检给出的受保护三元组` | `rollback-active` |

若要处理 intl，应单独说：

```text
同步当前 www Active 到 intl；intl 失败不要回滚 www。
```

这不是第五个 V2 操作，而是既有的独立 intl workflow。
