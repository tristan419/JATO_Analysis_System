# Candidate 人工验收与发布操作手册

这份手册记录 JATO/Ojeur 在腾讯云上的固定 Candidate / 固定 Active 发布方式，
以及可直接发给 Codex 的操作口令。以后不记得流程时，先让 Codex
读取本文件，再决定是否执行任何生产动作。

> 本文件描述发布代码，不描述 JATO 月更数据发布。部署 Candidate、切换代码
> 和发布 JATO 数据是三套不同授权，不能互相代替。

## 一句话原则

```text
main 不可变版本 -> 无公网 Candidate -> 人工页面验收
                    |                  |
                    | 不满意           | 满意
                    v                  v
                 废弃并继续改       同 artifact 更新固定 Active
                                      -> 清理 Candidate
                                      -> intl 可在之后独立同步
```

- `Active` 是当前正式公网服务。
- `Candidate` 是最新代码的服务器人工验收环境，不承接正式公网流量。
- `Active` 和 `Candidate` 的角色不互换。批准后仍由原 Active 端口承接公网，
  Candidate 仍是测试实例，直到用户明确清理。
- Candidate 验收失败只废弃 Candidate，Active 从未改变，因此不发生 Active
  回滚，也不需要 recovery/dry-run/apply。
- 服务器把更新前正在成功服务的 Active Git SHA、该 SHA 对应的 immutable
  artifact、slot env 和 Nginx preimage 记录为恢复源。只有更新 Active 这一步
  自身失败时才自动恢复它；不能拿“最近一次绿色 GitHub run”或按同一 SHA
  临时重建的包代替。
- 只有 Active 可以运行 JATO 月更 worker、scheduler 等单实例后台任务，
  Candidate 必须禁用这些任务，防止重复执行。
- Candidate 与 Active 使用同一套服务器数据连接做页面验收。任何写入型页面
  测试都有可能改变真实数据，必须由用户有意执行；它不是无副作用沙箱。

## 用户可以直接发送给 Codex 的口令

每条口令都只授权其字面动作，不自动授权下一步。

| 口令 | 预期结果 |
| --- | --- |
| `部署最新 main 到 Candidate，仅停在人工验收，不切公网` | 构建并启动精确 main SHA 的 Candidate，保持 Active 和公网路由不变，返回 SSH 隧道命令、本机验收地址、SHA 和 artifact 摘要。 |
| `废弃 Candidate <SHA>，Active 不动` | 停止并清理指定 Candidate；不得更新 Active 或删除旧 Active artifact/preimage。 |
| `批准 Candidate 更新 Active <SHA>` | 最后核对 SHA、健康和 artifact 后，在 maintenance fence 下更新固定 Active 并重启；公网上游端口不变。仅当安装、重启或公网核验失败时，自动恢复更新前正在成功服务的 SHA 与其原始 artifact。 |
| `确认同步当前 www Active 到 intl` | 只读识别当前 www Active，并把其 release root 内嵌的原始 immutable frontend artifact 同步到 intl；不接受手填历史 SHA。 |
| `确认发布完成并清理 Candidate <SHA>` | www 验收完成后停止 Candidate、撤销 preview 并释放测试槽；不等待 intl，且不删除 immutable artifact 或 Active 恢复证据。 |

Codex 收到口令后仍应先报告它观察到的 Active SHA、Candidate SHA、当前
Nginx 路由和后台任务归属。发现现场与口令不一致时应停止，而不是猜测端口
或重放旧 workflow。

## 目标状态机

### 1. 部署 Candidate

1. 只接受从最新 `main` 产生的 immutable artifact。
2. 自动识别当前 Active 槽，并把另一槽作为 Candidate。
3. Active 继续以 `MemoryHigh=6G`、`MemoryMax=8G` 承接公网。
4. Candidate 在测试阶段以 `MemoryHigh=3G`、`MemoryMax=4G` 运行。
5. Candidate 禁用月更 worker、scheduler 和其他单实例任务。
6. Candidate 停在 `candidate_ready`，不能自动切公网 Nginx。
7. 返回精确 SHA、服务端健康证据和仅供验收的访问方式。

### 2. 人工页面验收

用户在 Candidate 完整页面测试真实服务器环境中的业务功能。此阶段：

- `https://www.ojeur.cloud` 仍然是 Active，不能用它判断 Candidate；
- Candidate 页面必须显示其精确 SHA 和 `CANDIDATE` 标识；
- 测试不满意就废弃 Candidate，继续修改代码并部署新的 main SHA；
- 测试满意也不会自动上线，必须另发 `批准 Candidate 更新 Active <SHA>`。

### 3. 批准更新固定 Active

1. 再次核对 Candidate 的完整 SHA、archive SHA-256、健康、资源以及单实例
   任务未在 Candidate 运行。
2. 持久保存当前 Active 的 release root/current symlink、slot env、Active release
   link 和 Nginx frontend preimage，并绑定更新前正在服务的成功 SHA。
3. 复用现有 quiescence gate 检查真实任务状态：空闲时立即通过；只有实际 JATO
   写入、上传 digest 或 baseline promotion 正在执行时才有界等待。它不会取消
   正在运行的任务，也不要求用户再做一次确认。通过后暂停 scheduler 和月更
   入口；`active-slot` 和公网 upstream 端口保持不变。
4. 原子替换固定 Active 的 `current` symlink，并按固定 Active slot 和目标 SHA
   生成、替换 env；不能把 Candidate env 直接复制给 Active。随后重启固定
   Active。
5. Nginx 只更新 frontend root；后端仍代理原 Active 端口。执行 `nginx -t`
   和 reload 后核对公网精确 SHA、healthz、readyz、2 workers 与 6G/8G。
6. 成功后恢复仅属于 Active 的 scheduler/月更任务；Candidate 和 preview 暂时
   保留，等待用户单独确认清理。
7. 任一步失败都自动恢复更新前成功 Active 的 root、env、Active release link
   和 frontend preimage，重启并验证旧 SHA、6G/8G 与公网健康。恢复证明通过
   前 maintenance fence 不得解除，也不得把失败写成成功。

这里的自动恢复只是 Active 更新命令的失败保护，不是 Candidate 验收流程中的
额外步骤。Candidate 未获批准时 Active 根本不会改变；Active 更新及公网核验成功
后也不会再自动执行恢复。

实现时必须保证“人工验收的 artifact”就是 Active 使用的 artifact，不得在
批准后重新构建、重新上传或复制另一份同 SHA 包。固定 Active 重启可能带来
几秒连接中断，这是用更简单的固定角色模型换取的明确取舍。

### 4. 独立处理 intl 与 Candidate 清理

www 的固定 Active 更新成功后，下面两个动作互相独立，执行顺序不构成门禁：

1. `确认同步当前 www Active 到 intl`：可在之后任何合适时间，手动运行独立
   `sync-www-active-to-intl` workflow。它只有一个确认框，自动读取当前 www
   Active 的 content-addressed root、runtime seal 与内嵌 frontend artifact，
   先核对公网 www，再幂等同步并精确审计 intl；不依赖 Candidate handoff。
2. `确认发布完成并清理 Candidate <SHA>`：www 验收完成后即可停止 Candidate、
   撤销 preview 并恢复测试槽为空闲，不要求 intl 已经同步。

若 prepare workflow 最终标红，但服务端已经生成精确 `candidate_ready` handoff，
`discard-candidate` 仍可在验证该 handoff 后清理残留；它只接受 `success` 或
`failure` 的已完成 run。`release-candidate` 和 Active 批准仍只接受 `success`，
`cancelled` 等不确定终态一律拒绝。

GitHub handoff 或 frontend artifact 已缺失/过期时，**只允许 Candidate 清理走
canonical server fallback，不允许据此批准 Active**。cleanup workflow 会自动
切换证明来源：当前 `main` 控制脚本在腾讯云 production lock 下只读采集请求
run/SHA/archive 对应的 canonical checkpoint、evidence、content-addressed archive、
内嵌 frontend identity，以及 Candidate 的物理 slot 和固定 preview port `18002`。
runner 验证这些证据后，仍交给原有清理执行器；执行器会再次在同一生产锁内核对
路径、摘要和实时 Candidate 状态，再执行精确清理。因此不存在仅凭手填 SHA 清理
任意 slot 的路径。checkpoint、evidence、archive、slot 或 preview 任一不一致都会
拒绝，Active 保持不动。fallback 还会从 canonical journal 中重建最初
`candidate_ready` attestation，并与用户提供的原始 SHA-256 比对；输入任意格式
正确的摘要不能通过。

清理 Candidate 不等于删除 immutable artifact、旧 Active artifact、发布日志或
恢复证据。但当前四种 workflow 模式不提供“发布成功后再显式业务回退”的按钮；
本 PR 只保证 Active 更新过程自身失败时精确恢复更新前版本。以后若要主动回到
旧版，必须单独授权并复用已验证 artifact，不能仅按 Git SHA 临时重建。intl 的
同步与回退始终需要独立确认。

独立 intl workflow 只允许从当前 `main`、`production` environment 运行，并与
生产发布共用同一 concurrency lock。若当前 www Active 仍是 legacy、不是
`/opt/jato/releases/<commit>/<archive-sha256>` 形式，它会明确拒绝；应先完成一次
新的 fixed Active 发布，不能按手填 Git SHA 重新构建。若 intl 已经是同一精确
artifact，本次运行只审计并写回执，不重复部署。该动作不修改 Candidate、后端、
数据库或 JATO 月更数据。

`frontend-dist` 与 `release-candidate` handoff 均保留 30 天，和批准/清理回执
窗口一致。30 天内优先使用原始 GitHub artifact handoff；artifact 在 30 天后
缺失或显示 expired，不再阻断 `candidate_ready`/`rollback_completed` 中保留
Candidate 的精确废弃，也不阻断 `active_updated` 后的 Candidate 释放；workflow
会改用上述 canonical server cleanup。批准 Candidate
更新 Active 始终要求原始、未过期的 GitHub handoff，不使用 fallback。canonical
证据不完整或现场已漂移时只能拒绝并人工审查，不能重建 handoff 或猜测 slot。

## Candidate 页面怎么访问

### 本功能 PR 提供的入口

`prepare-candidate` 会复用双槽基础设施，但角色由 `active-slot` 固定，不执行
槽位互换。自动校验完成后停在 `candidate_ready`，并启动一个独立 transient
Nginx：

- 只监听 `127.0.0.1:18002`，腾讯云安全组没有新增公网端口；
- React 静态文件精确绑定本次 immutable release；
- `/v1`、`/healthz`、`/readyz` 和 `/docs` 只代理本次 Candidate 槽；
- `/candidate-preview.json` 绑定完整 SHA、artifact 摘要、物理槽位和端口，并
  禁止缓存；
- 页面顶部固定显示黄色 `CANDIDATE` 横幅、SHA 和 artifact 摘要；物理槽位只作
  诊断信息。Candidate origin 无法验证身份时显示红色阻断提示，不能伪装成
  Active；
- 独立预览 Nginx 受 `128M/256M` cgroup 限制，不改变公网 Nginx 配置，
  不执行公网 Nginx reload。

只可从 `main` 运行 `production-release`：合并到 `main` 的 push 会自动
执行 `prepare-candidate`，也可手动选择该模式。该模式完成腾讯云
Candidate attestation 后
会跳过 www provenance、Cloudflare intl 发布和最终 parity audit。生产 workflow
不再提供旧 `prepare-and-switch` 槽位互换选项；固定角色 workflow 提供四个明确
模式：`prepare-candidate`、`approve-candidate-to-active`、`discard-candidate`
和 `release-candidate`。后两者只清理 Candidate，不读取或修改 intl。

部署完成后，在本机建立隧道：

```bash
ssh -N -p "$SSH_PORT" \
  -L 18002:127.0.0.1:18002 \
  "$SSH_USER@$SSH_HOST"
```

然后在本机直接打开：

```text
http://127.0.0.1:18002
```

这个链接不需要知道 Candidate 当次落在 `8000` 还是 `8001`。如果没有一个
精确、健康的 `candidate_ready`，18002 必须拒绝连接，不能回退显示 Active。
它与 www 是不同 origin，现有 www localStorage 登录态不会自动带过来；应在
Candidate 页面重新使用账号密码登录。OAuth 回调在 18002 上尚未作为验收入口。

### `candidate.ojeur.cloud` 的边界

固定域名可以后续接到同一个 18002 origin，但必须使用 Cloudflare Access
保护的 named Tunnel：

```text
candidate.ojeur.cloud -> Cloudflare Access -> named Tunnel
                      -> 127.0.0.1:18002
```

不能给 `candidate.ojeur.cloud` 创建直连 CVM 的 A/AAAA 记录，也不能让预览
Nginx 监听 `0.0.0.0`。当前仓库和 GitHub production environment 尚没有
专用 Tunnel ID、Access Application/AUD、精确 reviewer policy 和 CVM
cloudflared 凭据，因此本 PR 先交付可用且不暴露源站的 SSH 隧道本地入口；固定域名
必须作为单独基础设施动作配置，不能复用并扩权现有 Pages token。

### 数据与后台任务边界

Candidate 与 Active 使用相同的服务器数据库、Redis 和业务数据连接，因此可
用于排查“本地数据与服务器数据不同”的问题，也意味着页面写操作会作用于真实
生产数据。只有用户明确希望验证的页面操作才可执行。Candidate 必须禁用 JATO
月更 worker、scheduler 和其他单实例后台任务，避免两个实例重复消费或自动执行；
这些后台 gate 与页面的数据访问能力是两件不同的事。

### 一次性历史事故边界

旧 `29df5e6e...` 发布仍可能受 recovery-only production hold 保护。它不是
Candidate 正常发布流程的一部分，也不应继续扩展成日常 dry-run/apply 平台。
若 hold 仍存在，新 Candidate 会在任何服务器变更前明确拒绝，并指向
[`FREE_RELEASE_RECOVERY_RUNBOOK.md`](./FREE_RELEASE_RECOVERY_RUNBOOK.md) 的事故
记录；严禁重试旧 production run、手删 checkpoint/残留或绕过 hold。

## Release Control 的后续边界

短期由用户在任务中发送上述口令，Codex 通过 GitHub Actions 和腾讯云受控脚本
执行。普通 Ojeur Active/Candidate 页面不能直接拥有服务器 shell 权限。

未来若增加 Release Control，它应是独立于业务实例的控制面，只调用
经过鉴权、审计和幂等约束的发布动作，并展示：

- Active、Candidate 和旧 Active artifact/preimage 的 SHA 与健康状态；
- `部署 Candidate`、`废弃 Candidate`、`批准 Candidate 更新 Active`；
- `确认同步 intl`、`回滚`、`清理 Candidate`；
- 每一步的操作者、时间、输入 SHA、结果和不可变证据。

浏览器按钮不能拼接或执行任意 shell，也不能根据自己所在端口猜测 Active。
控制面必须从 `active-slot`、Nginx、systemd、`/readyz` 和 artifact identity
共同解析真实角色，并在证据不一致时拒绝操作。
