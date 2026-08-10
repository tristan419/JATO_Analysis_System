---
goal_control:
  id: fixed-active-candidate-release-v2
  status: active
  objective: >-
    将复杂蓝绿/事故恢复体系收敛为固定 Active/Candidate 的四操作发布 V2，
    Candidate 每次 prepare 使用生产一致性快照生成独立可写数据库沙箱并以应用内 admin
    身份直接进入页面；完成代码、CI、腾讯云无流量验收后，再由用户授权把同一已测试构件
    更新到正式 www Active。Candidate 测试数据永不进入 Active；intl 继续使用既有的
    Active 到 intl 独立同步流程，不在 V2 中新增编排。
  current_phase: candidate_runtime_secret_isolation_hotfix
  current_step: publish_hotfix_draft_pr
  waiting_on: draft_pr_publish_then_github_required_checks
  pause_reason: none
  next_action: >-
    修复首次服务器初始化前发现的 Candidate root/backend.env 凭据暴露：8001 使用动态
    jato-candidate 身份且不继承 Active backend.env，并由现有 verifier fail closed。通过独立
    PR 合并后，继续用户已经授权的服务器 role/ACL/env 合同初始化与当前 main Candidate
    prepare。禁止自动更新 Active 或同步 intl。
  release_authorization_contract:
    source_path: main_to_candidate_to_explicit_user_approval_to_active
    main_may_advance_without_active: true
    candidate_may_be_replaced_repeatedly: true
    candidate_may_update_active_automatically: false
    candidate_may_update_intl: false
    active_requires_explicit_user_approval: true
    active_uses_exact_tested_artifact: true
    active_update_triggers_intl_automatically: false
    intl_sync_path: existing_sync_www_active_to_intl_from_current_active
    intl_failure_rolls_back_www_to_previous: false
    intl_failure_preserves_www_active: true
  progress:
    worktree_ready: true
    worktree: /Users/litristan/.codex/worktrees/candidate-runtime-secret-isolation/JATO_Analysis_System
    branch: codex/candidate-runtime-secret-isolation
    base_main_sha: b128f5e5d1066e883f26649cad0441d362aa1e04
    remote_main_matches_base: true
    design_recorded: true
    historical_inventory_evidence_recorded_below: true
    inventory_command_present_in_v2_runtime: false
    server_inventory_complete: true_read_only_2026_08_07
    server_inventory_mutation_performed: false
    server_target_filesystem: ext4_linux_6_8
    store_manifest_primitives_complete: true
    store_manifest_unit_tests_passed: 8
    manifest_cli_unit_tests_passed: 2
    local_v2_tests_passed: 135_candidate_controller_and_admission
    current_fixed_release_ci_equivalent_tests: 1285_passed_15_skipped
    controller_store_admission_tests_passed: 102
    release_seal_tests_passed: 21
    monthly_role_tests_passed: 18
    admission_primitives_complete: true_local
    database_revision_primitives_complete: true
    candidate_database_privilege_probe_complete: true_local
    candidate_database_role_configured_on_server: false_legacy_readonly_role_requires_reconciliation
    four_operation_methods_present: true
    steady_state_four_operations_complete: true_local
    legacy_first_update_active_complete: true_local_b_b
    legacy_server_archive_unique_and_verified: true_exact_offline_reconstruction
    legacy_archive_exact_bytes: 22269916
    legacy_archive_exact_sha256: 6af46992b1da87b6cb38d2cbc3a4bf9240f1dc82746f457c22bc69e74d78cc5e
    legacy_manifest_sha256: 9f62cda70530ec38560c9fa25846eee8ea767f7bcbd67f6bac49f3eb198fd947
    a0_archive_recovery_complete: true
    a0_helper_complete: false_rejected_and_removed_before_push
    a0_helper_tests_passed: 0_removed_with_rejected_helper
    a0_exact_frontend_materialization_verified: true_local
    a0_exact_tar_data_filter_entries_verified: 2637
    a0_server_execution_authorized: false
    manifest_build_metadata_verification_complete: true_local
    v2_source_critical_closure_complete: true_local_13_files
    sourceable_runtime_builder_complete: false_removed_helper_only_change
    local_ready_blockers_open: 0
    independent_review_passed: true_hotfix_no_p0_p1_p2
    server_acceptance_blockers_open: 4_runtime_secret_isolation_role_acl_dropin_prepare
    update_active_retry_idempotent: true_local
    rollback_active_retry_idempotent: true_local
    rollback_sigkill_reference_safe: true_local_and_linux_ci_target_ext4_unprobed
    rollback_atomic_exchange_linux_ci_passed: true
    failed_prepare_release_cleanup_complete: true_local
    legacy_store_coexistence_gc_complete: true_local
    nonblocking_jato_release_lock_complete: true_local
    fixed_active_route_required_before_candidate: true_local
    fixed_preview_contract_complete: true
    active_candidate_role_contract_complete: true
    candidate_database_env_contract_complete: true_local
    candidate_systemd_isolation_contract_complete: true_local
    candidate_effective_dropin_allowlist_complete: true_local
    candidate_monthly_http_423_gate_complete: true_local
    legacy_active_candidate_prepare_complete: true_local
    legacy_active_candidate_discard_complete: true_local
    dedicated_candidate_unit_contract_complete: true_local
    existing_active_to_intl_sync_reused_unchanged: true
    verified_failure_restore_complete: true
    frontend_runtime_identity_check_complete: true
    v2_only_gc_complete: true
    archive_cache_gc_wired_to_ssh_user_cache: true_local
    same_filesystem_staging_complete: true
    remote_lock_handoff_complete: true_local
    remote_v2_entry_drafted: true
    remote_v2_entry_syntax_passed: true
    workflow_v2_integration_complete: true_local
    workflow_validators_passed: 2
    runtime_seal_final_path_binding_complete: true_local
    v1_incident_workflow_entries_removed: true_local
    v1_deep_source_cleanup_deferred_to_phase_b: true
    v2_active_baseline_resolver_complete: false
    candidate_indirect_intl_prewarm_decoupled: false_out_of_scope
    intl_workflow_files_modified: false
    candidate_external_side_effect_sandbox: false_documented_operator_limit
    runtime_python_lines: 4882_after_final_p2_closure
    workflow_unit_tests_passed: 115
    deployment_tests_passed: 1285
    deployment_tests_skipped: 15
    backend_tests_passed: 103
    frontend_tests_passed: 375
    frontend_build_and_router_checks_passed: true
    full_local_ci_after_final_runtime_code: true_scripts_and_frontend
    post_ci_changes_documentation_only: true_final_ci_result_record_only
    ci_validation_complete: true_github_checks_green_on_b80bcb93
    candidate_sandbox_initial_ci_failure: fullstack_frontend_vite_auth_token_test_environment_leak
    candidate_sandbox_ci_fix_scope: one_test_file_plus_goal_no_production_code
    candidate_sandbox_ci_fix_local_validation: 373_tests_types_build_router_passed_with_github_env
    candidate_sandbox_ci_fix_commit: c1a87b47eea0d94d1dc3bbc01b9efb17d39a6c73
    candidate_sandbox_ci_fix_pushed: true
    candidate_sandbox_ci_verified_head: b80bcb932c4e4d7eaa941169e5b06a6c10f219b6
    candidate_sandbox_github_checks: all_required_and_cloudflare_green_before_post_215_sync
    candidate_sandbox_merge_authorized: true_user_2026_08_10
    candidate_sandbox_post_215_sync_complete: true_local
    candidate_sandbox_post_215_synced_tree_tests: 1279_passed_15_skipped_and_375_frontend
    bom_admin_pull_request_215_merged: true_main_40ae3211
    candidate_sandbox_draft_ready_for_human_review: true
    previous_pull_request_214_merged: true_main_30f3e2e4
    current_fix_commit: 36c2d9f96eecf1524a69e7fd81ae18d0234025a7
    current_fix_github_checks: 13_of_13_green
    pull_request_opened: false_current_hotfix_pending
    pull_request_number: none_current_hotfix_pending
    pull_request_url: none_current_hotfix_pending
    pull_request_is_draft: false_not_opened
    previous_pull_request_217_merged: true_main_619466e8
    candidate_public_gateway_commit: 991a44f8499a1210317aafb5da1f3183b7ee0769
    candidate_fixed_public_link_required: true
    candidate_public_gateway_design: dnspod_to_shanghai_nginx_basic_auth_to_127_0_0_1_18002
    candidate_public_gateway_implemented: true_local
    candidate_public_gateway_contract_tests: 22_passed_2_skipped
    candidate_public_gateway_style_check: passed
    candidate_public_gateway_goal_yaml_check: passed
    candidate_public_gateway_independent_review: passed_no_p0_p1
    candidate_public_gateway_github_checks: 13_of_13_green
    candidate_dns_configured: true
    candidate_tls_configured: true
    candidate_basic_auth_configured: true
    candidate_current_sha_verified: b21695163df510e4dd8e91b4701446d917f7d8b8
    candidate_public_link_may_fallback_to_active: false
    active_changed_by_this_step: false
    intl_changed_by_this_step: false
    fixed_active_nginx_contract_migrated_on_server: true_behavior_preserved
    first_candidate_attempt_run: 31253025482_failed_safe
    candidate_runtime_contract_verified_on_server: true_read_only_2026_08_08_main_b2169516
    candidate_no_worker_runtime_verified_on_server: true_read_only_2026_08_08
    candidate_preview_verified_on_server: true_read_only_2026_08_08_18002_loopback
    candidate_writable_sandbox_design_recorded: true
    candidate_writable_sandbox_implementation: true_local_verified
    candidate_writable_sandbox_fifo_capacity: 1
    candidate_writable_sandbox_transition_max_databases: 2
    candidate_application_no_login_admin: true_local_committed
    candidate_sandbox_may_write_active_database: false
    candidate_sandbox_may_update_active_or_intl: false
    bom_colour_library_followup_in_this_pr: false_out_of_scope
    candidate_sandbox_pull_request_opened: false_merged
    candidate_sandbox_pull_request_number: 219_merged
    candidate_sandbox_pull_request_url: https://github.com/tristan419/JATO_Analysis_System/pull/219
    candidate_sandbox_initial_commit: 0949dcdb39a2d12f88e678f0daa0b0563d33048a
    candidate_sandbox_runtime_modules_added: 0
    candidate_sandbox_actions_or_workflows_added: 0
    candidate_sandbox_controller_net_lines: 501
    candidate_sandbox_admission_net_lines: 125
    candidate_sandbox_tests_passed: 129
    candidate_sandbox_all_script_tests: 1279_passed_15_skipped
    candidate_sandbox_frontend_tests: 375_passed_post_215_main_sync
    candidate_sandbox_frontend_build_and_router: passed
    candidate_sandbox_independent_review: passed_final_no_p0_p1_or_actionable_p2
    candidate_sandbox_p1_snapshot_permissions: fixed_streamed_dump_restore_real_postgres_passed
    candidate_sandbox_p1_superuser_release_execution: fixed_candidate_role_and_nobody_no_postgres_release_code
    candidate_sandbox_p2_cluster_binding: fixed_explicit_database_host_port_and_socket
    candidate_sandbox_p2_forced_interrupt_orphan: fixed_strict_marker_reference_gc_tests_passed
    candidate_sandbox_p2_preview_identity_drift: fixed_fail_closed_before_database_mutation
    candidate_sandbox_p2_banner_database_identity: fixed_required_and_displayed
    candidate_sandbox_p2_discovery_sql_argv: fixed_stdin_file_dash
    candidate_sandbox_real_postgres_integration: passed_snapshot_restore_migrate_finalize_admission_head
    candidate_sandbox_real_server_integration: false_authorized_current_request_not_executed
    candidate_server_role_acl_reconciliation: false_authorized_current_request_not_executed
    candidate_server_dropin_reconciliation: false_authorized_current_request_not_executed
    candidate_sandbox_initialization_authorized: true_user_current_request_2026_08_10
    prepare_candidate_authorized: true_user_current_request_after_safe_initialization
    candidate_runtime_secret_isolation_blocker: confirmed_root_and_active_backend_env_on_main_b128f5e5
    candidate_runtime_secret_isolation_hotfix_worktree: /Users/litristan/.codex/worktrees/candidate-runtime-secret-isolation/JATO_Analysis_System
    candidate_runtime_secret_isolation_hotfix_branch: codex/candidate-runtime-secret-isolation
    candidate_runtime_secret_isolation_server_mutation: false
    candidate_runtime_secret_isolation_environment_files_reset: true_local
    candidate_runtime_secret_isolation_dynamic_user: true_local
    candidate_runtime_secret_isolation_effective_uid_gate: true_local
    candidate_runtime_secret_isolation_redis_disabled: true_local
    candidate_runtime_secret_isolation_tests: 135_focused_and_1285_all_scripts
    candidate_runtime_secret_isolation_workflow_validators: 2_passed
    candidate_runtime_secret_isolation_independent_review: passed_no_p0_p1_p2
    candidate_runtime_secret_isolation_pull_request: false_pending_review
    candidate_sandbox_merge_sha: 2dea140f328c1d7077ca0792979b47bcca4dca8e
    bom_colour_implementation_merge_sha: b128f5e5d1066e883f26649cad0441d362aa1e04
    bom_colour_goal_pull_request: 218_merged_main_d40981e8
    existing_v2_goal_assessment: partially_achieved
    existing_v2_core_code_complete: true
    existing_v2_fixed_candidate_link_complete: true
    existing_v2_server_prepare_verified: true_previous_readonly_candidate
    existing_v2_server_update_active_verified: false
    existing_v2_server_distinct_rollback_verified: false
    existing_v2_writable_business_test_ready: false_runtime_hotfix_and_server_init_pending
    production_changed: false
  may_continue_without_new_authorization:
    - local_read_only_audit
    - documentation_updates
    - local_v2_implementation
    - local_and_ci_test_preparation
  explicit_authorization_required:
    - server_bootstrap_or_cleanup
    - candidate_sandbox_database_provision_or_rotation
    - prepare_candidate_on_tencent
    - update_or_rollback_active
    - merge_pull_request
    - production_release
    - github_production_secret_migration
  stop_conditions:
    - observed_server_state_contradicts_documented_baseline
    - change_would_touch_active_or_intl_database_content
    - change_would_cross_this_pr_scope
  updated_at: "2026-08-10T11:35:03+08:00"
---

# Fixed Active / Candidate Release V2

> 状态：实施中
> 开始日期：2026-08-06
> worktree：`/Users/litristan/.codex/worktrees/candidate-runtime-secret-isolation/JATO_Analysis_System`
> branch：`codex/candidate-runtime-secret-isolation`
> 基线：`main@b128f5e5d1066e883f26649cad0441d362aa1e04`（已包含 #219、#220）
> 当前 PR scope：仅让固定 Candidate 以动态非 root 身份运行、清除 Active env 继承、禁用
> Candidate Redis，并同步现有 verifier、测试和操作文档；不改四操作、Active、intl、
> 生产数据库内容、BOM 颜色业务或 JATO 数据

## 0. Goal Control 使用规则

本文件是该 Goal 的权威工作源，平台 Goal 和 Plan 是其运行时镜像：

1. 每个工作单元开始前，先读取顶部 `goal_control`。
2. 只执行 `current_phase` 范围内且不违反 `stop_conditions` 的动作。
3. 每完成一项工作，先更新 `progress`、实施日志、证据和 `next_action`。
4. 再把相同状态同步到平台 Goal/Plan，避免对话状态与仓库记录分叉。
5. 用户直接修改本文件后，下一次工作以修改后的控制块为准；若新内容扩大生产或
   数据权限，仍需遵守 `explicit_authorization_required`，MD 不能替代明确授权。
6. `status=paused` 时停止继续实现；`status=complete` 只有在验收矩阵全部通过且无
   必要工作遗留时才能同步为 Goal complete。
7. 失败不会自动变成新 hotfix；先把完整事实、影响范围和唯一下一步写入本文件，再
   决定是否修改代码。

## 0.1 旧 Goal 达成审计（2026-08-09）

结论：`partially_achieved`，不能标记 `complete`。

已达成：

- 固定角色和固定端口：Active=8000，Candidate=8001/18002，不交换角色。
- `prepare-candidate`、`discard-candidate`、`update-active`、`rollback-active` 四操作及不可变
  release/manifest/指针合同已进入 main，并通过本地与 CI 测试。
- Candidate 固定上海 HTTPS 入口与外层门禁已经建立；Candidate 失败不会自动切 Active，
  也不会触发 intl。
- 上一次服务器 Candidate 证明了 3G/4G、后台单实例任务禁用、Active 公网保持原版本。

尚未达成：

- 独立可写沙箱代码已随 #219 合入 main，但首次服务器初始化尚未执行，当前没有承载最新
  main 的可写 Candidate。
- 上线前审计发现 8001 仍会以 root 运行并继承 Active `backend.env`；必须先合并本独立
  hotfix，使 Candidate 使用动态非 root 身份、清空继承的 Active env，并禁用共享 Redis。
- PostgreSQL 专用角色、Active CONNECT ACL、bootstrap env 和新版 8001 drop-in 仍需在
  Candidate 停止状态下一次性初始化并验证。
- `update-active` 尚未在当前 V2 服务器链路完成一次用户批准后的正式切换验收。
- 尚未形成 distinct `active.previous`，因此真实服务器 `rollback-active` 也未完成端到端验收。
- 当前 Candidate 运行的代码不等于最新 main；这是“main 可持续前进、Active/Candidate 需人工
  部署”的预期状态，但意味着旧 Goal 的最终验收矩阵尚未闭环。

#219 已原位把 Candidate 数据边界从“生产库只读”改为“生产快照的独立可写沙箱”，并补齐
Candidate 应用免登录。本 hotfix 只关闭首次上线审计发现的运行身份和 Active secrets 泄漏，
不新增 workflow、操作或状态机。合并后继续执行用户已经授权的腾讯云初始化与
`prepare-candidate`；仍不会运行 `update-active` 或同步 intl。

## 1. 目标

把当前不断叠加 checkpoint、incident recovery、hold/fence 和 transient unit 的发布体系，收敛为一个可理解、可测试、可回退的固定角色模型：

```text
新 main immutable release
        |
        v
prepare-candidate -> 固定 Candidate（8001 / 国内人工预览）
        |
        +-- 不满意 -> discard-candidate -> Active 完全不动
        |
        +-- 满意 -> update-active -> 固定 Active（8000 / 正式公网）引用同一 release
                                      |
                                      +-- 启动或健康失败 -> 自动恢复 Active previous
                                      +-- 上线后需要回退 -> rollback-active
```

角色始终固定：

- Active 永远承接正式公网，固定使用端口 8000。
- Candidate 永远用于腾讯云真实环境中的人工页面测试，固定使用端口 8001。
- Candidate 不与 Active 交换角色，也不直接接管公网。
- Candidate 测试通过后，Active 直接引用 Candidate 已验证的同一个不可变 release；不重新构建、不重新上传、不重新组装。
- Candidate 每次 prepare 从 Active 数据库取得一致性快照，恢复为独立可写沙箱；运行期间
  不连接、也没有权限连接 Active 数据库。Candidate 测试写入只保留在该沙箱中。
- Candidate 必须禁用月更 worker、scheduler 等单实例后台任务；应用认证只在 Candidate
  runtime 禁用，使固定测试入口直接进入 admin UI，Active 认证合同不变。

## 2. 用户可见的四个操作

### 2.1 `prepare-candidate`

1. 获取唯一部署锁。
2. 在改变服务器运行状态前完成所有输入预检。
3. 增量上传完整不可变 release 的变化块到临时路径。
4. 校验 archive 大小、SHA-256 和 manifest。
5. 解包到 staging，完成校验后原子改名为 content-addressed release 目录。
6. 使用 `pg_dump` 从 Active 取得一致性快照，恢复到新建的 Candidate 专用数据库；验证
   schema revision、Candidate 写权限及 Active 数据库拒绝连接后，才生成新的 Candidate env。
7. 记录 `candidate.previous`，原子更新 release 指针和数据库 env，再重启固定 Candidate 8001。
8. 验证 `/healthz`、运行 SHA、数据库身份、3G/4G cgroup、Candidate 单实例任务禁用，以及
   固定预览入口 18002。
9. 全部验证成功后删除旧 Candidate 数据库；稳态只保留一个，切换窗口最多两个。
10. 失败时删除本次新数据库并恢复旧 Candidate 指针/env；若原来没有 Candidate，则停止
    8001。Active、生产数据库与公网不动。
11. 若旧 Candidate 的 pointer/env/runtime/preview 身份本来就不一致，在任何指针改写或
    服务重启前拒绝；用户运行既有 discard 后再 prepare，不引入恢复状态机。

### 2.2 `discard-candidate`

1. 获取唯一部署锁。
2. 停止固定 Candidate 服务。
3. 先按受限 `jato_candidate_*` marker 删除 Candidate 专用数据库；删除失败时保留
   pointer/env/preview 身份以便同一操作安全重试，不把失败伪装成已清理。
4. 数据库删除成功后清除 Candidate 当前/上一版本指针和预览缓存；root-owned 0600 数据库
   env 保留原专用角色凭据，但仍指向已删除的沙箱，因此 8001 在下一次 prepare 前 fail-closed。
5. 仅删除不再被任何受保护指针引用的 release/staging。
6. Active、Nginx 公网路由、Active 数据库和 JATO 数据不动。

过渡期例外：legacy Active 尚未登记为 V2 release 时，discard 只停止 Candidate 并清除
Candidate 指针，暂缓 release GC；这样不会把 store 外的现网 Active 误判为未引用版本。
首次 Active 迁移完成后才恢复普通四指针 GC。

### 2.3 `update-active`

1. 获取唯一部署锁。
2. 只接受当前 `candidate.current` 指向且已通过人工验收的 release。
3. 核对 release SHA、archive SHA-256、manifest、数据库 revision 兼容性。
4. 若 JATO 发布/写入任务正在执行则拒绝重启 Active；不建立额外 quiescence 平台。
5. 记录 `active.previous`，原子更新 `active.current`。
6. 使用 Active 专属环境重启固定 Active 服务 8000；不能复制 Candidate 的禁用 worker 环境。
7. 验证内部 `/healthz`、预期运行 SHA、6G/8G cgroup、Active 单实例任务状态以及公网健康。
8. 任一步失败，立即恢复旧 `active.current` 并重启旧版本，再验证公网。

### 2.4 `rollback-active`

1. 获取唯一部署锁。
2. 必须接收用户从只读预检确认的 commit/archive/manifest 三元组；目标只能等于仍受
   `active.current` 或 `active.previous` 保护的 release，不能隐式猜测 previous。
3. 以 `B/A` 回退到 A 时，使用内核原子交换一次得到 `A/B`；因此不存在 `A/A`、
   `B/B` 或任一版本失去四指针保护的中间态。服务器不支持原子交换时变更前拒绝。
4. 以 Active 专属环境重启 8000，验证内部健康、目标 SHA、6G/8G cgroup 和公网健康。
5. 控制器捕获到的失败再次原子交换回 `B/A` 并验证 B；若进程被强制终止，持久状态只会
   是 `B/A` 或 `A/B`。同一 A 的显式重试不会 toggle；反向切换 B 必须重新明确授权。
6. 回退失败时保持结构化失败报告，不删除任一受保护 release。

## 3. 服务器目录与指针

建议的最小持久状态：

```text
/opt/jato/releases/<commit>/<archive-sha256>/
/opt/jato/slots/8000/current   # Active current
/opt/jato/slots/8000/previous  # Active previous
/opt/jato/slots/8001/current   # Candidate current
/opt/jato/slots/8001/previous  # Candidate previous
/opt/jato/staging/
/opt/jato/operation-reports/
<inventory-resolved existing production lock>
```

四个指针都使用现有 `/opt/jato/slots` 单一命名空间，不再另建
`/opt/jato/pointers`。这样可直接复用固定 8000/8001 systemd 模板，并避免服务启动、
GC 和回滚各自读取不同权威指针。指针应为 root 管理的原子 symlink；release 目录在
校验完成后只读。操作报告是追加写的结果证据，不参与下一次操作的准入判断，因此
不会再出现“旧 checkpoint 阻断新 Candidate”的全局耦合。

V1/V2 并存期间，V2 必须复用服务器 inventory 解析出的现有生产 flock 路径；否则
两套 workflow 仍可能同时修改服务。只有 V1 完全删除后，才能通过一次显式迁移把锁
统一到 `/run/lock/jato-release.lock`。锁路径不从历史 checkpoint 推断。

## 4. 必要门禁

只保留以下门禁：

- 生产 workflow 只允许来自 `main`，且四个操作均为人工触发。
- 单一部署锁。
- archive / manifest / SHA-256 完整性。
- Candidate 固定 3G/4G，且月更 worker、scheduler 等单实例任务禁用。
- Candidate 专属 env 必须显式写 `APP_JATO_MONTHLY_ENABLED=false`；不能再通过
  legacy active-slot 不匹配来间接禁用。prepare 同时检查 env 与实际 HTTP 423
  `reason=explicitly_disabled`；腾讯云验收另外只读确认 Candidate cgroup 内没有月更
  worker 进程，不在控制器内建立 `/proc` 监管状态机。
- Active 专属 env 单独写 `APP_JATO_MONTHLY_ENABLED=true`，不能复制 Candidate env。
- Active 固定 6G/8G；更新前后均验证健康与预期运行 SHA。
- Candidate prepare 先从 Active 只读快照，在新沙箱中运行目标 release 的 migration，并验证
  沙箱 `current=heads`；Active 数据库本身不执行 migration。
- `update-active` 与 `rollback-active` 继续只读比较正式数据库 `current/heads`；不一致时报告
  `migration-required` 并拒绝更新 Active。
- Nginx 正式公网始终指向固定 Active 端口 8000。
- Active 重启前检查是否有正在执行的 JATO 发布/写入任务。

单独返回 HTTP 200 不足以证明版本正确；健康检查必须同时证明运行 release SHA。

### 4.1 Candidate 使用生产快照的可写沙箱边界

Candidate 的价值是让新代码面对服务器上的真实数据量、真实 PostgreSQL schema、索引、
查询计划和写入交互，同时绝不把测试写入生产。每次 `prepare-candidate` 都从 Active 创建
新的独立可写数据库沙箱，而不是让 Candidate 连接生产数据库：

- 使用 PostgreSQL `pg_dump`/`pg_restore` 获取事务一致性快照；不使用会长时间阻塞连接或
  依赖空闲数据库的 `CREATE DATABASE ... TEMPLATE`。
- 新数据库使用不可复用的操作标识命名；恢复、revision、权限和应用健康全部通过后，才
  原子替换 root-owned 0600 的 `/etc/jato-fullstack/candidate-database.env`。
- Candidate 应用角色只在新沙箱拥有普通业务写权限，并保持 NOSUPERUSER/NOCREATEDB/
  NOCREATEROLE/NOREPLICATION/NOBYPASSRLS。角色必须对 Active 数据库无 CONNECT；不能仅依赖
  `PUBLIC CONNECT` 的默认状态，prepare 必须显式验证拒绝连接。
- Candidate env 使用独立强随机 JWT secret，应用级 `APP_AUTH_ENABLED=false`，使固定测试
  入口无需第二次应用登录并直接获得 admin UI。该设置只能存在于 8001 runtime，不能固化
  进 Active artifact 或 Active env；固定 HTTPS 网关仍保留独立访问控制。
- systemd 继续使用 `ProtectSystem=strict`、无 Linux capabilities、PrivateTmp；
  `/opt/jato/shared` 及 legacy raw/processed 目录保持只读，仅 Candidate cache 和 Candidate
  数据库可写。
- Candidate 继续禁用月更 worker、scheduler、Hermes 和所有预热后台任务；对数据库以外的
  邮件、外部 webhook、对象存储和管理操作不做虚假的“万能沙箱”承诺，未隔离的外部副作用
  仍不得在 Candidate 触发。
- FIFO 容量为 1：稳态只保留当前 Candidate 数据库；换新期间允许旧/新两个数据库并存。
  新 Candidate 完整验证成功才删除旧数据库；任一步失败就删除新数据库并保留旧 Candidate。
- `update-active` 只让 Active 使用已经测试的同一不可变 release，永不复制 Candidate env、
  Candidate JWT secret 或 Candidate 测试数据。`rollback-active` 同样不读取 Candidate 数据库。

Active 可以长期落后 `main`。`main` 的新提交只会生成新的 Candidate；只有用户人工
验收的精确 commit/archive/manifest 三元组才能进入 `update-active`，不会自动追最新
main，也不会因 Candidate 重建而改变 Active。

### 4.2 发布授权与既有 intl 同步边界

唯一允许的发布关系是：

```text
main -> Candidate（可反复替换测试）
     -> 用户明确批准精确构件
     -> www Active 使用同一构件

www Active -> 既有独立 sync-www-active-to-intl -> intl
```

- Candidate 不合格时，继续修改代码、合并 `main`、重建 Candidate；Active 可以长期
  停留在旧稳定版本，提交数落后本身不是故障。
- `prepare-candidate`、`discard-candidate` 以及 `main push` 均无权修改 Active 或 intl。
- “批准上线”是一笔显式发布授权，必须绑定已人工验收的 commit/archive/manifest
  三元组；www Active 不允许重新构建、重新上传或改用另一个最新 SHA。
- `update-active` 只更新并验证 www Active，不自动排队、调用或修改 intl 同步。
- intl 沿用既有 `sync-www-active-to-intl`：在用户另行启动时，从当时的 current Active
  导出并同步；V2 不新增输入、receipt、自动 dispatch 或跨 workflow 状态。
- intl 同步失败不影响已经成功的 www Active，也不得触发 www 回退；这是两个独立操作。
- Candidate 永远不能自动升级为 Active，也不能触发 intl。Active mutation 必须可追溯
  到用户对该精确已测试构件的明确批准。

现有完整包没有独立的 top-level release manifest；只有 runner 计算的 archive
bytes/SHA、`hermes/deploy_release.json` 和 frontend manifest。V2 因此新增一个小型
sidecar `release-v2-manifest.json`，至少绑定：

- schema version、repository 和完整 main commit SHA；
- archive bytes 与 archive SHA-256；
- frontend artifact identity、checksum 和 frontend build ID；
- archive 内已经固化的 build metadata 摘要。

sidecar 本身另算 SHA-256，并把该摘要作为服务器控制命令的显式输入。服务器先校验
sidecar bytes/SHA，再校验 archive；不能把现有内嵌 metadata 误当作已存在的顶层
manifest 契约。

GitHub run ID/attempt、操作者和操作时间只写入追加式 operation report，不写进
release manifest。这样同一不可变 archive 在 workflow retry 时仍是同一个 release，
不会重演“稳定构件绑定易变执行事实，正常重试被判篡改”的旧问题。

## 5. 明确不再建设的内容

- 每个事故一套专属 workflow、plan、hold 或硬编码 incident SHA。
- 扫描全部历史 checkpoint 后才能开始下一次发布。
- 多阶段 recovery fence/hold 平台。
- 每个 Candidate 动态创建 transient runtime systemd/Nginx 身份。
- 物理蓝绿槽位互换。
- 数十阶段 checkpoint 状态机。
- Candidate 测试成功后再次上传或重新组装 Active release。

历史 V1 在 V2 验收前只做禁用和隔离，不立即删除；V2 完成腾讯云端到端验收后，另开独立清理 PR 删除 V1 脚本、workflow、事故常量和测试。

### 5.1 复杂度预算

V2 必须保持在以下边界内；超过边界时先停下来重新设计，不能以“再加一个最小
hotfix”为理由扩展：

- 用户操作永远只有四个：prepare、discard、update、rollback。
- 持久运行状态只有四个 slot pointer 和不可变 release；operation report 只记结果，
  不能成为后续操作的输入或门禁。
- 服务器并发协调只有一个既有 production flock；不新增 hold、fence、checkpoint、
  journal 或 incident 状态机。
- V2 runtime 最多三个 Python 模块和一个薄 Bash 入口；3,500 行触发停止扩展并审查。
  4,200 行不再作为可被格式化或拆文件规避的字面硬上限，而是强制独立审查线。测试和文档
  不计入该预算。超过审查线后仍严禁增加 action、workflow、checkpoint、recovery、新模块或
  事故常量；只允许有真实服务器证据、净增不超过 60 行的现有职责根因修复，且后续永久能力
  必须先删除等量重复/废弃逻辑。该规则取代 Step 3B 的临时数字冻结，不再逐次抬高数字上限。
- 2026-08-09 已触发 4,200 行独立审查：结论是不新增 runtime module、action、workflow 或
  recovery；只允许在现有 `prepare/discard/admission` 中原位替换旧 SELECT-only Candidate
  路径，并同步删除旧只读 probe/断言。若实现出现第二套凭据存储、第五个操作或事故状态，
  必须立即停止。最终净行数和删除的旧路径须在本节记录，不能用拆文件规避审查。
- P1/P2 根因修复后的三个 runtime 模块为 4,882 行：controller 净增 501 行，admission
  净增 125 行，总净增 626 行；旧 same-database/SELECT-only probe 与 prepare gate 已原位
  删除，不是并排保留第二套路径。该数字超过旧 `+60` bug-fix 预算，因此不能描述成“小修复”；
  它是用户明确批准的 Candidate 数据边界替换，经写前复杂度审查后以“零新 action、零新
  workflow、零新 runtime module、零 recovery 状态”收口。相较初稿增加的 156 行用于消除
  root 临时 dump、postgres 执行目标代码、默认集群命令和不可收敛 orphan 四个真实问题，
  已触发最终独立复审；除测试与诊断修正外不再扩大 runtime。4,882 是当前冻结事实，后续若
  还需增加发布控制能力，必须先删除旧 V1/重复代码或重新取得范围决策。
- 一个统一的结构化 operation report schema；失败必须包含 passed、failed、
  notReached、before/after 和 mutation flags。
- Candidate/Active 使用固定 systemd unit 与固定 Nginx 配置；每个 release 不创建
  transient runtime unit、Nginx identity 或事故专属 workflow。`prepare-candidate` 允许
  一个无监听、无指针、完成即销毁的 3G/4G transient dependency-build scope；它只限制
  依赖构建资源，不是运行槽或持久身份。
- 任何新需求若不能表述为四操作之一的预检、执行或验证，必须放到独立系统，不得
  塞进发布控制器。

## 6. Release 保留与垃圾回收

垃圾回收必须保护下列四个指针：

- `active.current`
- `active.previous`
- `candidate.current`
- `candidate.previous`

同一 release 可被多个指针引用，所以正常只保留 2–3 个唯一版本。V2 垃圾回收先生成
mark-and-sweep 计划；只要 release store 出现未知条目就拒绝清理。确认某个 V2 release
未被四指针或当前操作引用后，直接删除该 content-addressed 目录，不实现额外 trash
状态机或恢复队列。失败 staging 由上传/构建边界清理，不进入 release store。垃圾回收
不读取旧 checkpoint，也不删除 legacy 或缺少有效 V2 manifest 的目录。

传输 archive cache 只识别 `<cache>/<40-hex>/<64-hex>.tar.gz` 及其 `.partial`、`.sha256`
文件。其保护集合同样只来自四指针；删除未保护身份前必须非阻塞取得对应的既有永久
`.lock`。lock 正忙、缺失、symlink 或路径不安全时保留文件并报告，绝不删除 `.lock`。
四操作成功后 best-effort 清理未保护的 archive/partial/sha；清理失败不逆转已经成功的
指针或服务操作。

## 7. 当前事实基线

截至开始实施时，仓库和历史 Actions 审计确认：

- 当前远端 `main` 为 `37a9905c…`；线上已知健康 Active 为较旧 `cd4557cb…`。
- 当前新 Candidate 链路没有一次完整端到端成功记录。
- 旧流程存在 `c354a2d3…` 和 `392a5256…` 的 checkpoint/release/preimage 残留记录。
- 最新清理被 `392a…` checkpoint 的 `Permission denied` 阻断，并被错误包装成 invalid JSON。
- 旧 Active 没有新流程期望的 `/readyz`、durable active symlink 和 Nginx include。
- 现有流程会扫描所有历史 checkpoint；任一旧文件权限或状态异常都会阻止新的 Candidate。
- 当前代码明确拒绝 legacy Active 首次更新，且 workflow 没有可完成该 bootstrap 的日常入口。
- 事故链中未发现 Active 被切换、数据库迁移或 JATO 数据被修改的证据；fail-closed 有效，但日常发布不可用。
- 已只读确认 8001 未运行且 18002 未监听；旧 checkpoint 不进入 V2 准入。首次迁移前
  仍必须确认当前 Active 对应的原始 content-addressed archive 是否唯一存在，以及旧
  Nginx boot-reconcile drop-in 的准确状态。

## 8. 迁移策略

迁移分两阶段，不把删除旧系统混入首次可用版本：

### 阶段 A：V2 Candidate 无流量验收与首次 B/B 更新

1. 完成本地实现、范围审计、独立 PR 与 CI。服务器只读预检确认 legacy Active anchor 精确为
   `/opt/jato/slots/8000/current -> /opt/JATO_Analysis_System-main`、公网固定指向 8000，并盘点
   8001 的 unit/drop-in/env、Candidate bootstrap role、Active CONNECT 拒绝和共享路径对动态
   UID 的可读/可遍历权限；未知配置直接报告，不建立 recovery。
2. 经用户单独授权配置 Candidate 沙箱管理员/应用角色后，从最终 main 运行普通
   `prepare-candidate` 得到 B。prepare 从 Active 一致性快照恢复独立可写沙箱，只安装/使用
   固定 8001/18002，前后证明 legacy Active、生产数据库与 www 身份不变。
3. 用户通过 18002 人工测试真实服务器页面。不满意就 `discard-candidate` 或合并新代码
   后再次 `prepare-candidate`；Active 可以长期停在 legacy。
4. 只有用户对 B 的精确三元组单独授权，才运行普通 `update-active`。首次成功直接得到 B/B；
   同一操作
   安装/核验固定 8000 合同与兼容别名 `/opt/jato/active`，让未修改的
   `sync-www-active-to-intl` 之后仍从 current Active 导出。
5. 可捕获的 update 失败恢复 legacy unit/env/current/compat 与公网身份；成功后 www 已完成，
   不等待 intl。B/B 在下一次 C/B 前没有 distinct rollback。旧 checkpoint/journal/evidence
   保持原位且不参与 V2 准入或 GC；断电/SIGKILL 中间态只人工盘点，不建设自动 recovery。

### 阶段 B：删除 V1 深层实现

只有阶段 A 的 prepare/discard/首次 update，以及后续 distinct previous rollback 全部在服务器通过后，
才另开清理 PR：

- 本阶段 A 已从生产调用面删除 recovery/settlement 事故 workflow；阶段 B 再删除其
  不可达的底层脚本、hold/fence 源码和只服务于旧状态机的内部实现。
- 删除只服务于旧状态机的单测与事故常量。
- 更新腾讯云部署文档和 Candidate 人工操作手册，以 V2 为唯一权威路径。

## 9. 验收矩阵

| 场景 | 预期结果 | Active 是否变化 |
|---|---|---|
| archive SHA 错误 | prepare 在解包/启动前拒绝 | 否 |
| DB revision 不一致 | 返回 migration-required | 否 |
| Active 快照或新沙箱恢复失败 | 删除新沙箱，保留旧 Candidate | 否 |
| Candidate 仍能连接 Active 数据库 | prepare 在启动前拒绝 | 否 |
| Candidate 沙箱写权限不足 | prepare 在启动前拒绝 | 否 |
| Candidate env 缺少独立 JWT / 免应用登录配置 | prepare 在启动前拒绝 | 否 |
| 新 Candidate 完整验证成功 | 删除旧沙箱，稳态只保留当前沙箱 | 否 |
| Candidate 启动失败 | 恢复旧 Candidate 或停止 8001 | 否 |
| Candidate 健康但 SHA 不符 | prepare 失败并清理 | 否 |
| Candidate worker 意外启用 | prepare 失败并清理 | 否 |
| 人工页面不满意 | discard 后 8001 停止 | 否 |
| update 前存在 JATO 写任务 | 拒绝重启 | 否 |
| 首次 B/B update 启动或公网验证失败 | 恢复 legacy preimage 并验证公网 | 短暂重启后回 legacy |
| 普通 C/B update 启动或公网验证失败 | 自动恢复更新前 Active | 短暂重启后回旧版 |
| B/B 请求 rollback | 返回 rollback_unavailable，零修改 | 否 |
| distinct previous rollback 成功 | 原子交换 current/previous，公网健康 | 是，显式回退 |
| GC 执行 | 四指针引用的 release 均保留 | 否 |
| archive cache lock 正忙或路径不安全 | 保留对应 cache 并输出诊断；不删除 lock | 否 |

固定 8000 意味着 `update-active` 和 `rollback-active` 要重启正式 backend，会有一个
有界的短暂不可达窗口。Candidate 验证和自动 pointer 恢复降低失败风险，但不能提供
真正零停机；绝对零停机需要保留两个可接公网的 Active 槽位和 Nginx 端口切换，这与
本 V2 的固定角色目标冲突。本计划接受短重启窗口，不再伪装成零停机蓝绿。

## 10. 实施日志

### 2026-08-09 / Step 3L：Candidate 可写数据库沙箱初稿被独立审查拒绝

- 用户确认新的测试边界：Candidate 不再直连 Active 数据库，而是在每次 prepare 时使用
  最新生产一致性快照生成独立可写沙箱；测试写入允许发生，但只能留在沙箱中。
- 生命周期采用 FIFO 容量 1：稳态一个 Candidate 数据库，切换时最多旧/新两个；新版本
  完整验证后删除旧库，失败时删除新库并继续保留旧 Candidate。没有新增第五个发布操作、
  checkpoint、recovery fence 或数据库控制平台。
- Candidate 固定链接在应用层不再要求登录，8001 专属 runtime 使用
  `APP_AUTH_ENABLED=false` 并返回 admin；Active 的认证设置和账号数据保持不变。网关访问
  控制仍保留，因为 Candidate 暴露的是生产快照。
- 已从远端 `main@619466e8` 创建干净 worktree
  `/Users/litristan/.codex/worktrees/candidate-writable-sandbox-fifo/JATO_Analysis_System` 和分支
  `codex/candidate-writable-sandbox-fifo`。实现严格留在既有 admission/controller、固定 8001
  systemd 合同和 Candidate 前端认证路径内；没有新增 runtime module、发布操作、workflow、
  checkpoint 或 recovery。
- `prepare-candidate` 现通过磁盘 staging 执行 `pg_dump --format=custom`、新建安全前缀数据库、
  `pg_restore --single-transaction`、目标 release Alembic upgrade 与直接最小权限授权；URL、
  密码和 JWT 不进入 argv、报告或对象 repr。新 env 通过 root-owned 0600 原子替换，Candidate
  健康、SHA、权限、后台任务和 preview 快照身份全部通过后才删除旧沙箱。
- admission 现证明同 PostgreSQL 集群但不同 database/role、角色无 superuser/createdb/
  createrole/replication/bypassrls 或继承 membership、沙箱 DML/sequence 权限完整、database/schema
  CREATE 被拒绝，并从 Candidate 连接内证明它对 Active database 没有 CONNECT。
- Candidate 前端用统一 origin helper 识别固定域名/18002；初始化时清除残留 Active token，
  以 `candidate/admin` 调用 `/auth/me`，忽略 Candidate URL 中的 OAuth token。页面顶部同时显示
  commit、artifact 和生产数据库快照时间；Active origin 的 token、延迟 refresh 与登录行为不变。
- 失败矩阵覆盖 dump/provision 后的 Candidate 恢复、FIFO、旧库删除失败回退、恢复失败保留新库、
  discard 删除失败可重试、恶意 marker 拒绝、update-active 不复制 Candidate env，以及 Candidate
  角色/权限每一项 fail-closed。controller + admission 聚焦测试 `123 passed`；全部
  `03_Scripts/tests` 为 `1273 passed, 15 skipped`；前端 `70` 个文件、`373` 项测试、类型检查、
  production build 和 router regression 全部通过；两个 production workflow validator 通过。
- 首次服务器 prepare 前仍有两个需单独授权的配置动作，不能由 PR 自动暗改：其一，把现有
  SELECT-only Candidate 角色从 `pg_read_all_data` 等 membership 收紧为无继承角色，并使它对
  Active database 的真实 CONNECT 检查失败；其二，在 Candidate 停止时原子替换线上历史
  `20-candidate-readonly.conf` 为新的 sandbox 合同并 `daemon-reload`。随后第三道独立授权才是
  真正运行 `prepare-candidate` 创建首个可写快照沙箱。
- 当前尚未 stage、commit、push 或创建本 Candidate PR。没有部署、创建数据库、修改
  PostgreSQL ACL、替换 systemd drop-in、重启服务或触碰 Active/intl/JATO 数据。先前完整
  前端套件在另一审查进程同时占满 CPU 时出现 31 个无关超时；并发结束后以相同命令重跑
  全部 `373/373` 通过，确认不是产品回归。
- 独立静态审查结论为“无 P0、两个 P1、两个 P2”，因此上述测试通过不等于可部署，Draft PR
  也不能创建：
  - P1：root 创建的 `mktemp` 目录默认 0700，root `pg_dump` 写出的文件无法由
    `runuser -u postgres -- pg_restore` 遍历/读取，真实 prepare 会稳定失败；现有 mock 只检查
    命令文本，没有覆盖 Unix 权限。
  - P1：目标 release 的 Alembic Python 当前通过 `runuser -u postgres` 执行；这让未经上线的
    Candidate 代码获得数据库超级用户能力，可绕过后置的 `activeConnectDenied` admission，
    与“Candidate 永不写 Active”核心目标矛盾。
  - P2：`createdb/pg_restore/Alembic/psql/dropdb` 尚未显式绑定已经验证的 host/port，多集群或
    非默认端口环境可能误操作默认 Unix socket 集群。
  - P2：外层 timeout/SIGKILL 只终止 Bash 时，trap 不能作为必然清理保证；新数据库可能成为
    orphan，且当前 mutation/report 无法准确证明 FIFO 稳态。
- 唯一允许的下一步是在既有 provision/prepare 内修正这四点并补真实权限/命令边界测试；不得
  借机新增 action、workflow、checkpoint、recovery 或第二套数据库控制面。修复后必须重新
  独立审查，无 P0/P1 才能把本 Step 改回“本地实现完成”。
- BOM Colour Library 的 code→规范名称/swatch 自动补全与 BOM/选品统一渲染已登记为后续
  独立 [Draft PR #218](https://github.com/tristan419/JATO_Analysis_System/pull/218)。该 PR 当前仅含
  Goal/审计；因 `#215` 正在拥有 `create_material_sku`、BOM 测试和 `OrderGeniusPage.tsx` 的共享
  修改，颜色业务实现须等 `#215` 合并并从届时最新 main 重建，不能跨 PR 覆盖。

### 2026-08-09 / Step 3M：P1/P2 根因修复与真实 PostgreSQL 本地验收

- 初稿的 root 0700 dump 文件已删除，快照改为 `pg_dump` 到 `pg_restore` 的直接流式传输；
  restore 和目标 release Alembic 都使用 Candidate 数据库角色，Alembic 进程使用受限 OS
  用户 `nobody`。本机 peer `postgres` 只执行固定的角色预检、建库、权限收口、发现和删库，
  不执行任何目标 release Python。
- 所有 PostgreSQL 管理命令都绑定由 Active/Candidate URL 验证出的端口与固定本机 socket；
  dump/restore 使用拆分的 `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE` 环境，命令行只出现
  非敏感数据库名。`createdb` 明确使用 `template0` 和随机安全 marker 目标库。
- `prepare` 和 `discard` 复用现有四操作及 production flock，通过严格
  `jato_candidate_*` marker 发现数据库，并保护 Active、Candidate env 与 preview 正在引用的
  数据库。下一次 prepare 会删除未引用 orphan；discard 停止 Candidate 后删除安全命名空间内
  的沙箱。实际删除集合进入 mutation/report，强杀后不依赖 trap、checkpoint 或 recovery
  状态机才能收敛。
- Preview 身份现在同时绑定 release SHA、artifact、`databaseName` 和 snapshot UTC；新 Candidate
  失败时先恢复并验证旧 Candidate，只有恢复成功才删新库。旧库清理失败则整次 prepare 回退，
  不把两个沙箱当成成功稳态。
- 真实本地 PostgreSQL 16 临时集群验收先后暴露并修复三个 mock 没抓到的问题：
  1. `PGDATABASE` 不能塞完整 URL，且 `pg_restore` 必须明确指定目标数据库；
  2. `psql --command` 不展开 `--set` 变量，固定 SQL 必须通过 stdin/`--file -` 执行；
  3. psycopg 参数化查询会把 `LIKE 'pg_%'` 的 `%` 解释成非法占位符，已改成等价的 schema
     前缀判断。
- 修复后的同一临时集群真实完成：Candidate 角色预检、Active 全库 snapshot、single-transaction
  restore、Candidate Alembic、对象权限收口、14 项 writable/isolation admission 证明；最终
  `alembic current` 与 `heads` 都为 `20260715_0046 (head)`。该验证只使用本机临时数据库并在
  结束后删除，没有访问或修改腾讯云、Active、intl、JATO 数据或线上 PostgreSQL ACL。
- controller + admission 聚焦测试现为 `129 passed`，显式覆盖 createdb 碰撞不误删、pipeline
  timeout/进程消失竞态、未引用 orphan 收敛及 Active/env/preview 引用保护、partial cleanup
  失败后的 `removed` 与 `databaseChanged=true`。全部 `03_Scripts/tests` 为
  `1279 passed, 15 skipped`；前端 70 个文件、373 项测试、类型检查、production build、router
  regression 及两个 production workflow validator 全部通过。
- 最终独立复审确认初稿的两个 P1、两个 P2及后续发现的三个 P2 均已关闭，当前无 P0/P1，
  也没有剩余可行动项：inactive Preview metadata 漂移会在任何数据库 mutation 前拒绝；前端
  严格验证并显示沙箱 marker；discovery SQL 与其他固定 SQL 一样通过 stdin/`--file -`。
- 本地实现至此可以提交 Candidate Draft PR；仍不进行服务器配置、数据库 provision、Candidate
  prepare、Active 更新或 intl 同步。

### 2026-08-09 / Step 3N：Candidate 可写沙箱 Draft PR

- 已从干净基线 `main@619466e81528045f59ea64ad9bcdf69c60a219f8` 精确提交 12 个本业务线
  文件，初始 commit 为 `0949dcdb39a2d12f88e678f0daa0b0563d33048a`；未包含混合观察区、
  Hermes 开发事件或其他业务线文件。
- 已创建 [Draft PR #219](https://github.com/tristan419/JATO_Analysis_System/pull/219)，base 为
  `main`，head 为 `codex/candidate-writable-sandbox-fifo`。PR 保持 Draft，仅进入 GitHub checks；
  不因创建 PR 自动部署生产。
- 本步骤没有配置腾讯云 Candidate 角色/ACL，没有替换 8001 drop-in，没有运行
  `prepare-candidate`，也没有修改 Active、intl、生产数据库或 JATO 数据。
- 下一步只记录 GitHub checks。转 Ready、合并、服务器一次性合同迁移及首次可写 Candidate
  prepare 均需后续分别授权。

### 2026-08-09 / Step 3O：GitHub frontend 单测环境隔离修复

- #219 初次 GitHub `fullstack-frontend` 失败于
  `candidateAuthBootstrap.test.tsx` 的 Active 无令牌用例：workflow 固定注入
  `VITE_AUTH_TOKEN=ci-token`，而 Active 既有开发令牌逻辑会把角色设为 `admin`；测试却依赖
  本机未设置该变量并期待已存储的 `editor`。Candidate bootstrap、类型检查及其他测试没有在
  该日志中失败。
- 根因属于新增单测的环境泄漏，不是 Candidate 或 Active 生产代码错误。按已批准范围只在该
  单测中显式将无令牌场景的 `VITE_AUTH_TOKEN` 设为空，并在 `afterEach` 恢复所有 stubbed env；
  没有修改 `AuthContext`、Candidate runtime、后端、workflow 或部署脚本。
- 使用 GitHub 相同的 `VITE_API_BASE=/v1`、`VITE_AUTH_TOKEN=ci-token`、
  `VITE_USER_ROLE=admin`、`VITE_USER_NAME=github-actions` 完整执行 `npm run check:frontend`：
  70 个测试文件、373 项测试、TypeScript、production build 与 router regression 全部通过。
- 测试修复与 Goal 已提交为 `c1a87b47eea0d94d1dc3bbc01b9efb17d39a6c73` 并推送 #219；
  下一步只观察最新 head 的 checks，仍不转 Ready、不合并、不部署、不配置服务器。

### 2026-08-09 / Step 3P：#219 Draft CI 终态

- #219 的 `b80bcb932c4e4d7eaa941169e5b06a6c10f219b6` 已完成两套
  `fullstack-frontend`，均为 success；此前依赖 CI 环境的 Candidate auth bootstrap 断言已稳定
  通过。
- 同一 SHA 的 `fullstack-backend`、`smoke`、`frontend-release-contract`、
  `production-deployment-guard`、`release-coordination-evaluator` 及外部 Cloudflare Pages Preview
  均为 success，没有剩余失败或运行中 check。
- PR 继续保持 Draft。CI 全绿只代表本地/Runner 验证完成，不授权转 Ready、合并、腾讯云合同
  迁移、Candidate prepare、Active 更新或 intl 同步。

### 2026-08-10 / Step 3Q：同步已合并 #215 并复验组合树

- BOM Admin PR #215 已在新 head 的 required checks 全绿后合并为
  `main@40ae32112927b3e138a88e42cd43ccc611f4ba0f`。合并只更新 GitHub main，没有启动
  production release、Candidate、Active、intl、数据库或 JATO 操作。
- #219 与 #215 的修改文件交集为零。#219 已同步 #215 的最终代码树且无冲突；相对该 main
  仍精确保留原 12 个 Candidate 沙箱文件，没有把 BOM 文件或其他业务线带入 #219 差异。
- 同步后的本地组合验证为：全部 `03_Scripts/tests` 共 `1279 passed, 15 skipped`；前端
  `71` 个测试文件、`375` 项测试、TypeScript、production build 与 router regression 全部
  通过；两个 production workflow validator 通过，`git diff --check` 通过。
- 用户已明确授权按 `#215 -> #219` 顺序继续。下一步只推送 #219 同步 head 并等待 required
  checks；全绿后才转 Ready 和合并。服务器角色/ACL、8001 drop-in、首次可写 Candidate
  prepare、Active 更新及 intl 同步仍未获本步骤授权，继续保持不变。

### 2026-08-06 / Step 0：目标与开发边界

- 已创建持续目标。
- 已从远端最新 `main@37a9905c…` 创建独立 worktree 和 branch。
- 当前 worktree clean，未迁移混合观察区任何改动。
- 当前 PR scope 不包含业务前端、数据库迁移或 JATO 数据。
- 尚未创建 PR、合并、部署、清理服务器残留或切换流量。

### 2026-08-06 / Step 1：待执行只读盘点

下一步一次性收集：

- 所有 checkpoint/journal/evidence 的路径、phase/status/retryClass、UID/GID/mode/ctime 与父目录权限/ACL。
- 8000/8001/18002 的监听与 systemd 运行身份、ExecStart、环境、cgroup。
- Active/Candidate/slot/release symlink 实际目标。
- Nginx available/enabled/include 与公网 upstream。
- 月更 worker、scheduler/timer 的实际状态。
- Alembic current/heads，只读。
- releases、archives、checkpoints、preimages、backups、receipts 的空间占用。
- 正式公网健康、版本 SHA 和当前 Active 基线。

在这份 inventory 完成前，不执行线上清理、Candidate 启动、Active 更新或任何数据操作。

### 2026-08-06 / Step 1A：代码规模与首批结构事实

- 现有 `.github/workflows/production-release.yml` 为 3,099 行，并同时承担构建、
  增量传输、Candidate、Active 批准、Candidate 清理、www/intl 和事故证据。
- `tencent_bluegreen_release.sh` 为 6,407 行；主 workflow、recovery workflow、
  settlement workflow 和两个控制脚本合计 14,067 行。
- `03_Scripts/deploy` 当前约 38,566 行，`.github/scripts` 约 8,809 行；其中大量
  内容服务于旧 checkpoint、quarantine、preimage、receipt 和事故恢复状态机。
- 当前 `production-release` 在 `main` push 时自动选择 `prepare-candidate`，与
  V2 的“所有服务器操作均由用户明确触发”目标不一致。
- 现有 systemd 模板 `jato-fullstack-backend@.service` 已能固定使用 8000/8001、
  2 workers 和分槽 env，可作为 V2 固定服务模板的基础，不需要动态 transient unit。
- 现有 Candidate Nginx 模板已包含 18002、前后端同源代理和 identity endpoint；
  V2 将把它改为固定配置，不再为每次 Candidate 动态创建 systemd/Nginx 身份。
- `jato_quiescence_gate.py` 内的 `inspect_state()` 已能识别 JATO job、upload digest
  和 baseline promotion 的 busy 状态。V2 只复用这段只读判断，不复用 marker、
  hold、等待和 checkpoint 逻辑。
- 现有数据库门禁已使用 `PGOPTIONS=-c default_transaction_read_only=on` 比较
  Alembic current/heads；V2 保留只读比较语义，并把结果直接报告为 compatible 或
  migration-required。
- 旧 Nginx 可能仍安装
  `/etc/systemd/system/nginx.service.d/20-jato-bluegreen-boot-reconcile.conf`；它通过
  `Requires=` 绑定旧 boot reconcile。V2 若只停止调用旧脚本而不先处理中该 drop-in，
  后续 Nginx restart/reload 仍可能重新触发旧 active-slot 逻辑。服务器 inventory
  必须核对该文件，bootstrap 必须在同一原子操作中解除该依赖。
- `jato_monthly_update_service.py`、两条云同步脚本、Active frontend export 和 HTTPS
  安装脚本仍直接读取 `/var/lib/jato-release/active-slot`。V2 bootstrap 阶段不能直接
  删除该文件；必须逐项迁移到固定 Active 8000 / `active.current` 后，才能在 V1 清理
  PR 删除 legacy active-slot。
- Chrome、浏览器扩展和 native host 检查均正常，但腾讯云标签页当前未连接给控制
  会话；实时服务器 inventory 尚未执行，代码工作继续进行，线上事实不作推断。

### 2026-08-06 / Step 1B：只读 inventory 工具

> 历史记录：该临时 `inventory` 子命令随后在复杂度收敛时删除；服务器只读盘点改由
> 审查过的一次性命令完成。以下内容仅记录当时证据，不表示当前日常 controller 仍有
> 第五个 `inventory` 操作。

- 新增 `03_Scripts/deploy/fixed_release_v2.py inventory`。
- 该命令只使用 Python 标准库及只读系统命令，输出单一结构化 JSON；不会在服务器
  写 report、checkpoint、marker 或 receipt。
- 输出显式包含 `readOnly=true`、四类 mutation flag 全为 false，并把
  `permission_denied` 与 invalid JSON 分开，避免再次把权限问题误报成 JSON 损坏。
- 只读取 allowlist 环境变量，不输出数据库 URL、OAuth、token、API key 或其他密钥。
- 本机 smoke test 正常生成 `inventory_partial`；partial 是因为 macOS 没有服务器的
  systemd、Nginx 和监听端口，不是脚本崩溃。
- 同次只读公网探针确认 `https://www.ojeur.cloud/healthz` 为 HTTP 200，
  `build-meta.json` 仍绑定 `cd4557cb932374a0fefb6c80a5fac9fb75a67d62`。
- 腾讯云完整 inventory 仍待标签页连接后执行；在此之前不把本机 partial 当作服务器
  验收证据。
- 本轮再次检查时浏览器扩展仍未把腾讯云标签页连接给控制会话；未尝试其他绕过方式，
  未执行服务器命令。等待期间只推进不依赖线上布局的本地纯函数与测试。

### 2026-08-06 / Step 1C：Workflow、CI 与真实运行测试缺口

- 早期曾提议另建 `.github/workflows/candidate-release-v2.yml`；Step 2M 已撤回该方案，
  最终复用并收窄现有 `production-release.yml`，只暴露
  prepare/discard/update/rollback 四个显式操作，避免复制第二套上传和部署逻辑。
- `main push` 只触发 CI，不进入 production environment，也不自动启动 Candidate。
- prepare 才构建和增量上传；另外三个控制操作不构建、不打包、不上传 release，
  只发送受信任 V2 控制器和 expected identity。
- 所有生产 mutation job 使用 GitHub `production` environment、同一 concurrency
  group 和同一服务器 flock；intent guard 在进入 environment 前再次核对 ref/main。
- GitHub `production` environment 必须在仓库设置中限制 deployment branch 为
  `main`、启用 required reviewer，并把 SSH/Cloudflare 等生产 secret 只保存在该
  environment。仓库内 YAML/validator 可被 feature branch 自己修改，不是最终权限边界。
- CI 仍以伪造 systemctl/Nginx/curl 的事务单测、静态合同和 workflow validator 为主；
  真 systemd/cgroup、真实数据库角色与 8001/18002 必须在腾讯云无流量 Candidate 验收，
  不把 CI mock 冒充服务器证据。
- 腾讯云先单独覆盖 legacy Active 不变、prepare、人工页面、discard 与第二次 prepare。
  update/rollback 只有 Candidate 验收后且用户另行授权才测试；不能用函数单测绿色替代
  完整生命周期。该拆分由 Step 2N 的权限顺序纠正取代早期“一次跑完”提案。

### 2026-08-06 / Step 1D：确定的复用边界

直接复用的独立原语：

- `validate_release_archive.validate_archive()`：archive bytes/SHA、tar 路径/类型/
  权限、展开空间和控制文件验证。
- `frontend_release_artifact.verify_release()`：前端 manifest、payload SHA/bytes、
  build metadata 与安全物化。
- `verify_backend_readiness.verify_backend_readiness()`：`/readyz` 与 exact commit。
- `prepare_backend_release.update_env/prepare_metadata/confirm_metadata()`：运行 env
  和 release metadata。

只抽取小逻辑、不调用完整旧模块：

- 从旧 workflow 抽出显式 basis 的断点增量 rsync；不再扫描 checkpoint 选 basis。
- 从旧 controller 抽出只读 Alembic current/heads 比较。
- 从 quiescence gate 抽出 JATO job/upload/baseline busy 解析，以及 update/rollback
  短重启窗口所需的既有非阻塞 admission locks；不复用 marker/hold/wait 状态机。
- 从 storage guard 抽出安全路径、运行进程引用和 mark-and-sweep；只读取四指针和当前
  staging，不读取任何历史 checkpoint。
- 保留 release 到 shared data 的链接策略，避免每个 immutable release 复制或分叉
  `01_RAW_DATA`、`04_Processed_data` 等可变业务数据。

明确不 import/source 的 V1 组件包括完整 `tencent_bluegreen_release.sh`、
`release_checkpoint.py`、完整 `guard_release_storage()`、完整 `hold_gate()` 和所有
incident recovery/fence/hold 状态机。

### 2026-08-06 / Step 1E：首批测试结果

- `fixed_release_v2.py` 通过 Python compile 与 `git diff --check`。
- 新增 4 个 inventory 单测，覆盖：完整只读报告、secret allowlist、权限错误分类、
  system command partial 诊断。
- 测试结果：`4 passed`。

### 2026-08-06 / Step 1F：GitHub production environment 实际核验

通过 GitHub API 只读核验，不依赖仓库 YAML 推断：

- `production` environment 的 custom deployment branch policy 只有 `main`。
- 已配置 required reviewer `tristan419`。
- `can_admins_bypass=false`，管理员不能绕过该 environment gate。
- environment secrets 当前只有 Cloudflare account/token 与 `SSH_KNOWN_HOSTS`。
- `SSH_HOST`、`SSH_USER`、`SSH_PORT`、`SSH_PRIVATE_KEY`、`SSH_PASSWORD` 仍位于
  repository-level secrets。

结论：environment 的 main/reviewer 边界已成立，但生产 SSH 凭据仍可被一个不声明
`environment: production` 的仓库 workflow 请求。V2 发布前必须经用户授权，把 SSH
连接凭据迁入 production environment，并验证旧 V1/其他合法 workflow 的依赖；不能
在本 PR 中静默移动或删除。

### 2026-08-06 / Step 2A：Manifest 与单一 slots 存储原语

- 新增 `03_Scripts/deploy/release_v2_store.py`，不 import/source 任何 V1 checkpoint、
  recovery、systemd 或 Nginx 状态机。
- `ReleaseIdentity` 只由完整 commit SHA 与 archive SHA-256 组成。
- `ReleaseManifest` 严格绑定 archive bytes/hash、frontend content identity/checksum/
  build ID 和 build metadata hash；拒绝未知字段与非 canonical JSON。
- manifest 明确拒绝 GitHub run/attempt 等易变执行事实，workflow retry 不产生新
  release identity。
- pointer 只允许 `/opt/jato/slots/8000|8001/current|previous` 的相对 symlink，拒绝
  regular file、dangling link、越出 release root 和任意第三层目录。
- 原子 pointer 更新使用同目录临时 symlink、`os.replace` 和目录 fsync；清除 pointer
  只删除 symlink，绝不删除 release 目录。
- GC 当前只生成 mark-and-sweep 计划，不执行删除；保护四指针和当前 in-flight release，
  不读取历史 checkpoint。
- 新增 8 个单测；与 inventory 合计 `12 passed`。Python compile、120 字符检查和
  `git diff --check` 均通过。
- `fixed_release_v2.py` 另新增 2 个 manifest CLI 单测，覆盖幂等重试、sidecar SHA、
  archive tamper 的结构化拒绝；当前本地 V2 测试总计 `14 passed`。

### 2026-08-06 / Step 2B：复杂度收口与下一工作单元

- 已把 V2 runtime 限定为最多三个 Python 模块和一个薄 Bash 入口，目标不超过
  3,000 行；当时两个 runtime 模块合计 1,628 行。
- 下一工作单元只实现两个无状态只读原语：JATO busy admission 和数据库 revision
  current/heads 比较。两者都不写 marker、checkpoint、数据库或业务数据。
- 腾讯云实时事实仍由 `server_inventory_complete=false` 明确标记；在 inventory
  之前不会实现或试跑线上 bootstrap、systemd/Nginx mutation。

### 2026-08-06 / Step 2C：JATO admission 与数据库只读比较

- 新增第三个也是最后一个 runtime 模块 `release_v2_admission.py`。
- JATO snapshot 覆盖 job、pending operation、current process、upload digest、digest
  attempt、baseline promotion 和存活 PID；未知状态、坏 JSON、权限问题、symlink、
  读取中变化均 fail closed。
- stable reader 同时比较打开 FD 与读取后 pathname 的 device/inode/size/mtime；测试用
  byte-for-byte 相同内容的 `os.replace` 和 pathname unlink，证明不会把旧 inode 当成
  当前稳定状态。
- 锁顺序已作为纯 lexical planner 固定为：maintenance；然后 worker、active bundle、
  upload initiate、baseline promotion、全部 job state、逐 upload digest→state。
  planner 不 stat/open/mkdir/flock，也不包含 ingestion/recovery lock。
- 真正 lock lease 尚未实现。最终 update/rollback 必须在 maintenance 下 first scan，
  非阻塞取得全部 final locks，再 second scan；完整持有到新 Active 健康或旧 Active
  自动恢复完成。锁文件缺失时的 owner/mode 策略必须由腾讯云 inventory 决定。
- 数据库 env 不再被 shell source；只从 root-owned、精确 `0400/0600`、非 symlink 的
  `KEY=value` 文件解析 `APP_DATABASE_ENABLED` 与 `APP_DATABASE_URL`。不支持 `export`
  或变量展开，这是部署配置格式契约。
- 数据库子进程使用不继承 parent 的最小 env；Active code 执行 `alembic current`，
  Candidate code 执行离线 `alembic heads`，命令白名单测试明确禁止所有 migration、
  dump/restore verb。
- PostgreSQL URL 只在子进程私有 env 中规范为已安装的 `postgresql+psycopg` driver；
  raw output、连接串和其派生 hash 均不进入错误或 repr。disabled 返回
  `compatible/database-disabled`，revision 不同返回 `migration-required`。
- 两名独立只读审计分别复核锁/TOCTOU 与数据库隔离；数据库审计结论为无代码级阻断。
- 当前三个 runtime 模块共 2,443 行。鉴于剩余四操作仍需事务回滚和统一报告，复杂度
  预算现一次性冻结为 3,500 行预警、4,000 行硬上限；不得再次随需求抬高。
- 本地 V2 测试现为 `44 passed`；Python compile、120 字符检查和 `git diff --check`
  均通过。
- Chrome 可列出腾讯云标签页，但接管命令页时超时；未执行任何服务器命令，控制已
  释放，`server_inventory_complete` 继续为 false。

### 2026-08-06 / Step 2D：把服务器未知事实收敛到一次 inventory

- `fixed_release_v2.py inventory` 现从 systemd `MainPID` 对应的 `/proc` 读取真实
  运行目录、固定 allowlist 环境、进程启动时间和 UID/GID；读取前后比较进程 start
  time，避免把 PID 复用后的另一进程误当作 Active。
- 仅输出 `APP_PROJECT_ROOT`、月更 job root 和 active-bundle lock 等运行命名空间；
  数据库 URL、token、OAuth 和非 allowlist 环境变量不会进入 JSON。
- inventory 使用同一套 JATO stable reader 和纯 lock planner，报告所有既有 lock
  path 的 owner、group、mode 与缺失状态；它不会创建缺失 lock、取得 flock、mkdir
  或写 marker。
- inventory 使用 Active 运行 release 的 Python/Alembic 与 root-owned backend env，
  只读执行 `current`，并用同一 Active baseline code 执行 `heads`。这样服务器只需跑
  一次命令，就能同时确认数据库基线与 lock opener 所需事实。
- 测试中的 `/proc` 环境和 systemd `ExecStart` 故意混入 secret/数据库 URL，断言
  结构化结果不泄露；报告不再输出原始 ExecStart，只保留 workers 和 executable。
- 独立复审最初发现 4 个阻断项，已在同一轮全部修复：job/upload/maintenance 父目录
  symlink 逃逸；MainPID 未在整轮结束后复验；JATO namespace 失败未上升为 partial；
  缺失 lock 未报告父目录权限。没有拿未闭环版本去服务器试错。
- inventory 现要求固定 unit 与 `APP_RELEASE_SLOT` 一致、Active runtime 唯一，并在
  整轮结束后重新核对 systemd MainPID、`/proc` start time 和 UID/GID。缺失 lock
  同时报告直接父目录和最近存在父目录的 owner/group/mode。
- 独立二次复审确认 4 个 blocker 全部关闭；剩余“恶意进程在枚举后替换父目录”的
  理论竞态由未来 mutation lock lease 的 inode 复验处理，不阻止本次只读盘点。
- 三个 runtime 模块现为 2,994 行，仍低于冻结的 3,500 行预警线。全套本地 V2
  结果为 `50 passed`；Python compile、120 字符和 whitespace 检查通过。
- 这一步只增强了将来要上传到腾讯云执行的只读诊断命令；尚未执行服务器 inventory，
  未创建 lock、未停止服务、未改变 Nginx/数据库/JATO 数据或任何线上指针。

### 2026-08-06 / Step 2E：腾讯云终端控制连接诊断

- Chrome 能正常列出已登录的腾讯云轻量云实例标签页，但接管该标签页连续两次超时；
  两次均在页面控制建立前停止，没有向服务器发送命令。
- 本机只读诊断确认：Chrome 正在运行；ChatGPT 扩展已安装并启用；native host manifest
  存在且 extension origin 完整；浏览器安装与 profile 选择正常。
- 按 Chrome 控制恢复规范，下一步必须先由用户授权打开一个新的 Chrome profile
  window，然后只重试一次连接。不能改用 AppleScript、shell UI 自动化或其他旁路。
- 当前事实保持 `server_inventory_command_executed=false`、
  `server_inventory_complete=false`；这不是服务器 inventory 失败，更没有触发服务器、
  Nginx、systemd、数据库或 JATO 数据变化。

### 2026-08-06 / Step 2F：授权后的唯一 Chrome 恢复重试

- 用户已明确授权打开新的 Chrome profile window；恢复脚本成功打开 Default profile
  的新窗口，轻量连接可同时看到新窗口和已登录腾讯云标签页。
- 按恢复规范只重试一次接管：新标签页在接管/导航前超时，浏览器控制会话再次重置；
  没有到达腾讯云命令输入，也没有向服务器发送任何内容。
- Chrome、扩展安装状态和 native host 静态诊断虽然均正常，但规范要求此时停止重复
  重试并由用户从插件界面重新安装 Browser 插件。禁止改用 AppleScript、shell UI
  自动化或 SSH 旁路规避该边界。
- 当前等待项为 `user_reinstall_browser_plugin`。Goal 保持 active；服务器 inventory、
  lock lease 和四个 mutation operation 仍未开始。

### 2026-08-06 / Step 2G：Chrome 登录恢复与只读 SSH 路径确认

- 用户重新安装插件并明确授权启动 Chrome 后，官方恢复脚本已成功启动 Chrome；扩展能
  创建受控空白标签页并打开腾讯云登录页，用户扫码后登录态已确认。
- OrcaTerm URL 已在 Chrome 中成功打开。腾讯云终端采用特殊画布，整页快照和 DOM
  定位均超时；扩展还能列出现有 OrcaTerm 标签页，但 `claimTab` 错误地把当前浏览器
  instance 判为 unavailable。没有向网页终端输入任何命令，也没有服务器 mutation。
- 本机随后只读发现既有 SSH alias `tencent-cloud`，使用 `~/.ssh/tencent_lh.pem`，
  因而不再依赖不稳定的网页画布。下一步只通过 stdin 在远端 Python 内存中执行已经
  通过 50 项测试的 inventory，不上传文件、不落临时脚本。
- Chrome 启动/登录问题已解决；当前唯一工作单元恢复为服务器只读 inventory。任何
  bootstrap、清理、服务重启、Nginx、数据库或 JATO 数据操作仍未授权且不会执行。

### 2026-08-06 / Step 2H：旧 SSH alias 不可达，未触达服务器

- 已先在本机验证三模块 stdin-only loader 能正常启动 CLI，随后仅尝试一次
  `tencent-cloud` alias；连接 `150.158.141.14:22` 在 TCP 阶段超时，退出码 255。
- 因 SSH 尚未建立，远端 Python 和 inventory 均未启动；管道端的 BrokenPipe 只是
  连接失败后的本地生成器退出，不是服务器脚本错误。
- 没有服务器文件写入、命令执行、服务/Nginx/数据库/JATO 数据变化。该旧 alias 不再
  重试，也不能作为当前 Lighthouse 实例 `lhins-ce58rnqi` 的有效控制路径。
- 唯一剩余路径是用户已打开且登录的 OrcaTerm。下一步只修复现有标签页的 extension
  instance claim 绑定，不再创建重复标签页；claim 成功后才发送一次只读 inventory。

### 2026-08-06 / Step 2I：OrcaTerm 关键只读基线与复杂度纠偏

- 通过已登录 OrcaTerm 执行了一次只读命令。命令在 `ACTIVE_RUNTIME` 后因网页编辑器
  截断 heredoc 停在续行提示符，已立即用 Ctrl+C 取消并恢复 shell prompt；未运行后半段
  Python、数据库或 JATO hash，也没有任何服务器写入。
- 已确认固定 Active `jato-fullstack-backend@8000.service` 为 active/running，工作目录仍
  是 legacy `/opt/JATO_Analysis_System-main/06_AppPlatform/backend`，MemoryHigh=6G、
  MemoryMax=8G；8000 是唯一 8000/8001/18002 listener，内部与公网 healthz 均为 200。
- `jato-fullstack-backend@8001.service` 已加载但 inactive/disabled；固定 Preview unit 和
  `jato-bluegreen-production.service` 均不存在。Nginx `jato_fullstack_api` 仍只指向
  `127.0.0.1:8000`。
- 四指针现状为：`8000/current` 存在；`8000/previous`、`8001/current`、
  `8001/previous` 均不存在。`/var/lib/jato-release/active-slot` 仍是 root:root 0644 的
  legacy regular file。
- 392 权限事实得到纠正：checkpoint 为 root:root 0600，journal 已是 ubuntu:ubuntu
  0600，并非两份都需要改 owner。V2 不读取旧 checkpoint，因此未经用户明确授权不做
  owner repair，也不把它作为 V2 bootstrap 前置条件。
- 当前未提交 runtime 已有 2,994 行，却尚无四操作。永久 inventory 与重复 JATO state
  reader 会让 V2 在交付前再次变成平台，现停止扩大并改为直接实现四操作：复用现有
  systemd/Nginx/archive 校验和只读 `jato_quiescence_gate.inspect_state()`，不新增
  checkpoint、journal、hold、fence 或 recovery。
- 新规模目标为完整四操作后的 runtime 约 1,500–2,000 行；若四操作完成前再次接近
  3,000 行则停止，而不是继续提高复杂度预算。

### 2026-08-06 / Step 2J：稳定快照测试与四操作复审

- 同一 worktree 在本轮出现并发写入；主任务没有覆盖中间状态，而是等待 runtime 与
  测试文件稳定，并在测试前后比较 mtime/size，确认测试期间快照未变化。
- 当前三个 runtime 模块合计约 1,700 行，永久 inventory 已删除，四个用户操作方法均
  已出现；稳定快照测试结果为 `25 passed`。
- 绿色单测还不足以交付。两名独立只读复审汇总出 8 个必须同轮闭环的缺口：archive
  materialize；Preview 固定配置；discard GC；JATO restart lease；Active 月更 role；
  Candidate/Active 统一失败恢复；rollback 持久 report；rollback DB revision gate。
- 后续仍只允许在三个既有模块与对应测试内闭环，不增加 recovery/checkpoint/hold/fence，
  不把 operation report 当作下一次操作输入。当前没有提交、推送、PR、部署或线上 mutation。

### 2026-08-06 / Step 2K：中断后审计与单一 JATO 门禁收口

- 中断后核对 worktree、branch、HEAD 与文件范围均未漂移；仍只有 V2 runtime、测试和
  本文档范围内的改动，没有 workflow、服务器或业务数据变化。
- 当前三个 Python 模块加薄 Bash 入口共 2,375 行，低于 3,000 行停止线；远端入口
  `bash -n` 通过，V2/JATO gate 的中断后快照为 `68 passed`。
- 代理中断前落盘的 `jato_restart_guard` 与远端入口调用的既有
  `jato_quiescence_gate hold` 重复。按“只保留一个 owner”原则删除前者：外层 gate
  继续负责 marker、final locks 和 Active restart 窗口，V2 controller 只验证 gate
  已持有并复用现有 `inspect_state()`。
- 月更 gate 增加显式固定角色：Candidate 永远 fail closed；Active 必须声明 role=active、
  slot=8000，并继续尊重 quiescence marker；role 未设置时完整保留 legacy 逻辑。
- 当前仍未完成固定 Preview、统一失败恢复、workflow V2 接线与 CI；没有把 68 项测试
  误当作完整交付证据。

### 2026-08-06 / Step 2L：首次服务器失败面一次性静态审计

- 三个独立只读审计没有修改代码或线上，集中检查 controller、remote shell、systemd、
  Nginx、数据库 cwd、release store、现有 workflow 与测试盲区。共确认 14 个真实断点，
  没有把它们拆成上线后的事故 hotfix。
- 本轮已在同一 worktree 收口 10 项：Alembic 固定在 backend cwd；ScrapingToolkit 改为
  非 editable 安装；release 与 Preview 读取权限闭环；缺失的固定 Preview 合同幂等安装；
  所有 Preview/DB/旧 Candidate 恢复点写前预检；Candidate 和公网都核验 frontend
  `deployCommit` 与 `frontendBuildId`；build 增加 3G/4G、可用内存与 20 分钟上限；slot
  env 显式复用六个 durable path；GC 只识别带有效 V2 manifest 的 release；Active
  恢复无法证明时复用现有 quiescence gate 的 exit 81 保留 maintenance marker。
- Active update 与 rollback 现在复用同一个小型失败恢复函数。失败时精确还原指针对和
  原 `8000.env`，再验证旧后端、公网 readyz 和公网 frontend；恢复失败报告同时保留
  trigger 与各 restore error。Candidate 同样恢复原指针、env、Preview identity 和原
  服务启停状态，不再忽略 systemctl 返回码。
- 新增固定公网合同：Nginx 后端永远是 8000，frontend 永远读取
  `/opt/jato/slots/8000/current/...`。日常 update/rollback 只验证该合同，不重写 Nginx；
  公网前后端随同一 pointer 更新，避免再次出现“新后端、旧前端”式功能回退。
- operation report 现包含准确 `stage`、`passed`、`notReached`、expected/actual、
  `stateRestored` 和 mutation 分类；snapshot 单项失败也不会阻断 JSON 诊断生成。报告
  仍只是追加证据，不参与下一次操作准入。
- release store 对旧 V1 `commit/archive` 目录只报告、不删除；discard 在停止 Candidate
  或清指针前先完成 GC plan，未知目录不会造成半清理。
- 当前 controller/store/admission 为 `41 passed`，月更固定角色门禁为 `18 passed`，
  合计 `59 passed`；薄 Bash 入口 `bash -n`、120 字符与 whitespace 检查通过。三个
  Python 模块加薄 Bash 共 2,891 行，仍低于 3,000 行停止线。
- 剩余四项是首轮集成而非新事故系统：一次性把 legacy Active 登记并重启到 V2 固定
  8000；把 extraction staging 放到 `/opt/jato/staging`；在旧入口完成验签/构建后释放
  同一 flock 再交给 V2 controller；将现有 build + immutable archive + 增量 rsync 接到
  四操作 workflow/CI。四项完成前不去服务器试 Candidate。

### 2026-08-06 / Step 2M：V2 workflow 接线与旧门禁收口

- `production-release` 已改为只允许人工 `workflow_dispatch`，页面只暴露
  prepare、discard、update、rollback 四个操作；不再由 main push 自动启动生产动作。
- prepare 继续复用单次前端构建、完整确定性 archive、`gzip --rsyncable` 与 checksum
  rsync；Active V2 archive 可用时只传变化块，无法证明 basis 时必须显式批准首次全量
  上传。Candidate 不再触发 Cloudflare/intl 部署。
- archive SHA 确定后生成 canonical `release-v2-manifest.json`；远端解包改到
  `/opt/jato/staging`，验证与 `/opt/jato/releases` 同设备。旧入口在完成 archive 与
  frontend 验签后关闭原 flock fd，再由 V2 controller 重新取得同一 production lock。
- prepare/control 无论成功失败都保留结构化操作日志；旧 checkpoint、approval、cleanup
  和 parity job 暂时静态 `if:false`，生产路径不可达，待阶段 B 删除源码。
- 已发现并删除两个永远 false 的旧 Cloudflare prepare-and-switch step，其中一个残留
  空 step 会使 Actions workflow 在运行前即无效。intl prewarm 与现有 validator/CI
  正在同一轮收口，不把它们留作上线后 hotfix。
- 当前三个 Python runtime 加薄 Bash 共 2,910 行，仍低于 3,500 行预警线。workflow
  YAML 解析、两个 Bash `bash -n`、whitespace 检查均通过；旧 validator 仍因其写死
  push/incident hold/checkpoint 契约而失败，属于当前正在替换的预期门禁差异。
- 线上仍为旧 Active；没有上传、服务重启、Nginx/数据库/JATO 数据修改，也没有创建、
  合并或部署 PR。

### 2026-08-06 / Step 2N：权限顺序纠正与 legacy archive 证据

> 本 Step 的“首次 update 内迁移 legacy”结论已由 Step 2Y 取代；保留这里只为审计历史。

- 用户重新明确权限顺序：prepare 没有权力改变 Active；没有先明确 update Active，intl
  更不能变化。因此 legacy bootstrap 从首次 prepare 移到首次显式 `update-active`。
  prepare 在 legacy Active 下只读其健康和数据库 current，只允许启动 8001/18002。
- `update-active` 仍需页面单独勾选确认并绑定已验收 Candidate 的 commit/archive/manifest
  三个摘要；只有首次 update 额外绑定旧 Active archive。该操作只更新 www Active；
  intl 继续由既有独立同步任务从 current Active 同步。Candidate 操作不得触发两者。
- 只读检查公网 `healthz` 为 200，`build-meta.json` 仍绑定
  `cd4557cb932374a0fefb6c80a5fac9fb75a67d62` 与 frontend build ID
  `6c1eaac66588fd55356e651f60c6747d981ab31e4011dcea7adadf067e9bb61f`。
- 只读 Actions run `29998824639` 的不可变完成证据给出旧 Active 原始 archive：
  `archiveSha256=6af46992b1da87b6cb38d2cbc3a4bf9240f1dc82746f457c22bc69e74d78cc5e`、
  `archiveBytes=22269916`，frontend checksum 与线上均为
  `5afc9475cf256980943d22a80867c7270ee073cc28e18c6797c6687e1dec1ff2`。
  GitHub 只保留 9 KB 的完成 receipt；完整 22 MB archive 未作为 Actions artifact 保存，
  因此首次 update 前仍要只读确认服务器 content-addressed cache 中该精确文件存在。
- 同轮测试现为 V2/controller/store/admission/Active export `48 passed`；Nginx 固定合同与
  既有 installer 已逐字节一致，两个 shell 语法和 whitespace 检查通过。线上零 mutation。

### 2026-08-06 / Step 2O：Candidate 读取真实生产数据的硬隔离

- 根因确认：仅禁用 Candidate 月更 worker 不能阻止页面/API 使用 Active 数据库账号写入；
  已把安全边界落在现有 admission、Candidate 合同安装器和启动后 verifier，没有新建
  endpoint blacklist、状态机或 recovery 平台。
- `prepare-candidate` 现在会在首个 release materialize 之前读取 root-owned env，要求
  `/etc/jato-fullstack/candidate-database.env` 精确为 0600、数据库启用、PostgreSQL 目标
  与 Active 相同、账号与 Active 不同。错误报告不包含 URL、账号密码或 env 内容。
- 固定 8001 systemd drop-in 强制 `default_transaction_read_only=on`、statement/lock
  timeout、`ProtectSystem=strict`、`NoNewPrivileges`、共享 JATO 目录只读，只开放独立
  Candidate cache；已有合同漂移会拒绝，缺失合同只由现有 `_ensure_preview_contracts()`
  安装并 daemon-reload。
- Candidate 启动后继续核验 effective `EnvironmentFiles`、PGOPTIONS、ProtectSystem、
  ReadOnlyPaths/ReadWritePaths，以及 slot env 的 read-only/monthly/role。`update-active`
  在使用已测试 Candidate 前会再做一次数据库隔离核验。
- 当时本地 V2/store/admission/export/workflow 测试为 `65 passed`；月更固定角色为
  `18 passed`；两个 workflow validator 与两个 Bash 语法检查通过。未运行 GitHub CI，
  未创建 PR，未配置数据库角色，未安装服务器合同，未启动 Candidate，生产零 mutation。
- intl 语义按用户纠正：既有独立同步失败不回退已经成功的 www Active。

### 2026-08-06 / Step 2P：撤回不必要的 intl 编排

- 用户指出既有 `sync-www-active-to-intl` 已经从 current Active 同步，不需要 V2 自动
  编排。已撤回本轮误加的自动 queue job、sync workflow 新输入、专项 receipt、validator
  和测试；`sync-www-active-to-intl.yml` 与当前分支基线保持逐字节无差异。
- `update-active` 的职责重新收窄为只更新 www Active。intl 仍是之后由用户另行启动的
  既有操作；失败不连坐 www，也不在本 PR 新建处理系统。
- 本次纠偏未修改 Candidate 数据隔离、四操作 controller、Active export helper 或月更
  固定角色门禁；未运行生产操作。

### 2026-08-06 / Step 2Q：拒绝旧 drop-in 覆盖 Candidate 合同

- 最终只读复审发现：8001 原先只核验预期环境片段“存在”，旧蓝绿遗留的 persistent 或
  `system.control` drop-in 仍可能在之后覆盖数据库 URL、只读环境或资源限制。
- 修复复用现有 `_verify_candidate_runtime_isolation()` 与
  `20-candidate-readonly.conf`：`EnvironmentFiles` 和 `DropInPaths` 现在必须与固定合同
  完整相等；发现额外文件时 prepare 恢复原 Candidate 并在结构化报告中给出
  expected/actual，不自动清理服务器未知配置。
- Candidate 3G/4G 与 200% CPU 直接写入受版本控制的固定 drop-in，不再对 8001 调用
  `systemctl set-property`，从根因上避免新建第二套隐藏 `system.control` 配置。Active 的
  6G/8G 现有更新逻辑不变。
- 当时本地 V2/store/admission/export/workflow 测试为 `75 passed`，其中
  controller/store/admission 为 `61 passed`；月更固定角色为 `18 passed`。两个 workflow
  validator、两个 Bash 语法检查及 whitespace 检查通过。
- 尚未完成的证据明确保持为：GitHub CI、腾讯云 Candidate 专用只读数据库账号、线上
  effective systemd/进程核验和 18002 人工页面测试。未创建 PR，未修改服务器、数据库、
  JATO 数据、Active 或 intl。

### 2026-08-06 / Step 2R：恢复既有 intl 边界并补真实只读/月更证明

- 终审发现虽然 intl workflow 无 diff，但提前把共享 Active export helper 从
  `/opt/jato/active` 改到 legacy 阶段尚未就绪的 `8000/current`，仍会间接破坏既有同步。
  helper 与其测试已完整恢复到 `main`；validator 重新保留“workflow_dispatch-only、
  main-only、从 current Active 导出、不得依赖 Candidate”的既有合同。
- Candidate 数据库 admission 不再把“用户名不同”当成只读证明。它现在使用 Candidate
  runtime 的 psycopg 连接真实目标，以 transaction read-only 和有效 PostgreSQL 权限
  查询证明角色没有平台/对象写权限；输出只含布尔证明，不输出 URL、用户名或密码。
- Candidate backend ready 后实际请求月更路由；只有 HTTP 423 且
  `reason=explicitly_disabled` 才能完成 prepare。异常会恢复原 Candidate，Active 不变。
  cgroup 无月更子进程仍保留为腾讯云 canary 的只读运行证据。
- 当前本地 V2 聚焦测试 `76 passed`，controller/store/admission `63 passed`，月更角色
  `18 passed`；两个 validator 和两个 Bash 语法检查通过。唯一尚未实现的本地发布边界
  是首次 legacy→固定 Active 的一次性 update 路径；未创建 PR 或执行线上 mutation。

### 2026-08-07 / Step 2S：本地 CI 回归与首次 legacy Active 更新路径

> 本 Step 的“旧版本经 Candidate/update 登记”结论已由 Step 2Y 取代；旧代码不会作为
> Candidate 启动。

- 完整 Actions 等价测试首次运行暴露 1 个真实 Bash 默认变量问题和 6 个仍绑定 V1
  checkpoint/approval/cleanup 的旧测试。已在现有远端入口补默认 release system，并把
  对应测试迁到四操作 V2；当时完整结果为 `915 passed, 15 skipped`，不是只跑聚焦用例。
- 首次 legacy Active 不新增第五种 bootstrap/recovery 操作，而由现有 `update-active`
  负责：绑定已验收 Candidate，捕获旧 8000 pointer/env/unit/compat alias，安装固定
  8000 合同并更新 www Active；失败在同一进程恢复旧状态并验证旧运行身份。
- 当时曾提议第一次成功迁移时让 `active.previous` 保持为空；后续幂等终审证明这会让
  第一次真实升级成功后没有主动回滚点。该提议已由 Step 2X 的“同版本登记后再升级”
  取代，仍不把无法由 V2 manifest 证明的 legacy 目录伪装成 release。
- legacy→Candidate→下一固定版本→rollback 的聚焦测试已覆盖；没有修改 Nginx 公网
  指向、数据库内容、JATO 数据或 intl。

### 2026-08-07 / Step 2T：runtime seal 原子移动根因修复

- 发现 runtime seal 原先在 `/opt/jato/staging` 记录 `.venv/bin/python` 的绝对路径；目录
  原子移动到 content-addressed release 后，即使字节完全相同也会验证失败。
- 一度评估“移动后再生成 seal”，但只读复核证明 build/chmod/verify 失败会把半成品留在
  immutable store，导致同 identity 重试永久卡住，因此在提交前撤回该方向。
- 最终复用现有 seal helper，增加默认关闭的 `--recorded-runtime-root`：在 staging 读取、
  哈希真实 runtime，但仅把根内解释器的记录路径绑定为确定的最终 release 路径；staging
  以同参数验证，原子移动后不带参数再次验证。解释器若解析到 staging 外则拒绝。
- 未使用新参数的旧 V1/既有 seal 行为逐字节保持不变。真实 staging→`os.replace`→final
  验证、外部解释器拒绝及 controller 不重写 seal 均有测试；当前相关聚焦结果为
  `76 passed`，两个 workflow validator、shell syntax 和 style check 均通过。

### 2026-08-07 / Step 2U：V1 死代码与隐藏触发耦合审计

- `production-release.yml` 当前实际可达面只有四个 job：coordination guard、frontend
  build、prepare deploy 和三项 control；但文件仍保留约 1,871 行 `if:false` 或旧
  `prepare-and-switch` 不可达代码，包括 checkpoint、旧 approval/cleanup、inline intl
  发布和 parity audit。它们应一次删除，而不是继续以“禁用”名义长期保留。
- 删除前发现两个必须同时收口的真实语义耦合：coordination guard 仍把旧
  `audit_frontend_parity` receipt 当成最后生产 baseline；`intl-edge-prewarm` 仍监听每次
  `production-release` 成功，因此 Candidate 成功会间接启动一次 intl 只读预热审计。
- 第二项不代表 V2 应接管 intl。用户最终确认 intl 是 www 成功后的独立既有流程，
  因而本 PR 不修改 prewarm、同步 workflow、Active export helper 或 intl runtime；
  Candidate/update-active 本身不 dispatch intl。
- 上述两项仍在本地设计审计中，未提交、未触发 Actions、未创建 PR、未操作服务器。

### 2026-08-07 / Step 2W：www 成功与 intl 同步彻底解耦

- 用户再次明确：`update-active` 成功后 www 已经发布成功，不以 intl 结果作为发布事务
  的一部分，也不等待 intl。
- intl 需要更新时，用户另行运行仓库既有 `sync-www-active-to-intl`，从当时的 current
  www Active 同步；同步失败只报告并重试 intl，www 保持新 Active，绝不自动回退。
- 为避免扩大本 PR，已撤回 `intl-edge-prewarm.yml` 的本地改动；
  `sync-www-active-to-intl.yml` 原本就没有改动。当前分支不再包含任何 intl workflow diff。

### 2026-08-07 / Step 2V：首次 legacy Active 运行身份按真实路由修正

- 线上只读请求证明 legacy www 的 `/healthz` 返回 `200 status=ok`，公网 `/readyz`
  仍由 SPA fallback 返回 HTML；同时 8000 是纯后端，不拥有前端 `build-meta.json`。
  原先要求内外两个 origin 都提供 `/readyz` 和 `build-meta.json` 会在首次 Active 更新前
  错误拒绝，测试桩掩盖了真实 Nginx/后端边界。
- 修复只复用 controller 现有 `_verify_public()` 与 `_legacy_runtime_identity()`：内部 8000
  `/readyz` 证明后端 SHA，公网 `/healthz` 证明正式路由可达，公网 `build-meta.json` 证明
  frontend build 与同一 SHA；三项不一致均在 pointer/service mutation 前拒绝。
- 固定 V2 Active 的公网验证也改用实际存在的 `/healthz` 加 `build-meta.json`，内部 backend
  SHA 继续由紧邻的 8000 `/readyz` 验证。未新增迁移、恢复或 endpoint 平台。
- controller 聚焦测试当前 `46 passed`，新增覆盖明确禁止重新依赖公网 `/readyz` 或内部
  `build-meta.json`，并验证 public/internal identity 不一致时 Active 指针零 mutation。
  本轮只有公网只读请求；未连接服务器终端、未重启服务、未修改数据库或 JATO 数据。

### 2026-08-07 / Step 2X：四操作幂等、JATO 锁与存储根因统一收口

> 本 Step 的同版本 Candidate bootstrap 结论已由 Step 2Y 取代；其幂等、锁和存储结论
> 继续有效。

- 三路独立只读终审一次列出 controller、store 与 remote path 的共同失败面，没有部署后
  再逐点打 hotfix。P0 根因是 `update-active` 每次都覆盖 previous，且 rollback 使用交换
  语义；当前已改为同 target 只校验/收敛、`B/A -> A/A` 单向回退。指针已切而服务未重启
  的重试也会收敛目标，不需要 checkpoint 或 recovery receipt。
- Active 更新不再调用旧 30 分钟 wait/marker gate。V2 在现有 controller 内获取单一生产
  锁，并通过 admission 非阻塞获取 JATO 自己的 maintenance、worker、active-bundle、
  upload、baseline 和动态 state 锁；任一忙碌立即拒绝，不等待、不写 marker、不修改任务。
- `prepare-candidate` 现在在任何 8001 mutation 前证明 effective Nginx 固定到
  `8000/current`；新 promotion 后若数据库、合同或 runtime 验证失败，只删除本次新建且
  四指针均未引用的 manifest-proven V2 release。连续 prepare 和 discard 都执行四指针
  mark-and-sweep；exact SHA/SHA256 但缺 manifest 的 V1 目录只保留、不阻塞合法 V2 GC，
  损坏/伪造 manifest 仍 fail closed。
- 首次 legacy→V2 不允许把新业务版本直接登记成唯一 current。先准备与当前 legacy
  commit/frontend build 完全相同的不可变 artifact，执行一次同版本 `update-active` 并形成
  `active.current == active.previous`；之后再准备/批准新版本，得到正常 `new/old` 回滚点。
  这仍只使用 prepare/update 两个现有操作，不新增第五种 bootstrap 状态。
- Candidate/Active slot env 现在同时绑定 commit 与 archive SHA。prepare 报告绑定
  commit/archive/manifest 三元组；intl 仍由既有独立 Active→intl workflow 处理，本分支
  对两个 intl workflow 均保持零 diff。
- 该 Step 当时 controller/store/admission 聚焦回归为 `86 passed`，两个 Bash/Python 语法检查和
  whitespace 检查通过。三个 runtime 模块共 `3,861` 行，已超过 3,500 预警但仍低于
  4,000 硬上限；运行时范围自此冻结，不再加入 archive GC 平台、恢复状态机或第五操作。
- 该 Step 当时尚未完成：fixed-v2 远端 early split/current-main 控制 payload、事故 workflow 生产入口
  删除、完整本地 CI、Draft PR 与腾讯云一次性 bootstrap/Candidate 验收。未提交、推送、
  创建 PR、操作服务器、数据库、JATO 数据、Active 或 intl。

### 2026-08-07 / Step 2Y：终审纠正 legacy 登记与显式 rollback

- Step 2S/2X 中“把旧同版本构件作为 Candidate 再 update”的路线已被本 Step 取代。旧
  `cd4557cb…` 的 GitHub artifact 已过期，且旧后端不具备 V2 Candidate 只读/月更禁用
  合同，因此不能安全启动成 Candidate。
- 首次迁移改为一次性直接登记 legacy Active 基线：先只读证明服务器精确旧 archive，
  再用最终 main 控制文件生成同业务版本 wrapper 并登记 A/A；缺少唯一证据就停止。它不
  进入日常四操作或永久 recovery 系统。该服务器证据和一次性登记尚未完成，不能标记为
  已验收。
- `rollback-active` 改为用户明确提供受保护的 commit/archive/manifest 三元组，并按
  `B/A -> A/A` 只更新 current；控制器捕获的失败恢复 `B/A`，不再隐式交换。
- 运行身份仍只有固定 8000/8001；短命 `systemd-run` 仅是 3G/4G dependency-build scope。
  archive cache 只按四指针保护，取得既有非阻塞 lock 后 best-effort 清理，永不删除 lock。
- www/intl 边界不变：www update 成功即终态；既有 intl 同步由用户另行启动，失败不回滚
  www。两份 intl workflow 保持零 diff。

### 2026-08-07 / Step 2Z：本地收口、完整回归与 A0 停止条件

- `prepare-candidate` 在复用旧 Candidate 前先证明 pointer、slot env、8001 runtime 与
  18002 preview 身份一致；不一致时在任何 pointer 改写或服务重启前拒绝，并明确要求
  先执行现有 `discard-candidate`。未增加 checkpoint 或恢复状态机。
- `rollback-active` 的中间态测试证明正常 `B/A -> A/A` 只写 current，从不出现 `B/B`；
  捕获到验证失败时恢复 `B/A`。运行时三个 Python 模块正好 `4,000` 行，达到既定硬上限，
  范围冻结。
- archive cache root 已从 SSH 用户的绝对 `$HOME/.cache/jato-releases/archives` 传入四个
  操作；remote 和 validator 对缺失、相对路径及 parent traversal fail closed。两个 intl
  workflow 仍为零 diff。
- Candidate 的强保证范围写实为 PostgreSQL SELECT-only、共享数据目录只读、JATO 月更与
  单实例后台任务禁用；它不是对 Airflow 等所有外部系统副作用的通用沙箱，人工测试不得
  点击这类写操作。
- 完整本地结果：两个 workflow validator 通过；workflow/unit `115 passed`；部署套件
  `959 passed, 17 skipped`；后端 `52 passed`；前端 `69` 个文件、`370` 项测试、production
  build 和 7 项 router regression 全部通过；Bash 语法和 whitespace 检查通过。
- A0 不能在未知服务器事实下盲写。旧 `cd4557…` 源码没有 `/readyz`，且旧依赖必须与现网
  venv 比较；因此先只读确认唯一 archive、frontend/runtime、systemd/Nginx、数据库、锁、
  指针和资源，再在同一 Draft PR 加入可删除的一次性登记 helper。日常 controller 不恢复
  legacy 特判，也不增加第五操作。直连 SSH 本次超时，未执行任何服务器命令或 mutation。
- 终审另确认 rollback 的可捕获失败恢复已覆盖，但 `B/A -> A/A` 在 current 改写后、
  8000 重启前若被 SIGKILL，B 可能暂时失去四指针引用。该项作为 Draft blocker 保留；
  只能在既有四指针模型内解决或由用户明确接受，不能为此重建 checkpoint/recovery 平台。
- www/intl 权限再次锁定：`update-active` 成功即 www 成功；intl 只由用户之后另行运行既有
  Active→intl 同步，失败只影响 intl，绝不回滚 www。

### 2026-08-07 / Step 3A：独立 Draft PR 已建立

- 34 个已审查文件以 commit `9c172109bf357016b4a466cac30b91b4b7c42f9f` 推送到
  `codex/simple-candidate-release-v2`，并建立 Draft PR
  [#214](https://github.com/tristan419/JATO_Analysis_System/pull/214)。
- PR 明确记录：www Candidate/Active 是 V2 唯一发布范围；两份 intl workflow 零 diff，
  intl 仍由用户之后另行启动既有 Active→intl 同步，失败不回滚 www。
- PR 保持 Draft。A0 服务器只读 inventory 和 rollback 极端 SIGKILL 引用窗口仍是 Ready
  blocker；未执行合并、部署、服务器 mutation、Active 切换或 intl 同步。

### 2026-08-07 / Step 3B：rollback 强杀窗口改为四指针内的原子交换

- 终审确认旧 `B/A -> A/A` 顺序会在 current 已写而 8000 尚未重启时短暂丢失 B 的四指针
  引用。Step 2Y/2Z 的 A/A 日常回退语义由本 Step 取代；A0 首次基线仍可使用 A/A。
- 修复只扩展现有 release-store 指针原语和 `_switch_active()`：rollback 使用 Linux
  `renameat2(RENAME_EXCHANGE)` 一次把 `B/A` 变为 `A/B`；macOS 本地验证使用等价
  `renamex_np(RENAME_SWAP)`。内核不支持时在任何指针改变前拒绝，不做顺序降级。
- 回退失败时同样原子交换回 `B/A`，再恢复旧环境并验证 B。SIGKILL 前后持久指针对只可能
  是 `B/A` 或 `A/B`，两个版本始终受 Active current/previous 保护。
- 同一 A 的重试只验证或收敛 A，不自动 toggle。只有用户再次明确提交 B 的完整
  commit/archive/manifest 三元组，才允许 `A/B -> B/A`。
- 新增 store 与 controller 状态测试后，聚焦回归为 `100 passed`；完整脚本套件为
  `1221 passed, 15 skipped`，workflow/backend 相关回归为 `40 passed, 2 skipped` 与
  `18 passed`。三个 V2 runtime 模块由 4,000 行增至 4,125 行，全部增量用于可移植原子
  交换、失败恢复与验证；硬上限一次性调整为 4,200 并重新冻结，不借此加入其他能力。
  尚需 Linux PR CI 和腾讯云目标文件系统能力验证；没有新增 workflow、checkpoint、
  recovery 或 intl 改动。

### 2026-08-07 / Step 3C：rollback Linux CI 通过，阻塞收敛到 A0

- commit `d74fd7b8ec39f003b89dbf9a36c6283f3cf80b71` 已推送到 Draft PR #214；两轮 CI 中
  frontend/backend/smoke、发布合同、生产门禁、release coordination 和 Cloudflare PR
  preview 全部通过。Cloudflare 结果只是 PR preview，不是 intl 生产同步。
- Linux runner 上的 store 测试实际执行 `renameat2(RENAME_EXCHANGE)` 并通过，因此原先
  rollback SIGKILL 引用窗口不再是本地/CI blocker。腾讯云目标 slots 文件系统仍须验收，
  不允许用 GitHub runner 结果替代服务器事实。
- 只读 SSH `150.158.141.14:22` 再次在 10 秒连接超时；Chrome 控制扩展当前没有可用实例，
  所以没有运行任何腾讯云命令。未创建 A0 helper，也未操作服务器、数据库、JATO 数据、
  Active 或 intl。
- 当前唯一 Ready blocker 是 A0：先取得服务器只读 inventory，再决定一次性登记实现和
  Candidate 无流量验收。PR 继续保持 Draft。

### 2026-08-07 / Step 3D：腾讯云 A0 只读 inventory 完成

- 通过已登录 OrcaTerm 只执行只读命令；未写服务器文件、未重启服务、未修改 Nginx、
  指针、数据库或 JATO 数据，也未触发 Candidate、Active 或 intl 操作。
- 主机为 Linux `6.8.0-106-generic`，`/opt` 位于 ext4；内存约 15 GiB、当时可用约
  9.1 GiB，磁盘约 178 GiB、可用约 123 GiB。目标文件系统具备 Linux 原子交换的必要
  平台条件，但尚未创建临时 symlink 做 `renameat2(RENAME_EXCHANGE)` 实机能力探针。
- `/opt/jato/slots/8000/current` 是 root 所有的 symlink，精确指向
  `/opt/JATO_Analysis_System-main`；`8000/previous`、`8001/current`、`8001/previous`
  与 `/opt/jato/active` 均不存在。现有 6 组旧 release 目录未被 V2 指针引用。
- Active 仅监听 `127.0.0.1:8000`，`/healthz` 为 200；`/readyz` 为旧版本预期的 404。
  `jato-fullstack-backend@8000.service` 为 active/running、2 workers、
  `MemoryHigh=6G`、`MemoryMax=8G`。8001 与 18002 均未监听，月更 worker 为
  inactive/disabled。两个历史 transient canary unit 为 failed 残留，但没有运行进程。
- Nginx 固定 upstream 为 `127.0.0.1:8000`，本机携带 www Host 的 TLS `/healthz`
  返回 200，满足 V2 固定 Active 端口的前置条件。V2 未对 Nginx 做任何改写。
- 前端 `build-meta.json` 是线上发布身份的权威证据：commit/app/deploy/GitHub SHA 均为
  `cd4557cb932374a0fefb6c80a5fac9fb75a67d62`，原 archive 为
  `6af46992b1da87b6cb38d2cbc3a4bf9240f1dc82746f457c22bc69e74d78cc5e`、
  22,269,916 bytes，并保留 frontend artifact/checksum/build ID。服务器工作树 Git HEAD
  为旧 `c84d7af…`，且没有 `cd4557…` commit object，因此 Git 不能替代发布 metadata。
- build metadata 记录的精确 cache 路径已经 MISSING；在 `/opt` 与 `/tmp` 也没有同尺寸
  archive。当前 live tree 曾被部署脚本合并 runtime、保留 mutable paths 并重写发布状态，
  不能重新打包后声称是缺失的原 archive。
- 数据库只读核验为 `current=head=20260715_0046`。Active legacy 进程没有 V2 role env，
  与尚未 bootstrap 的状态一致；现网 venv 为 Python 3.12.3，`pip freeze` 排序摘要为
  `a93d5dd1e0161e9c4978b348e04cbb9dc4ad2d3ba71f5ee71163ba4c9b2e39a`。
- `/etc/jato-fullstack/candidate-database.env`、`slots/8001.env` 与 Candidate readonly drop-in
  均不存在；`slots/8000.env` 已存在且为 root:root 0600。Candidate 数据库角色与 drop-in
  必须由后续已审查的 V2 prepare 前置步骤创建/验证，不能在 A0 inventory 中暗改。
- Draft PR #214 head `e531ae0c…` 的 13 个检查全部成功，base main 仍为 `37a9905c…`，
  PR 为 Draft、mergeable/clean，且没有 review thread。CI 通过不替代 A0 决策与服务器验收。
- 当前 A0 契约第 3 步的 fail-closed 条件已真实命中。下一步不能继续假定旧 archive 存在；
  必须在“精确恢复原 archive”与“显式授权用新 SHA 采纳当前 stable live Active”之间做一次
  契约选择。后一方案只能是可删除的一次性 helper，旧 SHA 仅作为 provenance，不得冒充
  新 wrapper 的 archive identity；四个日常操作和 intl 流程均保持不变。

### 2026-08-07 / Step 3E：旧 cd455/6af archive 逐字节恢复，A0 不再放宽 identity

- GitHub run `29998824639`、attempt 1 与 head `cd4557cb…` 的日志和 verified receipt 仍可读；
  原 frontend artifact 与 Candidate receipt 已到期并返回 410，完整 backend archive 当时从未
  上传为 Actions artifact，只曾传到现已缺失的服务器 cache。标准 rerun 会改变 attempt 与
  build timestamp，不能作为原构件。
- 在纯 `/private/tmp` 隔离目录使用 clean `git archive cd4557cb…`、Node `20.19.0`、npm
  `10.8.2`、原 `buildTimestamp=2026-07-23T10:20:12.168Z`、43 条 MSRP evidence、GNU tar
  `1.35`、GNU gzip `1.12` 与 `SOURCE_DATE_EPOCH=1784802012` 重建。frontend build ID
  `6c1eaac6…` 与 payload SHA `5afc9475…` 先独立命中；完整 archive 随后同时命中：
  `22,269,916` bytes、SHA-256
  `6af46992b1da87b6cb38d2cbc3a4bf9240f1dc82746f457c22bc69e74d78cc5e`，共 2,637 个成员。
- 包内 `hermes/deploy_release.json` SHA 为
  `7596dbc6b9f98d80a0885b78567371caec6b4d8da95c13b16abb9c69a4d42a6c`；由原 commit、
  archive、frontend identity/checksum/build ID 无猜测生成的 canonical V2 manifest 为
  652 bytes，SHA
  `9f62cda70530ec38560c9fa25846eee8ea767f7bcbd67f6bac49f3eb198fd947`。
- 因此 A0 不再允许“新 SHA 采纳 live tree”，也不再需要 300+ 行 live snapshot helper。
  但旧 archive 仍不能伪装为普通 Candidate：旧私有文件 mode 与当前 normal validator 合同
  不同，且缺少 V2 controls、seals、物化 frontend/dist、venv 与 `/readyz`。最短路径仍是
  可删除的一次性 helper，验证 exact identity 后在 staging 注入最终 V2 控制闭包，直接登记
  `active.current == active.previous == A`，不启动 8001/18002，不新增第五个日常 action 或
  workflow；成功后删除 helper。
- 只读终审同时发现 manifest 的通用完整性缺口：`buildMetadataSha256` 已生成和解析，但
  `_verify_manifest()` 未重新哈希实际 `hermes/deploy_release.json`，而 source seal 有意排除
  该文件。修复直接加入现有函数，缺失与篡改均在任何 Active mutation 前拒绝；新增测试后
  controller/store/admission 聚焦套件为 `102 passed`。这属于现有 manifest 根因修复，不是
  A0 专项平台。
- 本 Step 仅修改本地 Draft PR 文件和 `/private/tmp` 验证产物；没有上传原 archive、没有
  操作腾讯云、没有改指针、服务、Nginx、数据库、JATO 数据、Active、Candidate 或 intl。

### 2026-08-07 / Step 3F：A0 helper 收敛完成，未进入服务器执行

- 已重新接管用户打开的 OrcaTerm，并让终端实际返回 `CODEX_READONLY_OK`。随后只读确认线上
  legacy `.venv/bin/python -> python3 -> /usr/bin/python3.12`；直接复制旧 venv 会违反 V2
  relocatable runtime seal，因此该方案被服务器事实否决。终端检查没有写文件、重启服务或
  修改 Active/Candidate/intl/JATO；后续长输出截图通道超时，不把未取回的输出作为证据。
- source seal 的 `SOURCE_CRITICAL_FILES` 仍是 V1 checkpoint/boot-reconcile/旧蓝绿控制面，
  是 A0 被迫携带十个废弃文件的根因。现已直接把现有 policy 和测试收敛为 13 个 V2 文件：
  production trusted-control 11 项，加 source-seal helper 与 frontend materializer；没有复制
  V1 控制面。独立审查确认 fixed-v2 路径没有遗漏依赖。
- 合规的 `build_candidate_runtime()` 原本封在 `fixed_release_v2_remote.sh` 内。现只增加
  sourced 时跳过 dispatch 的内部复用边界；直接执行仍只有 prepare/discard/update/rollback
  四操作。A0 通过独立 `bash -c` 调用同一 3G/4G、`venv --copies`、pip/toolkit builder，
  没有复制一套 runtime 构建逻辑，也没有第五个 action/workflow。
- 一次性 `one_time_register_legacy_active_v2.py` 最终为 350 行，并把 exact A0 commit、archive
  SHA/bytes 与 manifest SHA 写死为 fail-closed 常量。旧 archive 的 0644 历史私有文件会被
  normal validator 正确拒绝，因此 A0 不伪装成普通 release：只在 exact SHA 命中后使用
  Python `tarfile` data filter 解包。2,637 个真实成员已离线通过 filter。
- helper 复用现有 frontend evidence 校验、runtime builder、source/runtime seal、store promote、
  database/JATO admission 和 Active restart verification；只登记 `A/A`，不启动 8001/18002。
  restart 失败会恢复 legacy current raw target、原 8000 env 与 compat link并重新验证 health。
  sudo 执行场景会在 runtime build 前把 staging root 交给 `SUDO_UID/SUDO_GID`，构建后再收回。
- 新 helper 测试覆盖：非 exact A0 零写入拒绝、坏 archive 零写入拒绝、成功 A/A、Active restart
  失败恢复、sourceable builder 无 dispatch、sudo builder staging 权限交接。source seal/workflow/
  controller/store/admission/helper 组合回归为 `153 passed`，style check 通过；完整脚本、backend、
  frontend 回归与下一次 Draft PR CI 尚未运行。
- 本 Step 仍没有上传 exact archive、没有在腾讯云运行 A0、没有创建 Candidate、没有改 Active、
  没有同步 intl，也没有修改数据库或 JATO 数据。PR #214 继续保持 Draft。

### 2026-08-07 / Step 3G：独立审查否决 A0 helper，回到产品取舍

- Step 3F 只证明 mocked/local happy path，并未证明 helper 可在真实服务器安全执行。独立逐项
  审查确认至少存在七个发布阻断：未绑定当前公网 legacy identity、staging 根目录最终仍可能
  为 `0700`、未安装真实 8000 V2 unit、重新安装 `>=` 依赖不等于现网 runtime、未证明
  `/opt/jato/shared` 与 legacy durable path 一致、失败恢复未覆盖 unit/limits/public identity，
  以及强杀后 `L/A`、`A/A` 中间态不能由原 helper 重试收敛。
- 继续给该 helper 增加 preimage、checkpoint 和恢复分支会重新建立刚刚删除的事故恢复系统。
  因此 helper 与其测试在尚未 stage、commit 或 push 前已删除；为 source helper 增加的 shell
  sourcing 边界和测试也一并删除。没有上传或运行这些代码，服务器仍只读。
- 两项通用根因修复不依赖 A0，继续保留：manifest 现在重新哈希实际
  `hermes/deploy_release.json`；source critical policy 从废弃 V1 文件改为真实 V2 控制闭包。
- 删除 helper 后重新执行当前差异的完整回归：V2/controller/store/admission/workflow/source-seal
  聚焦测试 `146 passed`；`03_Scripts/tests` 为 `1223 passed, 15 skipped`；部署相关后端测试
  为 `103 passed, 1 skipped`；style、Bash syntax 与 Python compile 均通过。
- 通用修复以 `6bb41ab0` 推送至 Draft PR #214；两个 CI 触发面的 backend/frontend/smoke/
  frontend-release-contract、production guard、release coordination 与 Cloudflare Pages 共 13 项
  checks 全部通过。该 push 没有触发 production release 或腾讯云操作。
- 首次迁移不能靠技术细节掩盖产品取舍。若必须从第一次正式更新起保留 legacy rollback，
  就只能使用执行后删除的一次性 A/A 迁移并完整承担其迁移验证；若优先最简单的长期代码，
  则可让已测 Candidate 首次形成 `B/B`，但必须明确披露：直到下一次 `C/B` 前没有 distinct
  rollback，且不可自动回到 legacy。用户尚未选择，controller 不会擅自实现后者。
- 服务器截图已确认 `8000.env` 存在，但 `candidate-database.env`、`8001.env` 和 Candidate
  readonly drop-in 缺失。后两项由正常 prepare 收敛；SELECT-only Candidate 数据库 role/env
  是 prepare 前仍需单独授权配置的真实前置条件。

### 2026-08-07 / Step 3H：用户确认 B/B，首次迁移收敛进现有 update-active

- 用户明确选择最简 B/B：首次已测 Candidate B 获得单独批准后，Active current/previous
  同时登记 B；成功后不自动回到 legacy，直到下一次 C/B 才出现 distinct rollback。
- 最初实现一度让 runtime 达到 4,335 行，并重复实现 systemd/public 恢复。按复杂度停止条件
  立即停止扩大；最终只保留现有 `update_active()` 的 legacy 分支，并将 `_switch_active()`
  原有三套重复恢复路径合并为一套。三个 runtime 模块现为 4,198 行，未抬高冻结上限。
- 服务器真实 `FragmentPath` 是共享
  `/etc/systemd/system/jato-fullstack-backend@.service`，不是显式实例 unit。实现现在允许可信
  shared/explicit 两种 preimage，但只写新的显式 `@8000.service`；失败时若原来使用 shared，
  删除新显式 override 并 daemon-reload 回原 FragmentPath。shared + stale explicit 组合在任何
  指针、unit、env、compat 或 restart mutation 前拒绝。
- 测试桩不再伪装真实条件：legacy fixture 使用 shared template、显式 8000 unit 缺失、旧
  WorkingDirectory/ExecStart/EnvironmentFiles；daemon-reload 会按文件存在性切换有效属性。
  现覆盖 B/B 成功与幂等重试、公网失败恢复、Active restart 失败恢复后重试成功、compat
  创建失败幂等恢复、stale explicit/unknown FragmentPath/previous 非空零写入拒绝，以及 B/B
  rollback 明确不可用。独立审查另发现两项 already-target 边界：退化时曾把目标 env 留给
  fallback；JATO 二次检查在服务变更前失败时曾多余交换指针。现分别由
  `_restart_active(..., write_env=True)` 生成 fallback env，以及在零 service mutation 时原样
  拒绝，并加入回归；mutation 现准确标记实际发生过的 pointer/service/traffic 变化，恢复后
  同时标记 `stateRestored=true`；legacy 自身恢复失败也会同时报告 trigger/restore。当前单文件聚焦结果
  为 `70 passed`；完整 `03_Scripts/tests` 为 `1233 passed, 15 skipped`；额外 GitHub/backend
  CI 边界为 `40 passed, 2 skipped`；workflow
  validator、Bash syntax、style 与 Python compile 均通过。
- 捕获到的普通失败会恢复 legacy current 原始 symlink、previous、8000 env、显式 unit 的
  原存在状态、compat link、6G/8G 运行与公网 build identity。按用户选择，断电、内核崩溃或
  SIGKILL 的跨文件中间态仍不建设 checkpoint/recovery 自动恢复；遇到时先人工只读盘点。
- 本 Step 仅有本地 Draft diff；未推送新 head、未操作服务器、未创建 Candidate、未更新
  Active、未同步 intl，也未触碰数据库或 JATO 数据。完整本地回归已完成，独立终审确认无 P0/P1；
  shared-template 接管、B/B 语义、失败恢复和 mutation 报告均通过复核。

### 2026-08-07 / Step 3I：B/B 实现推送 Draft 并通过 CI

- 四个已审查文件以 `e290d715` 提交并推送到 Draft PR #214；提交钩子自动生成的
  Hermes event 不属于本 PR 范围，已在推送前丢弃，未混入第五个文件。
- 该 code head 的 GitHub CI 终态为 13/13 通过：两个触发面的 backend、frontend、smoke、
  frontend release contract 和 production guard 全绿，release coordination 与 Cloudflare Pages 亦通过。
- PR 仍为 Draft；没有合并、production workflow、腾讯云写入、Candidate 启动、Active 更新、
  intl 同步、数据库或 JATO 数据变更。

### 2026-08-08 / Step 3J：固定 Active 契约迁移完成，首次 Candidate 安全失败后按根因收口

- PR #214 已合并为 `main@30f3e2e4`。经用户单独授权，在腾讯云生产锁内将 enabled Nginx
  配置迁移为 canonical fixed-Active 契约；`nginx -t`、本机 8000/18000 与公网 www/apex
  health 均为 200。公网仍只指向 `127.0.0.1:8000`，Active 仍为 PID 3481565、SHA
  `cd4557cb…`、2 workers、6G/8G；Candidate/intl/JATO 数据均未改变。迁移 preimage 位于
  `/var/lib/jato-release/nginx-preimages/fixed-active-v2-20260808T103130Z-1121064`。
- 随后经用户另行授权，只部署 Candidate。GitHub run
  [31253025482](https://github.com/tristan419/JATO_Analysis_System/actions/runs/31253025482)
  已完成构件上传、manifest 与 main SHA 复核；服务器在 Candidate runtime isolation 校验处
  拒绝。8001 曾按 3G/4G 启动，但在约 121ms 后恢复为空；8001/18002 均无监听，Candidate
  current/previous、slot env 与 preview identity 均已清空。Active 的 PID、SHA、资源和公网
  健康保持不变，未更新 Active 或 intl。该次失败留下一个约 1.2GB 的未引用有效 release；
  服务器仍有约 122GB 可用空间，当前不做未经授权的手工删除，待 Active 进入 V2 后由既有 GC
  回收。
- 完整只读审计确认不是数据库或页面代码问题，而是三个现有控制器根因：Ubuntu systemd 255
  对 `EnvironmentFiles` 使用换行列表而测试桩误用单行；`Type=simple` restart 后立即探测，未给
  实际约 6.2–6.5 秒的应用启动时间；legacy Active 指向受支持的外部旧目录时，失败清理仍调用
  只接受 V2 store 指针的 `remove_if_unreferenced()`，从而用 cleanup 错误遮蔽原始错误。
- 修复严格留在 `fixed_release_v2.py`：仅对 `EnvironmentFiles` 做保序空白规范化，Candidate 与
  Active 共用；HTTP 启动验证只用于刚重启的 backend/preview，最多 10 次、每次 2 秒、间隔
  1 秒，单服务最坏约 29 秒，每轮还会确认对应 systemd unit 仍 active。只重试连接失败、明确
  502/503/504，及携带这些状态的非 JSON 错误页；200 坏 JSON、SHA、配置、权限等确定性错误
  立即失败，最终始终报告最后一次结果。公网 health 与静态 frontend 不另开等待预算。legacy
  Active 下只延后本次新 release 清理并重新抛原始错误。没有修改 store/workflow，没有新增
  action、事故常量、checkpoint、recovery 或服务器身份。
- 测试桩已改成真实 systemd 多行输出，并覆盖多行接受、额外/重排拒绝、瞬态 backend/preview
  成功、混合 503/断连耗尽后报告最后错误、unit 停止和 200 坏 JSON 立即失败、Candidate 完整
  恢复、错误 SHA 单次立即拒绝，以及 legacy Active + fresh staging 失败仍保留根错误。当前
  聚焦套件 `76 passed`；最终 production CI 同款发布套件为 `811 passed, 15 skipped`，Python
  compile 与 diff check 通过。三个文件已由 commit `36c2d9f9` 推送至独立
  [Draft PR #216](https://github.com/tristan419/JATO_Analysis_System/pull/216)，base 仍为
  `main@30f3e2e4`。该 code head 的 13/13 GitHub checks 已全部通过：两套 backend、frontend、
  smoke、frontend release contract 与 production guard，以及 release coordination 和
  Cloudflare Pages 均为 success。本次只再推送 CI 证据文档；PR 继续保持 Draft。
- 三个 runtime 模块由 4,198 行增至 4,256 行，净增 58 行均属于上述通用根因修复。没有通过
  新模块搬移或格式压缩规避行数。原 4,200 字面硬上限已暴露会诱导这种规避，现改为强制独立
  审查线：超过后只允许有服务器证据、净增不超过 60 行的现有职责根因修复，并继续禁止新增
  action/workflow/module/recovery；后续永久能力必须先删除等量重复/废弃代码。当前未重新部署
  Candidate，后续仍需用户合并授权和用户 Candidate 部署授权两道独立步骤。

### 2026-08-08 / Step 3K：固定 Candidate 国内测试地址

- 已只读确认最新 `main@b2169516` 运行于固定 Candidate 8001，预览服务只监听
  `127.0.0.1:18002` 且健康；`candidate.ojeur.cloud` 尚无 DNS，现有 www 证书也不包含该域名。
- 固定地址采用最小独立网关：DNSPod A 记录直达上海腾讯云，独立 TLS vhost 先执行 Basic
  Auth，再且只反代 18002。网关不加入 Active 的 `DEPLOY_SERVER_NAME`，不包含 8000、Active
  include、www/intl 域名或 fallback；Candidate 不存在时返回 5xx，不能显示 Active。
- 同一个 `candidate.ojeur.cloud` 不绑定具体 commit。每次新的 main 经人工 dispatch 成功
  `prepare-candidate` 后，只更新固定 8001 的 Candidate 指针和 18002 identity；用户刷新固定
  地址即可测试最新构件。未获独立授权时绝不运行 `update-active` 或 intl 同步。
- 当前独立分支只新增 Candidate 公网 Nginx 模板，并修改现有 Nginx 合同测试和本手册/操作
  手册；没有修改 controller、workflow、前端、后端、数据库或 JATO 数据，也没有写服务器、
  DNS、证书或认证文件。完成本地验证后只创建 Draft PR，后续安装仍需单独授权。
- 服务器现有证书只覆盖 ojeur.cloud/www。安装时必须先建立 DNS 和临时 HTTP-only
  Candidate vhost，使用现有 Certbot Nginx authenticator 签发独立证书；随后再创建 Basic
  Auth、渲染最终模板并通过 `nginx -t` 后原子启用。不得让缺失证书的模板进入 enabled。
- 4 个已审查文件由 commit `991a44f8` 推送至独立
  [Draft PR #217](https://github.com/tristan419/JATO_Analysis_System/pull/217)。PR 不包含服务器、
  DNS 或生产写入；13/13 GitHub checks 已通过。未经用户另行授权不转 Ready、不合并、
  不安装网关。

## 11. 决策日志

| 日期 | 决策 | 原因 |
|---|---|---|
| 2026-08-06 | 固定 Active 8000、Candidate 8001，不交换角色 | 符合用户心智；人工测试与公网职责清晰 |
| 2026-08-06 | 测试通过后 Active 引用同一不可变 release | 避免二次上传/组装产生未经测试差异 |
| 2026-08-06 | 不延续事故 recovery 平台 | 旧状态全局耦合，是反复 hotfix 的主因 |
| 2026-08-06 | V2 验收后另 PR 删除 V1 | 降低首次迁移风险，并保留可审计退路 |
| 2026-08-06 | 文档随每一步同步更新 | 让事实、决定、测试与未完成项始终可追踪 |
| 2026-08-06 | 数据库 env 只解析、不 source | 避免配置文件执行 shell 及父环境 secret 污染 |
| 2026-08-06 | lock opener 等服务器 inventory | 缺失锁文件的 UID/GID 不能凭代码仓库猜测 |
| 2026-08-06 | 3,500 行预警、4,000 行硬上限 | 给必要事务回滚留空间，同时阻止再建发布平台 |
| 2026-08-06 | 日常操作只验固定公网 Nginx，不重写路由 | Active 指针同时驱动前后端，避免功能组合回退 |
| 2026-08-06 | 旧 V1 release 不进入自动 GC | 未有 V2 manifest 的历史构件只能显式迁移或清理 |
| 2026-08-06 | 首个 Candidate 可在 legacy Active 下独立运行 | prepare 只操作 8001/18002，并前后证明 Active anchor 不变 |
| 2026-08-06 | legacy→固定 Active 只在 Candidate 验收后单独迁移 | 首次迁移是历史边界，不进入永久 recovery/controller |
| 2026-08-06 | Active 可以长期落后 main | main 是待测代码来源，不是自动生产版本指针 |
| 2026-08-06 | www 批准只更新 Active | intl 沿用既有独立 Active→intl 同步，不在 V2 自动编排 |
| 2026-08-06 | intl 同步失败不回退 www | intl 故障不连坐已经成功的国内 Active |
| 2026-08-07 | update 同 target 不轮换 previous | 报告丢失或中断后的重试不能破坏回滚点 |
| 2026-08-07 | `B/A -> A/A` 日常 rollback 语义由 Step 3B 取代 | 终审发现强杀窗口会让 B 暂时失去四指针引用 |
| 2026-08-07 | rollback 使用内核原子交换 `B/A -> A/B` | 两个版本全程受保护；同目标重试不 toggle，反向切换需再次授权 |
| 2026-08-07 | legacy 直接登记同业务版本基线后再准备新 Candidate | 旧代码无 Candidate 安全合同；首次真实升级前仍建立 A/A 回滚基线 |
| 2026-08-07 | V2 JATO admission 只用非阻塞应用锁 | 不等待、不写 marker、不建设部署维护平台 |
| 2026-08-07 | 原 archive 缺失时不从 live tree 冒充重建 | live tree 已含部署后 mutable/runtime 变化；A0 必须显式选择新 identity 或精确恢复原件 |
| 2026-08-07 | 原 archive 精确恢复后保持旧 commit/archive identity | 大小与 SHA 已逐字节命中；不再授权新 SHA adoption，也不从 live tree 推断 |
| 2026-08-07 | A0 使用可删除 helper，日常仍只有四操作 | exact archive 解决证据，但 legacy→A/A 仍没有安全编排入口；不污染 controller/workflow |
| 2026-08-07 | source seal critical policy 从 V1 收敛为 V2 | 修现有 policy 根因，不把废弃事故控制面复制进 A0 与未来 release |
| 2026-08-07 | A0 复用 sourceable runtime builder | 线上旧 venv 外指系统 Python；复用现有受限 builder，禁止复制依赖安装能力 |
| 2026-08-07 | Step 3F A0 helper 结论被独立审查否决 | local mock 未覆盖真实 unit、权限、runtime、durable path、恢复和强杀状态；未提交即删除 |
| 2026-08-07 | 首次 rollback 与最简 B/B adoption 交由用户明确选择 | 两者安全属性不同，不能由实现者静默降低首次回退能力 |
| 2026-08-07 | 用户选择首次 legacy→B/B | 不建立 A/A helper/recovery；接受下一次 C/B 前没有 distinct rollback |
| 2026-08-07 | shared template 迁移只写显式 @8000 override | 与腾讯云真实 FragmentPath 一致；失败可删除 override 回到原模板 |
| 2026-08-08 | 固定 Active Nginx 迁移只改契约、不改路由 | 8000、TLS、缓存、API timeout 和页面行为均保持，先满足 Candidate 前置条件 |
| 2026-08-08 | systemd 多值属性按保序语义比较 | 兼容 systemd 255 的换行输出，同时继续拒绝缺失、额外或重排项 |
| 2026-08-08 | restart 后只增加有界就绪等待 | 修复真实服务器启动竞态；确定性身份/配置错误仍立即拒绝，不新增恢复系统 |
| 2026-08-08 | 4,200 从字面硬上限改为强制审查线 | 避免为过线而格式压缩/拆文件；结构边界与单次 60 行根因修复预算更能阻止平台膨胀 |
| 2026-08-08 | Candidate 使用固定上海 HTTPS 地址 | 每次 prepare 只替换固定 8001/18002 内容；地址不变，Active/intl 不随 Candidate 变化 |
| 2026-08-08 | Candidate 公网网关独立于 Active vhost | Basic Auth 覆盖全部路径，且任何失败都不能回退显示 8000 Active |
| 2026-08-09 | Candidate 使用生产快照的独立可写数据库 | 能验证真实写入交互，同时测试数据不进入 Active 数据库 |
| 2026-08-09 | Candidate 数据库 FIFO 容量 1 | 稳态占用最小；新版本失败时仍保留上一个可测试 Candidate |
| 2026-08-09 | Candidate 应用免登录 admin，网关访问控制保留 | 省去重复应用账号；生产快照仍不能向公网匿名暴露 |
| 2026-08-09 | Active 更新不复制 Candidate 数据 | 上线只复用已测 immutable release，Candidate 测试写入永不提升 |
