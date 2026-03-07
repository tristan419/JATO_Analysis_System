# 日志采集接入预留（v1）

> 文档定位：日志接入规范、字段约定与可观测演进方向。
> 返回总览：[ROADMAP（总览导航）](./ROADMAP.md)

## 当前能力

- 已新增 `dashboard/logging_utils.py`
- 已新增 `03_Scripts/logging_utils.py`
- `runner.py` 已接入基础日志，支持环境变量 `JATO_LOG_LEVEL`
- `runner.py` 已接入 `sessionId`（单次页面会话内稳定）
- 支持 `JATO_LOG_JSON=1` 输出 JSON 日志
- 日志已覆盖关键路径：数据源解析、健康检查告警、加载模式与耗时
- ETL / 分区 / 刷新脚本已统一接入日志格式与 `jobId`

## 建议接入路径

1. 本地/VM：stdout 日志 + 系统日志收集（journald）
2. 云环境：stdout -> 平台日志（CloudWatch/Log Analytics）
3. 统一字段：时间、级别、模块、会话ID、筛选摘要

## 预留字段建议

- `sessionId`
- `datasetVersion`
- `cacheMode`
- `sidebarLoadSeconds`
- `analysisLoadSeconds`
- `detailLoadSeconds`
- `analysisRows`

## 下一步

- [x] 补日志脱敏规则（用户输入、路径）
- [x] 在 ETL/刷新脚本统一接入同一日志格式与 `jobId`
