# Fullstack Development Spec（2026-04-11）

## 1. 目的

这份规范用于减少以下类型的问题：

- 前后端字段名不一致。
- 页面依赖了“猜测中的字段”，而不是后端真实 contract。
- 功能已经写完，但没有最小单测，导致回归只能靠人工点击发现。

核心原则不是“写了文档就一定没 bug”，而是把高频错误从“靠记忆避免”改成“靠规范和测试阻断”。

## 2. 适用范围

以下改动必须先补规范再开发：

- 新页面接新接口。
- 现有接口新增、重命名、删除字段。
- 任何跨前端、后端、数据库的业务语义变更。
- 任何需要 review / materialize / batch / reporting 串联的功能。

纯样式调整、文案调整、小范围本地重构可不单独起一份专题 spec，但仍要满足测试和验收要求。

## 3. 开发前必须写清楚的内容

每个跨层功能至少要明确以下内容：

1. 数据来源：字段来自哪个 endpoint / serializer / table。
2. canonical contract：以后端 serializer 返回字段为准，不能以前端猜测命名为准。
3. 字段语义：例如 observedAtUtc 表示抓取观察时间，updatedAtUtc 表示当前价格记录更新时间，不能混用。
4. fallback 规则：是否兼容旧字段，兼容多久，谁负责移除。
5. 错误态：空数据、null、接口失败、字段缺失时页面怎么展示。
6. 验收样例：至少列 1 个正常样例、1 个空值样例、1 个回退样例。

## 4. 开发执行顺序

必须按下面顺序执行：

1. 写或补专题 spec。
2. 明确 contract owner。
   当前项目默认：后端 serializer 是 API contract 唯一事实来源。
3. 实现功能。
4. 补单元测试。
5. 执行本地验证。
6. 更新相关文档入口和说明。

不允许跳过第 4 步直接结束。

## 5. Contract 规则

- 后端返回字段一旦对外暴露，前端 type 和页面消费逻辑必须同步更新。
- 同一个业务对象不能同时混用 observation contract 和 current price contract。
- 如果必须兼容旧字段，兼容逻辑要集中在一个 helper / adapter 中，不能散落在多个页面。
- serializer 改名时，必须同时改：
  - 前端 type
  - 页面 helper / adapter
  - 单元测试
  - 文档中的 contract 描述

## 6. 测试最低要求

如果仓库原来没有 unit test 目录，开发者需要主动创建。

当前项目约定：

- 前端单测目录：06_AppPlatform/frontend/src/tests/unit
- 后端单测目录：06_AppPlatform/backend/tests/unit

每次做功能改动，至少要补以下之一：

- 纯前端逻辑：补前端 unit test。
- 纯后端 contract：补后端 unit test。
- 跨层 contract：前后端各至少一条用例，锁住字段名和语义。

## 7. 当前项目推荐验证命令

前端：

```bash
cd 06_AppPlatform/frontend
npm run check:frontend
```

后端：

```bash
cd 06_AppPlatform/backend
pip install -r requirements-dev.txt
python -m pytest tests/unit
```

## 8. 对这次 MSRP 问题的直接结论

这次问题的根因不是“代码一定写错”，而是“current price contract 没有被文档化并用测试固定住”。

实际教训：

- observation 返回 msrpValue / observedAtUtc。
- current price 返回 currentMsrpValue / lastPriceChangeAtUtc / updatedAtUtc。
- 页面之前把两种 contract 混成了一种，所以会在运行时访问 undefined。

后续同类页面必须先看 serializer，再写 frontend type 和 UI。