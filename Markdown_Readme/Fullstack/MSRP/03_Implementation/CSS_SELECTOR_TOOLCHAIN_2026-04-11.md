# CSS Selector 检查工具链

Date: 2026-04-11

## Playwright CLI（官方 Codegen / Inspector）

仓库：https://github.com/microsoft/playwright-cli.git

### 用途

- 在浏览器中**交互式点选**页面元素，自动生成 CSS / XPath selector
- 可用于验证和填充 draft YAML 中 `profile.css.*` 字段（当前都是 `TODO_SELECTOR`）
- 配合 `playwright codegen <URL>` 录制操作流程，直接生成 Python/JS 测试脚本

### 安装

```bash
# npm 全局安装
npm i -g playwright
npx playwright install

# 或直接 pip (Python binding)
pip install playwright
playwright install
```

### 常用命令

```bash
# 打开 Codegen 录制器，交互式获取 selector
npx playwright codegen https://www.volkswagen.at/modelle/tiguan

# 指定 viewport / locale
npx playwright codegen --viewport-size=1280,720 --lang=de-AT https://www.toyota.at/new-cars/yaris-cross

# 用 Python binding
playwright codegen https://www.skoda-auto.cz/modely/kodiaq
```

### 工作流集成

1. 对每个 draft YAML 中的 `source_url` 运行 `playwright codegen <url>`
2. 在 Inspector 面板点选价格、车型名、动力总成标签
3. 将生成的 selector 填入 `profile.css.price` / `profile.css.model_name` / `profile.css.powertrain_label`
4. 关闭 codegen，将 selector 写回 YAML

### 批量处理思路

```python
# 从 draft YAML 中提取所有 source_url → 生成 playwright codegen 命令列表
import yaml, glob
for f in glob.glob("07_ScrapingToolkit/source_drafts/**/*.yaml", recursive=True):
    doc = yaml.safe_load(open(f))
    url = doc.get("source_url", "")
    if "todo.invalid" not in url:
        print(f"playwright codegen {url}")
```

> **注意**: CSS selector 填充需要逐站人工确认，不能纯脚本化——因为每个品牌官网结构不同。
> Playwright CLI 的价值在于加速这个人工过程。
