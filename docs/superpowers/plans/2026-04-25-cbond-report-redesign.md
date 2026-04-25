# CBond Report Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the static HTML report so it is visually stronger, more interactive, and easier to maintain without changing the existing daily Python pipeline.

**Architecture:** Keep Markdown as the handoff format, but insert a dedicated view-model layer between parsing and rendering. The Python side will normalize chart/list/detail payloads, and the browser will render a richer dashboard with a smaller, cleaner state model.

**Tech Stack:** Python 3.9+, Jinja2, vanilla JavaScript, ECharts, unittest

---

### Task 1: Lock in parser behavior with regression tests

**Files:**
- Create: `D:/cbond/tests/test_render_markdown_parser.py`
- Test: `D:/cbond/scripts/render_markdown_parser.py`

- [ ] **Step 1: Write the failing test**

```python
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from render_markdown_parser import parse_markdown


class ParseMarkdownTests(unittest.TestCase):
    def test_parses_summary_strategy_and_cards(self):
        sample = """# 可转债概览 · 2026-04-24

## 摘要
- 总数 2 只

## 策略推荐
### 双低
*经典双低*
| 排名 | 转债 | 正股 | 价格 | 转股溢价率 | PE | 20日σ | 综合得分 |
|---|---|---|---|---|---|---|---|
| 1 | 测试转债 (123001.SZ) | 测试股份 | 120.00 | 8.20% | 18.0 | 35.0% | 11.0 |

## AI应用
### 测试转债 (123001.SZ)
| 正股 | 行业 | 价格 | 涨跌幅 | 转股溢价率 | 纯债溢价率 | 20日年化σ | 纯债YTM | 余额(亿) | 评级 | 到期 | 相对价值 | Delta | 强赎 | 下修 | 策略分 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 测试股份 (000001.SZ) | 软件开发 | 120.00 | 1.25% | 8.20% | 12.30% | 35.0% | 1.1% | 5.00 | AA | 2030-01-01 | 0.95 | 0.66 | 已触发3天 | 触发≤85% | 双低#1 |
**主营**：主营测试业务。
**时序**：dates=2026-04-23,2026-04-24 delta=0.60,0.66 rv=1.02,0.95
**题材**：`#AI应用` `#信创-国产替代`
"""
        report = parse_markdown(sample)
        self.assertEqual(report["title"], "可转债概览 · 2026-04-24")
        self.assertEqual(report["summary"], ["总数 2 只"])
        self.assertEqual(report["strategy_picks"][0]["strategy"], "双低")
        self.assertEqual(report["sections"][0]["theme"], "AI应用")
        self.assertEqual(report["sections"][0]["cards"][0]["themes"], ["AI应用", "信创-国产替代"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_render_markdown_parser -v`
Expected: FAIL because the test file does not exist yet.

- [ ] **Step 3: Write minimal implementation**

No production change in this task. Create the test file and import the current parser.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_render_markdown_parser -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_render_markdown_parser.py
git commit -m "test: add markdown parser regression coverage"
```

### Task 2: Add view-model tests before introducing the new abstraction

**Files:**
- Create: `D:/cbond/tests/test_report_view_model.py`
- Create: `D:/cbond/scripts/report_view_model.py`

- [ ] **Step 1: Write the failing test**

```python
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from report_view_model import build_dashboard_view_model


class ViewModelTests(unittest.TestCase):
    def test_builds_highlight_cards_and_detail_payloads(self):
        report = {
            "title": "可转债概览 · 2026-04-24",
            "summary": ["总数 2 只"],
            "strategy_picks": [],
            "appendix": [],
            "sections": [
                {
                    "theme": "AI应用",
                    "cards": [
                        {
                            "bond_name": "测试转债",
                            "bond_code": "123001.SZ",
                            "stock": "测试股份 (000001.SZ)",
                            "industry": "软件开发",
                            "price": "120.00",
                            "day_chg": "1.25%",
                            "conv": "8.20%",
                            "pure": "12.30%",
                            "vol": "35.0%",
                            "pure_bond_ytm": "1.1%",
                            "relative_value": "0.95",
                            "delta": "0.66",
                            "balance": "5.00",
                            "rating": "AA",
                            "maturity": "2030-01-01",
                            "call_status": "已触发3天",
                            "down_status": "触发≤85%",
                            "strategy": "双低#1",
                            "business": "主营测试业务。",
                            "themes": ["AI应用", "信创-国产替代"],
                            "sparkline": {"delta": [0.60, 0.66], "rv": [1.02, 0.95]},
                        }
                    ],
                }
            ],
        }
        vm = build_dashboard_view_model(report, "2026-04-24", None)
        self.assertEqual(vm["hero"]["title"], "可转债概览 · 2026-04-24")
        self.assertEqual(vm["kpis"]["undervalued"], 1)
        self.assertEqual(vm["highlights"][0]["bond_code"], "123001.SZ")
        self.assertEqual(vm["explorer"]["items"][0]["detail"]["themes"], ["AI应用", "信创-国产替代"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_report_view_model -v`
Expected: FAIL with `ModuleNotFoundError` because `report_view_model.py` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create `report_view_model.py` with a single public function:

```python
def build_dashboard_view_model(report, trade_date, backtest):
    return {
        "hero": {"title": report.get("title", ""), "trade_date": trade_date, "summary": report.get("summary", [])},
        "kpis": {"undervalued": 0},
        "highlights": [],
        "explorer": {"items": []},
        "backtest": backtest,
    }
```

Then expand it only until the test passes.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_report_view_model -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_report_view_model.py scripts/report_view_model.py
git commit -m "test: add report view model coverage"
```

### Task 3: Refactor Python rendering around the new view-model layer

**Files:**
- Modify: `D:/cbond/scripts/render_html.py`
- Modify: `D:/cbond/scripts/render_markdown_parser.py`
- Modify: `D:/cbond/scripts/report_view_model.py`
- Test: `D:/cbond/tests/test_render_markdown_parser.py`
- Test: `D:/cbond/tests/test_report_view_model.py`

- [ ] **Step 1: Write the failing test**

Extend `test_report_view_model.py` with an assertion that chart data and explorer filter options are generated:

```python
self.assertTrue(vm["market_radar"]["scatter_points"])
self.assertIn("AI应用", vm["explorer"]["theme_options"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_report_view_model -v`
Expected: FAIL because the new keys are missing.

- [ ] **Step 3: Write minimal implementation**

Implement helpers in `report_view_model.py` for:

```python
def normalize_card(card, theme, idx): ...
def build_kpis(cards): ...
def build_highlights(cards): ...
def build_market_radar(cards): ...
def build_explorer(cards): ...
```

Update `render_html.py` so it:

```python
from report_view_model import build_dashboard_view_model

view_model = build_dashboard_view_model(report, args.trade_date, backtest)
html_out = tpl.render(
    title=args.title,
    view_model=view_model,
    css=css,
    js=js,
)
```

Keep `parse_markdown()` focused on parsing only.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest discover -s tests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/render_html.py scripts/render_markdown_parser.py scripts/report_view_model.py tests/test_render_markdown_parser.py tests/test_report_view_model.py
git commit -m "refactor: add dashboard view model pipeline"
```

### Task 4: Rewrite the template, styles, and browser interactions

**Files:**
- Modify: `D:/cbond/scripts/templates/base.html.j2`
- Modify: `D:/cbond/scripts/static/style.css`
- Modify: `D:/cbond/scripts/static/app.js`

- [ ] **Step 1: Write the failing test**

Add a rendering smoke test to `tests/test_report_view_model.py`:

```python
from jinja2 import Environment, FileSystemLoader

template = Environment(loader=FileSystemLoader(str(ROOT / "scripts" / "templates"))).get_template("base.html.j2")
html = template.render(title="x", view_model=vm, css="body{}", js="console.log('x')")
self.assertIn("market-radar", html)
self.assertIn("detail-drawer", html)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_report_view_model -v`
Expected: FAIL because the old template does not contain the new landmarks.

- [ ] **Step 3: Write minimal implementation**

Rewrite the front end with these landmarks and behaviors:

```html
<section class="hero">...</section>
<section class="market-radar">...</section>
<section class="strategy-deck">...</section>
<section class="explorer-workbench">...</section>
<aside class="detail-drawer" id="detailDrawer">...</aside>
```

And JS hooks:

```javascript
const state = { query: "", theme: "", quick: "all", view: "cards", sortKey: "relative_value", sortDir: "asc", selectedBond: null };
```

Support:
- card/table view toggle
- scatter click to open detail
- highlight chips
- detail drawer open/close
- filtered export/copy
- light-first visual design with no dark/black page background

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest discover -s tests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/templates/base.html.j2 scripts/static/style.css scripts/static/app.js tests/test_report_view_model.py
git commit -m "feat: redesign static cbond report ui"
```

### Task 5: End-to-end verification and report regeneration

**Files:**
- Modify: `D:/cbond/README.md`
- Generate: `D:/cbond/reports/2026-04-24/cbond_overview.html` (or a fresh verification artifact if preferred)

- [ ] **Step 1: Write the failing test**

No new code test. This task uses full-pipeline verification commands.

- [ ] **Step 2: Run rendering verification**

Run:

```bash
python scripts/render_html.py --in reports/2026-04-24/cbond_overview.md --out reports/2026-04-24/cbond_overview.html --title "可转债概览 · 2026-04-24" --trade-date 2026-04-24
```

Expected: command exits 0 and writes the HTML file.

- [ ] **Step 3: Run static verification**

Run:

```bash
python -m py_compile scripts/render_html.py scripts/render_markdown_parser.py scripts/report_view_model.py
python -m unittest discover -s tests -v
```

Expected: all commands exit 0.

- [ ] **Step 4: Run manual UI verification**

Open the generated report in a browser and verify:
- scatter chart renders
- card/table toggle works
- search and filters affect counts
- clicking a bond opens the detail drawer
- export/copy use filtered results

- [ ] **Step 5: Commit**

```bash
git add README.md reports/2026-04-24/cbond_overview.html
git commit -m "docs: refresh report workflow notes"
```
