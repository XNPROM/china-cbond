# China CBond Monitor — 可转债全景扫描

全市场可转债（~335只）全景扫描系统。每日自动抓取行情、计算 BS 定价与希腊字母、运行多策略选券、按题材分组，生成一份暗色主题交互式 HTML 仪表盘（支持亮色切换）并通过 GitHub Pages 发布。

## 报告预览

最新报告自动部署至 GitHub Pages：`https://xnprom.github.io/china-cbond/`

## 架构总览

```
iFinD API ──→ raw CSV/JSON (data/raw/asof=YYYY-MM-DD/)
                    ↓
               DuckDB (data/cbond.duckdb) ← 7 张表 + 索引
                    ↓
         assemble_dataset.py (SQL JOIN) → dataset.json
                    ↓
         bs_pricing.py → DB upsert + dataset.json 回写 (BS定价, 相对价值, 希腊字母)
                    ↓
         strategy_score.py → DB upsert (双低, 双低-偏股/平衡/偏债, 低估)
                    ↓
         generate_themes_direct.py → themes.jsonl + DB upsert
                    ↓
         build_overview_md.py → cbond_overview.md
                    ↓
         render_html.py → cbond_overview.html (含回测净值曲线)
```

## 每日刷新流程

```bash
ASOF=2026-04-23

# Step 1: 估值快照 + PE/市值 (~2min)
python scripts/fetch_valuation.py \
    --codes    data/raw/asof=2026-04-20/cbond_codes.txt \
    --universe data/raw/asof=2026-04-20/cbond_universe.json \
    --date     $ASOF \
    --out      data/raw/asof=$ASOF/valuation.csv

# Step 2: 20日年化波动率 (~1.5min)
python scripts/compute_volatility.py \
    --universe data/raw/asof=2026-04-20/cbond_universe.json \
    --asof     $ASOF \
    --lookback-days 45 \
    --out data/raw/asof=$ASOF/vol_20d.csv

# Step 3: 组装数据集 (即时; SQL JOIN)
python scripts/assemble_dataset.py \
    --trade-date $ASOF \
    --out data/raw/asof=$ASOF/dataset.json

# Step 4: BS 定价 + 希腊字母 (即时; 纯数学, 无 API)
python scripts/bs_pricing.py \
    --dataset    data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF

# Step 5: 策略评分 (即时)
python scripts/strategy_score.py \
    --dataset    data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF \
    --out        data/raw/asof=$ASOF/strategy_picks.jsonl

# Step 6: 题材分类 (即时)
python scripts/generate_themes_direct.py \
    --dataset data/raw/asof=$ASOF/dataset.json \
    --out     data/raw/asof=$ASOF/themes.jsonl \
    --trade-date $ASOF

# Step 7: 生成 Markdown (即时)
python scripts/build_overview_md.py \
    --dataset    data/raw/asof=$ASOF/dataset.json \
    --trade-date $ASOF \
    --out        reports/$ASOF/cbond_overview.md \
    --title-date $ASOF

# Step 8: 渲染 HTML (即时)
python scripts/render_html.py \
    --in         reports/$ASOF/cbond_overview.md \
    --out        reports/$ASOF/cbond_overview.html \
    --title      "可转债概览 · $ASOF" \
    --trade-date $ASOF
```

## 首次部署

```bash
# 初始化数据库
python scripts/init_db.py

# 回填历史数据
python scripts/backfill.py --raw data/raw/asof=2026-04-20 --trade-date 2026-04-20
```

### 全量可转债抓取（~30s）

```bash
python scripts/fetch_cb_universe.py --date 20260424
```

## 脚本说明

| 脚本 | 功能 | 耗时 |
|---|---|---|
| `fetch_valuation.py` | iFinD 批量抓取：价格、溢价率、评级、余额、强赎/下修条款、PE/PB/市值、纯债YTM、纯债价值、到期赎回价等 | ~2min |
| `compute_volatility.py` | 正股 20 日年化对数收益率波动率 | ~1.5min |
| `assemble_dataset.py` | DuckDB SQL JOIN 组装全字段 dataset.json | 即时 |
| `bs_pricing.py` | BS 看涨期权定价 + 纯债价值 = 理论价值，计算相对价值与 Delta/Gamma/Theta/Vega | 即时 |
| `strategy_score.py` | 双低(Top30) + 分域双低(偏股/平衡/偏债各Top10) + 低估(Top10) | 即时 |
| `generate_themes_direct.py` | 基于正股简介的关键词规则题材分类 | 即时 |
| `build_overview_md.py` | 读取 DB 中的策略与题材数据，生成结构化 Markdown | 即时 |
| `render_html.py` | Markdown → 交互式暗色仪表盘 HTML（Jinja2 + ECharts + 排序筛选导出） | 即时 |
| `backtest_weekly.py` | 周度再平衡回测引擎（含 T+1 入场、滑点、佣金） | ~4min |
| `fetch_cb_universe.py` | 通过 data_pool p05479 一次性拉取全市场可转债 + 申万行业 | ~30s |
| `fetch_underlying_profile.py` | 抓取正股公司简介文本 | ~1min |
| `init_db.py` | 初始化 DuckDB schema | 即时 |
| `backfill.py` | 从 raw 目录回填历史数据到 DB | 即时 |
| `refresh_data.py` | 数据新鲜度检测 + iFinD 重新拉取过期字段 | ~2min |
| `validate_data.py` | 数据质量校验（universe 规模、字段完整度、值域范围） | 即时 |
| `_etl_log.py` | ETL 运行日志上下文管理器（写入 etl_runs 表） | — |
| `report_view_model.py` | 仪表盘载荷构建器（解析 Markdown → JSON view model） | — |

## 共享基础设施 (`scripts/_*.py`)

| 模块 | 功能 |
|---|---|
| `_auth.py` | iFinD access_token 生命周期管理（缓存 6h，refresh_token 1 年）。Token 文件：`~/.codex_logs/ifind_refresh_token.txt` |
| `_ifind.py` | iFinD HTTP 接口封装：`basic_data_service`、`real_time_quotation`、`cmd_history_quotation`，含 `batched()` 批量助手 |
| `_db.py` | DuckDB 连接 (`data/cbond.duckdb`)、`init_schema()`、通用 `upsert()` ON CONFLICT DO UPDATE |

## DuckDB 数据库设计（7 张表 + 索引）

### universe — 券种静态信息

| 列名 | 类型 | 说明 |
|---|---|---|
| code | VARCHAR PK | 转债代码 (如 110073.SH) |
| name | VARCHAR | 转债名称 |
| ucode | VARCHAR | 正股代码 |
| uname | VARCHAR | 正股名称 |
| list_date | VARCHAR | 上市日期 |
| maturity_date | VARCHAR | 到期日期 |
| updated_at | VARCHAR | 更新时间 |

### valuation_daily — 日度估值与定价

| 列名 | 类型 | 说明 |
|---|---|---|
| trade_date | VARCHAR PK | 交易日期 |
| code | VARCHAR PK | 转债代码 |
| price | DOUBLE | 转债收盘价 (元) |
| conv_prem_pct | DOUBLE | 转股溢价率 (%) |
| pure_prem_pct | DOUBLE | 纯债溢价率 (%) |
| outstanding_yi | DOUBLE | 余额 (亿元) |
| rating | VARCHAR | 信用评级 |
| maturity_date | VARCHAR | 到期日 |
| change_pct | DOUBLE | 日涨跌幅 (%) |
| conv_price | DOUBLE | 转股价 (元) |
| no_call_start | VARCHAR | 不强赎承诺起始日 |
| no_call_end | VARCHAR | 不强赎承诺截止日 |
| call_trigger_days | INTEGER | 强赎已触发天数 |
| call_trigger_ratio | DOUBLE | 强赎触发比例 (%) |
| has_down_revision | VARCHAR | 是否有下修条款 |
| down_trigger_ratio | DOUBLE | 下修触发比例 (%) |
| ths_industry | VARCHAR | 同花顺行业 |
| pb | DOUBLE | 正股 PB |
| redemp_stop_date | VARCHAR | 强赎停牌日 |
| pe_ttm | DOUBLE | 正股 PE(TTM) |
| total_mv_yi | DOUBLE | 正股总市值 (亿元) |
| implied_vol | DOUBLE | 隐含波动率 (iFinD，暂无数据) |
| pure_bond_ytm | DOUBLE | 纯债到期收益率 (%) |
| ifind_doublelow | DOUBLE | iFinD 内置双低值 |
| option_value | DOUBLE | iFinD 期权价值 |
| surplus_days | INTEGER | 剩余期限 (天) |
| surplus_years | DOUBLE | 剩余期限 (年) |
| accum_conv_ratio | DOUBLE | 累计转股比例 (%) |
| dilution_ratio | DOUBLE | 转股稀释比例 (%) |
| bs_value | DOUBLE | BS 理论价值 (元) |
| relative_value | DOUBLE | 相对价值 = 市价 / BS理论价值 |
| bs_delta | DOUBLE | BS Delta (0=纯债, 1=纯股) |
| bs_gamma | DOUBLE | BS Gamma |
| bs_theta | DOUBLE | BS Theta (每日) |
| bs_vega | DOUBLE | BS Vega (每 1% 波动率变化) |
| pure_bond_value | DOUBLE | 纯债价值 (iFinD) |
| maturity_call_price | DOUBLE | 到期赎回价 (iFinD) |

### vol_daily — 正股波动率

| 列名 | 类型 | 说明 |
|---|---|---|
| trade_date | VARCHAR PK | 交易日期 |
| ucode | VARCHAR PK | 正股代码 |
| vol_20d_pct | DOUBLE | 20 日年化波动率 (小数, 0.35=35%) |
| n_samples | INTEGER | 样本数 (<20 说明数据不足) |

### underlying_profile — 正股公司简介

| 列名 | 类型 | 说明 |
|---|---|---|
| ucode | VARCHAR PK | 正股代码 |
| uname | VARCHAR | 正股名称 |
| industry | VARCHAR | 行业 |
| main_business | VARCHAR | 主营业务文本 |
| updated_at | VARCHAR | 更新时间 |

### strategy_picks — 策略选券

| 列名 | 类型 | 说明 |
|---|---|---|
| trade_date | VARCHAR PK | 交易日期 |
| code | VARCHAR PK | 转债代码 |
| strategy | VARCHAR PK | 策略名 (双低/双低-偏股/双低-平衡/双低-偏债/低估) |
| rank_overall | DOUBLE | 综合得分 |
| rank_conv_prem | INTEGER | 溢价率排名 |
| rank_price | INTEGER | 价格排名 |
| note | VARCHAR | 策略说明 |

### themes — 题材分类

| 列名 | 类型 | 说明 |
|---|---|---|
| trade_date | VARCHAR PK | 交易日期 |
| code | VARCHAR PK | 转债代码 |
| theme_l1 | VARCHAR | 主题材 |
| all_themes_json | VARCHAR | 所有题材 (JSON 数组) |
| business_rewrite | VARCHAR | 精简主营业务描述 |
| industry | VARCHAR | 申万一级行业 |

### etl_runs — ETL 运行日志

| 列名 | 类型 | 说明 |
|---|---|---|
| run_id | VARCHAR PK | 运行 ID |
| trade_date | VARCHAR | 交易日期 |
| step | VARCHAR | 步骤名 |
| started_at | VARCHAR | 开始时间 |
| finished_at | VARCHAR | 结束时间 |
| row_count | INTEGER | 处理行数 |
| status | VARCHAR | 状态 |
| note | VARCHAR | 备注 |

## 策略体系

### 1. 经典双低 (Top 30)

筛选条件：PE > 0 且波动率 > Q1

排名公式：`score = 1.5 × rank(转股溢价率) + rank(价格)`

取分值最低的 30 只。

### 2. 分域双低 (偏股/平衡/偏债各 Top 10)

按转股溢价率将转债分为三域：

| 分域 | 条件 | 特征 |
|---|---|---|
| 偏股 | 溢价率 < 20% | 跟涨能力强，Delta 高 |
| 平衡 | 20% ≤ 溢价率 < 50% | 股债兼备 |
| 偏债 | 溢价率 ≥ 50% | 债底保护强，Delta 低 |

每域内独立运行双低排名，各取 Top 10。避免偏股型（天然低溢价率）垄断双低排名。

### 3. 低估策略 (Top 10)

按 BS 相对价值（市价 / 理论价值）升序排列，取最低的 10 只。相对价值 < 1.0 表示市场价低于 BS 模型理论价。

## BS 定价模型

```
S = 转股价值 = 市价 / (1 + 转股溢价率/100)
K = 到期赎回价 (iFinD ths_maturity_call_price_cbond，缺省 110)
σ = 20 日年化波动率
r = 纯债到期收益率 (iFinD ths_pure_bond_ytm_cbond，缺省 2.5%)
T = 剩余年限 (iFinD ths_remain_duration_y_cbond，缺省 2 年)

BS 看涨期权价值 = S·N(d1) - K·e^(-rT)·N(d2)
理论价值 = BS 看涨期权 + 纯债价值 (iFinD ths_pure_bond_value_cbond)
相对价值 = 市价 / 理论价值
```

希腊字母同步计算：Delta (正股敏感度)、Gamma (凸度)、Theta (时间衰减/天)、Vega (波动率敏感度/1%)。

注意：该模型为纯 BS 定价，**未包含赎回条款和下修条款**，因此偏股型转债的理论价值会偏高（低估了赎回风险），使用时需注意。

## 题材分类

两套方案：

1. **`generate_themes_direct.py`**（默认）：基于正股简介文本的确定性关键词规则。包含 `THEME_RULES`（关键词→题材映射）、`THEME_OVERRIDES`（手工修正）和 `THEME_TO_INDUSTRY`（题材→申万行业）。
2. **`generate_themes_with_claude.py`**：批量调用 Claude Sonnet 获得更高质量分类。

题材词表定义在 `theme_vocabulary.md`，~85 个白名单标签，按板块分组（TMT、新能源、汽车、军工、医药等）。每只券最多 4 个标签。

## HTML 报告功能

- **暗色仪表盘**：暗色主题交互式仪表盘（支持亮色切换，localStorage 持久化）
- **KPI 概览卡**：总数、均价、中位溢价率、中位相对价值、低估数、板块分布
- **列排序**：点击表头排序（升序 → 降序 → 默认）
- **回测净值曲线**：ECharts 绘制周度再平衡策略净值曲线（含 dataZoom）
- **SVG 迷你图**：Delta 和相对价值趋势 sparkline
- **筛选**：文本搜索、题材下拉、快捷筛选按钮
- **导出**：导出 CSV / 复制代码
- **板块徽章**：板块色标、相对价值色彩编码、强赎/下修状态徽章
- **移动端适配**：<640px 自动切换卡片布局

## GitHub Pages 自动部署

`.github/workflows/deploy-pages.yml`：当 `reports/` 目录有新 push 时自动部署最新报告到 GitHub Pages。

## 技术栈与约定

| 项目 | 说明 |
|---|---|
| Python | 3.9+，stdlib + `duckdb` + `jinja2`，无 pandas/numpy |
| 数据源 | iFinD 量化 API (同花顺) |
| 数据库 | DuckDB 单文件 (`data/cbond.duckdb`) |
| 目录 | `data/raw/asof=YYYY-MM-DD/` 存原始快照，`reports/YYYY-MM-DD/` 存输出 |
| 单位 | 余额=亿元，价格=元，溢价率/波动率/YTM=%，相对价值=无量纲 |
| 双写 | 所有 fetch 脚本同时写 CSV/JSON 扁平文件 **和** DuckDB |
| assemble | `bs_pricing.py` 会把 BS 字段回写到 `dataset.json`，日常流程无需再次 `assemble` |

## 已知限制

- iFinD `ths_concept_*` 字段全部返回 ERR，无法获取结构化概念/板块数据，题材分类依赖正股简介文本
- iFinD `ths_implied_volatility_cbond` 隐含波动率字段对所有历史日期均返回 None，页面中该字段暂显示 "—"
- 余额为 0 的券视为已退市（强制赎回）
- 新上市不足 20 个交易日的券波动率样本不足，`vol_daily.n_samples` 可供判断
- Anaconda Python 与 iFinD 存在 SSL 握手问题，建议使用系统 Python
- BS 定价模型未包含赎回条款和下修条款，偏股型转债理论价值偏高
- 回测依赖历史 valuation_daily 数据的 PE/vol 字段，若历史数据缺失则双低策略可能选不出券

## 更新日志

### 2026-04-27

**UI 改进 (4项)**
1. 散点图去掉气泡大小（余额），改为统一等大圆点；去掉 "未分域" 显示
2. 卡片文字溢出修复：债券名称、副标题、统计数值均加 `text-overflow: ellipsis` 截断
3. 卡片新增纯债溢价率和隐含波动率字段（6 字段 3 列布局）
4. 强赎/下修状态改进：未触发显示绿色安全标签，已触发显示警告图标

**数据修复**
- 纯债溢价率 (`pure_prem`) 已修复，332/332 全部有数据
- 低估策略拓宽候选池（不再要求 PE>0），PE 为负的转债（如宏图转债）也可入选

**回测引擎修复**
- 修复 iFinD 历史 PE/波动率全部返回 0 的问题：改从正股历史数据 `history(ucode, 'pe_ttm')` 取 PE，从价格序列计算 20 日实现波动率
- 交易日推导改为全品种价格日期并集（243 日 vs 此前 63 日）
- 回测结果：双低 Top10 +35.9%，双低偏股 Top10 +94.6%，中证转债 +14.2%
