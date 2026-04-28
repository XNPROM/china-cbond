<p align="center">
  <strong>China CBond Monitor</strong><br>
  <em>可转债全景扫描系统</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.9%2B-blue" alt="Python 3.9+">
  <img src="https://img.shields.io/badge/database-DuckDB-yellow" alt="DuckDB">
  <img src="https://img.shields.io/badge/data-iFinD%20API-orange" alt="iFinD">
  <img src="https://img.shields.io/badge/frontend-SVG%20%2B%20Jinja2-green" alt="SVG + Jinja2">
</p>

---

## 项目简介

覆盖全市场 **~335 只** 存续可转债的每日全景扫描系统。

核心能力：

- **数据采集** -- 通过 iFinD 量化 API 批量获取行情、估值、条款、正股基本面等 30+ 字段
- **量化定价** -- Black-Scholes 期权定价模型，输出理论价值、相对价值及四项希腊字母
- **多策略选券** -- 经典双低、分域双低（偏股/平衡/偏债）、低估策略三套体系
- **题材分类** -- 基于正股主营业务文本的确定性关键词规则引擎，覆盖 ~85 个板块标签
- **回测引擎** -- 周度再平衡回测，支持 T+1 入场、交易成本、乘法复利
- **交互报告** -- 暗色/亮色主题切换的单文件 HTML 仪表盘（原生 SVG 回测曲线、排序筛选、CSV 导出）

最终产物为一份 **自包含的 HTML 文件**，可直接浏览器打开或通过 GitHub Pages 发布。

---

## 架构总览

```
                     iFinD 量化 API
                          |
          +---------------+---------------+
          |               |               |
  fetch_valuation   compute_vol    fetch_universe
     (行情/条款)      (20日波动率)     (全市场券种)
          |               |               |
          +-------+-------+-------+-------+
                  |               |
             DuckDB 7表        raw CSV/JSON
          (data/cbond.duckdb)  (data/raw/asof=YYYY-MM-DD/)
                  |
       +----------+----------+
       |          |          |
  assemble    bs_pricing  strategy
  _dataset    (BS定价)     _score
       |          |          |
       +-----+----+----+----+
             |         |
      generate_themes  build_overview_md
             |              |
             +--------------+
                    |
              render_html.py
                    |
         cbond_scanner.html
         (交互式仪表盘报告)
```

**数据流向**：iFinD API --> raw 文件 + DuckDB 双写 --> SQL JOIN 组装 --> BS 定价 --> 策略评分 --> 题材分类 --> Markdown --> HTML

---

## 快速开始

### 环境要求

| 依赖 | 版本 | 说明 |
|:---|:---|:---|
| **Python** | >= 3.9 | 建议使用系统 Python（非 Anaconda，避免 SSL 问题） |
| **duckdb** | >= 0.9.0 | 嵌入式分析数据库 |
| **jinja2** | >= 3.1.0 | HTML 模板引擎 |
| **numpy** | >= 1.20 | 仅回测引擎使用（计算实现波动率） |
| **iFinD** | -- | 同花顺 iFinD 量化终端（需有效 Token） |

### 安装

```bash
git clone https://github.com/XNPROM/china-cbond.git
cd china-cbond
pip install -r requirements.txt
```

### 首次初始化

```bash
# 1. 初始化 DuckDB 数据库（创建 7 张表 + 索引）
python scripts/init_db.py

# 2. 拉取全市场可转债列表
python scripts/fetch_cb_universe.py --date 20260424

# 3. 拉取正股公司简介（用于题材分类）
python scripts/fetch_underlying_profile.py \
    --universe data/raw/asof=20260424/cbond_universe.json
```

### iFinD Token 配置

系统从以下路径读取 Token：

| 文件 | 内容 |
|:---|:---|
| `~/.codex_logs/ifind_refresh_token.txt` | Refresh Token（有效期 1 年） |
| `~/.codex_logs/ifind_access_token_cache.json` | Access Token 缓存（自动刷新，有效期 6 小时） |

---

## 每日刷新流程

8 步管线，每日运行一次（交易日），总耗时约 5-8 分钟：

```bash
ASOF=2026-04-24

# Step 1  估值快照 -- 价格/溢价率/评级/余额/条款/PE/PB/市值等 (~2min)
python scripts/fetch_valuation.py \
    --codes    data/raw/asof=20260424/cbond_codes.txt \
    --universe data/raw/asof=20260424/cbond_universe.json \
    --date     $ASOF \
    --out      data/raw/asof=$ASOF/valuation.csv

# Step 2  正股20日年化波动率 (~1.5min)
python scripts/compute_volatility.py \
    --universe data/raw/asof=20260424/cbond_universe.json \
    --asof     $ASOF \
    --lookback-days 45 \
    --out      data/raw/asof=$ASOF/vol_20d.csv

# Step 3  组装全字段数据集 (DuckDB SQL JOIN, <1s)
python scripts/assemble_dataset.py \
    --trade-date $ASOF \
    --out        data/dataset.json

# Step 4  BS定价 + 希腊字母 (纯数学计算, <1s)
python scripts/bs_pricing.py \
    --dataset    data/dataset.json \
    --trade-date $ASOF

# Step 5  策略评分 (<1s)
python scripts/strategy_score.py \
    --dataset    data/dataset.json \
    --trade-date $ASOF \
    --out        data/strategies.json

# Step 6  题材分类 (<1s)
python scripts/generate_themes_direct.py \
    --dataset    data/dataset.json \
    --out        data/themes.json \
    --trade-date $ASOF

# Step 7  生成结构化 Markdown (<1s)
python scripts/build_overview_md.py \
    --dataset    data/dataset.json \
    --trade-date $ASOF \
    --out        data/overview.md \
    --title-date $ASOF

# Step 8  渲染交互式 HTML 报告 (<1s)
python scripts/render_html.py \
    --in         data/overview.md \
    --out        output/cbond_scanner.html \
    --title      "可转债全景扫描 $ASOF" \
    --trade-date $ASOF \
    --backtest   data/raw/asof=$ASOF/backtest_weekly.json
```

> **Tips**：Steps 3-8 均为纯本地计算，无 API 调用，可在几秒内完成。
> 非交易日运行会因为没有新数据而输出与上一交易日相同的结果，这是正常的。

---

## 策略体系

### 1. 经典双低 (Top 30)

从全市场中筛选 **PE > 0** 且 **波动率 > Q1（25分位）** 的转债，按以下公式排名：

```
双低得分 = 1.5 x rank(转股溢价率) + rank(价格)
```

得分越低越优，取排名前 30 只。该策略寻找**价格低、溢价率低**的双重安全边际品种。

### 2. 分域双低 (偏股/平衡/偏债各 Top 10)

按 **BS Delta** 将转债分为三个域，每个域**独立运行双低排名**，各取 Top 10：

| 分域 | Delta 条件 | 特征 |
|:---|:---|:---|
| **偏股** | Delta >= 0.7 | 跟涨能力强，对正股敏感 |
| **平衡** | 0.4 <= Delta < 0.7 | 股债兼备，攻守均衡 |
| **偏债** | Delta < 0.4 | 债底保护强，下行空间小 |

**设计意图**：经典双低天然偏向低溢价率品种（偏股型），分域排名避免偏股型垄断，让三类风格的优质券均有机会入选。Delta 作为分域标准比溢价率更精确，因为它综合了溢价率、波动率、剩余年限等多维信息。

### 3. 低估策略 (Top 10)

按 **BS 相对价值**（市价 / BS 理论价值）升序排列，取最低的 10 只。

- 相对价值 < 1.0 -- 市价低于理论价值，模型判定为"低估"
- 相对价值 = 1.0 -- 合理定价
- 相对价值 > 1.2 -- 模型判定为"高估"

该策略不要求 PE > 0，因此**PE 为负的转债**（如亏损公司对应的转债）也可入选。

---

## BS 定价模型

采用 Black-Scholes 欧式看涨期权模型 + 纯债价值的加法框架：

```
输入参数:
  S = 转股价值 = 转债市价 / (1 + 转股溢价率/100)
  K = 到期赎回价          (iFinD ths_maturity_redemp_price_cbond, 缺省 110)
  sigma = 正股20日年化波动率     (compute_volatility.py 计算)
  r = 纯债到期收益率       (iFinD ths_pure_bond_ytm_cbond, 缺省 2.5%)
  T = 剩余年限            (iFinD ths_remain_duration_y_cbond, 缺省 2年)

计算:
  d1 = [ln(S/K) + (r + sigma^2/2) * T] / (sigma * sqrt(T))
  d2 = d1 - sigma * sqrt(T)

  BS期权价值 = S * N(d1) - K * e^(-rT) * N(d2)
  理论价值   = BS期权价值 + 纯债价值 (iFinD ths_pure_bond_value_cbond)
  相对价值   = 转债市价 / 理论价值

希腊字母:
  Delta = N(d1)                    正股价格敏感度 (0=纯债, 1=纯股)
  Gamma = N'(d1) / (S*sigma*sqrt(T))  Delta的变化速度 (凸度)
  Theta = -(S*N'(d1)*sigma)/(2*sqrt(T)) - r*K*e^(-rT)*N(d2)  时间衰减/天
  Vega  = S * N'(d1) * sqrt(T)    波动率敏感度/1%
```

> **注意**：该模型为纯欧式 BS 定价，**未包含赎回条款和下修条款的期权价值**。偏股型转债的理论价值可能偏高（低估了发行人提前赎回的权利），使用时需留意。

---

## 回测引擎

`backtest_weekly.py` 实现周度再平衡策略回测，采用 Portfolio 类逐只持仓追踪。

### 回测流程

```
T日收盘后 --> 按当日数据选券 --> T+1日收盘价买入
--> 持有5个交易日 --> 下次调仓日+1收盘价卖出 --> 计算区间收益 --> 下一轮
```

### Universe 过滤

| 过滤条件 | 说明 |
|:---|:---|
| 余额 >= 2 亿 | 排除流动性不足的小规模转债 |
| 价格 <= 150 元 | 排除过高价格的投机性品种 |
| PE > 0 | 排除亏损公司转债（低估策略除外） |
| 波动率 > Q1 | 排除波动率过低的品种 |
| 最少持仓 >= 5 | 可选券不足 5 只时该策略当期 N/A |

### 关键参数

| 参数 | 默认值 | 说明 |
|:---|:---|:---|
| 调仓周期 | 5 个交易日 | `--holding-days 5` |
| 持仓数量 | Top 10 | `--top 10` |
| 滑点 | 10 bps (单边) | `--slippage-bps 10` |
| 佣金 | 2 bps (往返) | `--commission-bps 2` |
| 仓位 | 等权 | 每只转债等权重 |

### 成本模型

基于 Portfolio 类的换手感知成本模型：

- **买入成本**（单边滑点 + 半佣金）仅对**新建仓**的持仓收取
- **卖出成本**（单边滑点 + 半佣金）仅对**退出**的持仓收取
- **持续持有**的仓位不收取任何交易成本

```
新建仓: net_return = gross_return - buy_cost
退出仓: exit_drag = (退出数 / 前期持仓数) * sell_cost
组合收益 = mean(各持仓 net_return) - exit_drag
累计净值 = 净值 * (1 + 本期组合收益)
```

### 风险指标

| 指标 | 计算方式 |
|:---|:---|
| **Sharpe Ratio** | 年化（周度收益均值 / 标准差 * sqrt(周数/年)），rf=0 |
| **最大回撤** | max((peak - trough) / peak) |
| **平均换手率** | 每期 (进出持仓数 / 总持仓数) 的均值 |
| **年化收益** | (终值净值 ^ (252/交易日数) - 1) |

**基准**：中证转债指数 (000832.CSI)；iFinD 不可用时回退为全市场等权收益

### 用法

```bash
# 近1年回测
python scripts/backtest_weekly.py \
    --start-date 2025-04-24 --end-date 2026-04-24

# 从DB读取已有数据（快速，无API调用）
python scripts/backtest_weekly.py \
    --start-date 2025-04-24 --end-date 2026-04-24 --from-db

# 自定义参数
python scripts/backtest_weekly.py \
    --start-date 2025-04-24 --end-date 2026-04-24 \
    --top 20 --holding-days 10 --slippage-bps 5
```

---

## 题材分类

两套方案：

| 方案 | 脚本 | 原理 | 适用场景 |
|:---|:---|:---|:---|
| **关键词规则**（默认） | `generate_themes_direct.py` | 正股主营业务文本 --> 关键词匹配 --> 题材标签 | 日常使用，确定性高，无外部依赖 |
| **LLM 辅助** | `generate_themes_with_claude.py` | 批量调用 Claude Sonnet 分类 | 初始化/校准时使用 |

关键词规则引擎包含三级配置：
- **`THEME_RULES`** -- 关键词 --> 题材映射（~200 条规则）
- **`THEME_OVERRIDES`** -- 手工修正（针对特定转债的硬编码题材）
- **`THEME_TO_INDUSTRY`** -- 题材 --> 申万一级行业映射

题材词表定义在 `theme_vocabulary.md`，约 **85 个白名单标签**，按板块分组（科技TMT、新能源电力、高端制造、医药医疗、材料化工等）。每只券最多 4 个标签。

---

## HTML 报告功能

生成的 HTML 报告为**单文件自包含**（所有 CSS/JS 内联），可直接浏览器打开。

| 功能 | 说明 |
|:---|:---|
| **主题切换** | 暗色/亮色主题，localStorage 持久化用户偏好 |
| **KPI 概览** | 总数、均价、中位溢价率、中位相对价值、低估数、板块分布 |
| **散点图** | 转股溢价率 vs 价格散点，按板块着色，相对价值色标 |
| **策略面板** | 双低/分域双低/低估策略推荐表格 |
| **回测净值** | 原生 SVG 绘制双低/分域双低/低估/中证转债净值曲线，离线打开也能显示 |
| **列排序** | 点击表头排序（升序 --> 降序 --> 默认三态切换） |
| **文本搜索** | 实时搜索债券名称、正股名称、代码、题材 |
| **题材筛选** | 下拉多选题材标签过滤 |
| **详情抽屉** | 点击任一转债展开详情面板（完整基本面+策略信息） |
| **CSV 导出** | 一键导出筛选后的数据为 CSV |
| **SVG 迷你图** | Delta 和相对价值历史趋势 sparkline |
| **卡片视图** | 移动端（< 640px）自动切换为卡片布局 |
| **状态徽章** | 强赎安全/警告、下修条款、策略入选等状态标签 |

---

## 数据库设计

DuckDB 单文件数据库 (`data/cbond.duckdb`)，包含 **7 张表 + 4 个二级索引**。

### 表结构概览

| 表名 | 主键 | 记录数级别 | 说明 |
|:---|:---|:---|:---|
| `universe` | `code` | ~335 | 转债静态信息（代码/名称/正股/上市日/到期日） |
| `valuation_daily` | `(trade_date, code)` | ~335/天 | 日度估值全字段（价格/溢价率/条款/PE/PB/BS定价等 30+ 列） |
| `vol_daily` | `(trade_date, ucode)` | ~300/天 | 正股 20 日年化波动率 |
| `underlying_profile` | `ucode` | ~300 | 正股公司简介与主营业务 |
| `strategy_picks` | `(trade_date, code, strategy)` | ~70/天 | 策略选券结果 |
| `themes` | `(trade_date, code)` | ~335/天 | 题材分类与业务描述 |
| `etl_runs` | `run_id` | 累积 | ETL 运行日志 |

### 数据双写机制

所有 fetch 脚本在写入 DuckDB 的同时，也写一份 CSV/JSON 到 `data/raw/asof=YYYY-MM-DD/` 目录。这样既有结构化查询的便利（DuckDB），也保留了原始快照的可追溯性（扁平文件）。

---

## 脚本参考

### 数据采集

| 脚本 | 功能 | API 调用 | 耗时 |
|:---|:---|:---|:---|
| `fetch_cb_universe.py` | 拉取全市场可转债列表 + 申万行业 | `data_pool` | ~30s |
| `fetch_valuation.py` | 批量抓取行情/估值/条款/基本面 30+ 字段 | `basic_data` + `realtime` | ~2min |
| `compute_volatility.py` | 正股 20 日年化对数收益率波动率 | `history` | ~1.5min |
| `fetch_underlying_profile.py` | 正股公司简介文本 | `basic_data` | ~1min |
| `refresh_data.py` | 数据新鲜度检测 + 重新拉取过期字段 | `basic_data` | ~2min |

### 计算与评分

| 脚本 | 功能 | 输入 | 输出 |
|:---|:---|:---|:---|
| `assemble_dataset.py` | DuckDB SQL JOIN 组装全字段数据集 | 7 表联查 | `dataset.json` |
| `bs_pricing.py` | BS 期权定价 + 希腊字母 | `dataset.json` | DB 回写 + JSON 回写 |
| `strategy_score.py` | 双低/分域双低/低估策略评分 | `dataset.json` | DB 写入 + `strategies.json` |
| `generate_themes_direct.py` | 关键词规则题材分类 | `dataset.json` | DB 写入 + `themes.json` |

### 报告生成

| 脚本 | 功能 |
|:---|:---|
| `build_overview_md.py` | 读取 DB 中全量数据，生成结构化 Markdown |
| `render_html.py` | Markdown --> Jinja2 模板 --> 单文件 HTML 报告 |
| `render_markdown_parser.py` | Markdown 解析器（将 Markdown 转为结构化 dict） |
| `report_view_model.py` | 仪表盘 view model 构建（dict --> 前端 JSON payload） |

### 基础设施

| 脚本 | 功能 |
|:---|:---|
| `_auth.py` | iFinD Token 生命周期管理（自动刷新 access_token，缓存 6h） |
| `_ifind.py` | iFinD HTTP API 封装（`basic_data` / `realtime` / `history`，含 `batched()` 批量助手） |
| `_db.py` | DuckDB 连接管理、`init_schema()` 初始化、通用 `upsert()` |
| `_etl_log.py` | ETL 运行日志上下文管理器 |
| `init_db.py` | 执行 `schema.sql` 建表 |
| `backfill.py` | 从 raw 目录回填历史数据到 DB |
| `backfill_bs_delta.py` | 批量计算并回填历史 BS Delta 到 DB |
| `validate_data.py` | 数据质量校验（universe 规模、字段完整度、值域范围） |
| `backtest_weekly.py` | 周度再平衡回测引擎（Portfolio 类 + 风险指标） |

---

## 目录结构

```
china-cbond/
  scripts/
    _auth.py, _ifind.py, _db.py     # 基础设施
    fetch_*.py, compute_*.py          # 数据采集
    assemble_dataset.py               # 数据组装
    bs_pricing.py                     # BS 定价
    strategy_score.py                 # 策略评分
    generate_themes_direct.py         # 题材分类
    build_overview_md.py              # Markdown 生成
    render_html.py                    # HTML 渲染
    backtest_weekly.py                # 回测引擎
    static/
      app.js                          # 前端交互逻辑 (SVG 回测/排序/筛选/导出)
      style.css                       # 样式 (暗色+亮色主题)
    templates/
      base.html.j2                    # Jinja2 HTML 模板
    schema.sql                        # DuckDB DDL
  data/                               # (gitignore) 数据库 + 原始快照
    cbond.duckdb
    raw/asof=YYYY-MM-DD/
  output/                             # (gitignore) 生成的 HTML 报告
  tests/                              # 单元测试 (42 tests)
  theme_vocabulary.md                 # 题材标签词表 (~85个)
  requirements.txt                    # Python 依赖
  .github/workflows/                  # GitHub Pages 自动部署
```

---

## 已知限制

| 限制 | 说明 |
|:---|:---|
| iFinD 概念字段 | `ths_concept_*` 全部返回 ERR，无法获取结构化板块数据，题材分类依赖正股简介文本 |
| 退市判定 | 余额为 0 的券视为已退市（强制赎回），从数据集中排除 |
| 波动率样本 | 新上市不足 20 个交易日的券波动率样本不足，`vol_daily.n_samples` 可供判断 |
| SSL 兼容 | Anaconda Python 与 iFinD 存在 SSL 握手问题，建议使用系统 Python |
| BS 模型局限 | 未包含赎回条款和下修条款，偏股型转债理论价值偏高 |
| 回测数据依赖 | 回测需要历史 PE/vol 数据，iFinD 历史 API 部分字段返回 0，已通过正股 `history()` 接口补全 |

---

## 更新日志

### 2026-04-27 (v2)

**回测引擎重写**
- 新增 `Portfolio` 类：逐只持仓追踪，正确的换手感知交易成本模型（修复旧版 exit cost 权重错误）
- 策略函数改为纯函数，消除对 bond dict 的副作用（`dl_score`/`sector` 不再写入输入数据）
- 分域标准从溢价率改为 BS Delta（偏股 >= 0.7 / 平衡 0.4-0.7 / 偏债 < 0.4）
- 新增 universe 过滤：余额 >= 2亿 + 价格 <= 150元 + 最少持仓 5 只
- 新增风险指标：Sharpe ratio、最大回撤、平均换手率
- 基准改为中证转债指数 (000832.CSI)，iFinD 不可用时回退全市场等权
- `main()` 从 968 行单体函数拆分为 8 个独立函数
- 新增 `backfill_bs_delta.py` 历史 Delta 回填脚本

### 2026-04-27

**UI**
- 散点图改为等大圆点（去掉余额气泡大小），修复"未分域"显示问题
- 卡片文字溢出修复（`text-overflow: ellipsis`）
- 卡片新增纯债溢价率字段（5 字段 3 列布局）
- 强赎/下修状态徽章改进（安全/警告/下修图标）

**数据**
- 纯债溢价率 332/332 全部修复
- 低估策略候选池拓宽（不再要求 PE > 0）
- 删除隐含波动率字段（iFinD API 对所有日期返回 None）

**回测**
- 修复历史 PE/波动率返回 0 的问题：PE 改从正股 `history(ucode, 'pe_ttm')` 获取，波动率从价格序列计算
- 交易日推导改为全品种日期并集（243 日 vs 此前 63 日）
