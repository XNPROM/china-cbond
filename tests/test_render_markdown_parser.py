import pathlib
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from render_markdown_parser import parse_markdown


SAMPLE_MARKDOWN = """# 可转债概览 · 2026-04-24

## 摘要
- 总数 1 只

## 策略推荐
### 双低
*经典双低*
| 排名 | 转债 | 正股 | 价格 | 转股溢价率 | PE | 20日σ | 综合得分 |
|---|---|---|---|---|---|---|---|
| 1 | 东南转债 (127103.SZ) | 东南网架 | 156.76 | 19.42% | 105.09 | 55.29% | 1.0 |

## 建筑装饰-设计施工
### 东南转债 (127103.SZ)
| 正股 | 申万一级 | 申万二级 | 价格 | 涨跌幅 | 转股溢价率 | 纯债溢价率 | 20日年化σ | 纯债YTM | 余额(亿) | 评级 | 到期 | 相对价值 | Delta | 强赎 | 下修 | 策略分 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 东南网架 (002135.SZ) | 金属制品业 | 专业工程 | 156.76 | -0.75% | 19.42% |  | 55.29% | -8.82% | 20.00 | AA | 2030-01-03 | 0.93 | 0.79 | 已触发13天 | 触发≤85% | 双低#1 |
**主营**：东南网架主营包括钢结构、化纤、新能源三大业务板块。
**时序**：dates=2026-04-23,2026-04-24 delta=0.658,0.789 rv=1.028,0.928
**题材**：`#建筑装饰-设计施工` `#电力-新能源运营`
"""


class ParseMarkdownTests(unittest.TestCase):
    def test_parses_industry_from_shenwan_header(self):
        report = parse_markdown(SAMPLE_MARKDOWN)

        self.assertEqual(report["title"], "可转债概览 · 2026-04-24")
        self.assertEqual(report["summary"], ["总数 1 只"])
        self.assertEqual(report["strategy_picks"][0]["strategy"], "双低")

        card = report["sections"][0]["cards"][0]
        self.assertEqual(card["bond_code"], "127103.SZ")
        self.assertEqual(card["industry"], "金属制品业")
        self.assertEqual(card["themes"], ["建筑装饰-设计施工", "电力-新能源运营"])
        self.assertEqual(card["sparkline"]["delta"], [0.658, 0.789])


if __name__ == "__main__":
    unittest.main()
