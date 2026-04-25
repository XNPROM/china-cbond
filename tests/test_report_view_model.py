import pathlib
import sys
import unittest

from jinja2 import Environment, FileSystemLoader


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from report_view_model import build_dashboard_view_model


REPORT_FIXTURE = {
    "title": "可转债概览 · 2026-04-24",
    "summary": ["总数 2 只", "低估 1 只"],
    "appendix": ["相对价值 = 市价 / BS理论价值"],
    "strategy_picks": [
        {
            "strategy": "双低",
            "desc": "经典双低",
            "cells": ["1", "测试转债 (123001.SZ)", "测试股份", "120.00", "8.20%", "18.0", "35.0%", "11.0"],
        }
    ],
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
                },
                {
                    "bond_name": "稳健转债",
                    "bond_code": "113001.SH",
                    "stock": "稳健股份 (600001.SH)",
                    "industry": "建筑装饰",
                    "price": "98.00",
                    "day_chg": "-0.35%",
                    "conv": "58.20%",
                    "pure": "3.10%",
                    "vol": "21.5%",
                    "pure_bond_ytm": "2.6%",
                    "relative_value": "1.18",
                    "delta": "0.22",
                    "balance": "12.00",
                    "rating": "AA+",
                    "maturity": "2029-06-01",
                    "call_status": "不强赎至2026-05-20",
                    "down_status": "",
                    "strategy": "",
                    "business": "主营稳健业务。",
                    "themes": ["建筑装饰-设计施工"],
                    "sparkline": {"delta": [0.18, 0.22], "rv": [1.11, 1.18]},
                },
            ],
        }
    ],
}


BACKTEST_FIXTURE = {
    "start_date": "2026-01-23",
    "end_date": "2026-04-24",
    "trading_days": 60,
    "n_rebalances": 12,
    "cum_return_dl_pct": 12.4,
    "annualized_dl_pct": 38.1,
    "cum_return_sn_pct": 10.2,
    "annualized_sn_pct": 31.6,
    "cum_return_mkt_pct": 4.5,
    "annualized_mkt_pct": 13.8,
    "equity_curve": [
        {"date": "2026-04-23", "cum_dl": 0.102, "cum_sn": 0.081, "cum_mkt": 0.032},
        {"date": "2026-04-24", "cum_dl": 0.124, "cum_sn": 0.102, "cum_mkt": 0.045},
    ],
}


class ReportViewModelTests(unittest.TestCase):
    def test_builds_dashboard_payload_for_template_and_frontend(self):
        view_model = build_dashboard_view_model(REPORT_FIXTURE, "2026-04-24", BACKTEST_FIXTURE)

        self.assertEqual(view_model["hero"]["title"], "可转债概览 · 2026-04-24")
        self.assertEqual(view_model["hero"]["trade_date"], "2026-04-24")
        self.assertEqual(view_model["kpis"]["undervalued"], 1)
        self.assertEqual(view_model["kpis"]["total"], 2)
        self.assertEqual(view_model["highlights"][0]["bond_code"], "123001.SZ")
        self.assertIn("AI应用", view_model["explorer"]["theme_options"])
        self.assertEqual(view_model["explorer"]["items"][0]["detail"]["themes"], ["AI应用", "信创-国产替代"])
        self.assertTrue(view_model["market_radar"]["scatter_points"])
        self.assertEqual(view_model["strategy_panels"][0]["name"], "双低")
        self.assertEqual(view_model["backtest"]["summary"]["start_date"], "2026-01-23")

    def test_prefers_summary_level_kpi_counts_when_display_values_are_rounded(self):
        report = {
            **REPORT_FIXTURE,
            "summary": [
                "总数 2 只；1 只价格 >130，0 只价格 <90",
                "相对价值中位数 1.01；2 只低估（<1.0），0 只合理或高估",
                "分域分布：偏股1只 / 平衡1只 / 偏债0只",
            ],
            "sections": [
                {
                    "theme": "AI应用",
                    "cards": [
                        {
                            **REPORT_FIXTURE["sections"][0]["cards"][0],
                            "relative_value": "1.00",
                            "conv": "8.20%",
                        },
                        {
                            **REPORT_FIXTURE["sections"][0]["cards"][1],
                            "relative_value": "1.02",
                            "conv": "21.00%",
                        },
                    ],
                }
            ],
        }

        view_model = build_dashboard_view_model(report, "2026-04-24", None)

        self.assertEqual(view_model["kpis"]["undervalued"], 2)
        self.assertEqual(view_model["kpis"]["n_equity"], 1)
        self.assertEqual(view_model["kpis"]["n_balanced"], 1)
        self.assertEqual(view_model["kpis"]["n_debt"], 0)

    def test_new_template_shell_has_dashboard_landmarks(self):
        view_model = build_dashboard_view_model(REPORT_FIXTURE, "2026-04-24", BACKTEST_FIXTURE)
        env = Environment(loader=FileSystemLoader(str(ROOT / "scripts" / "templates")))
        template = env.get_template("base.html.j2")

        html = template.render(
            title="可转债概览 · 2026-04-24",
            view_model=view_model,
            view_model_json="{}",
            css="body{}",
            js="console.log('test')",
        )

        self.assertIn('market-radar', html)
        self.assertIn('explorer-workbench', html)
        self.assertIn('id="detailDrawer"', html)


if __name__ == "__main__":
    unittest.main()
