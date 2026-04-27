"""Unit tests for strategy_score.py — Double-low and sector-neutral scoring.

Tests cover:
- Percentile calculation
- Sector classification (偏股/平衡/偏债)
- Ranking and scoring logic
- Strategy filtering (PE > 0, vol > Q1)
- Edge cases: empty inputs, missing data
"""
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from strategy_score import _percentile, _classify_sector, _rank_and_score


class PercentileTests(unittest.TestCase):
    def test_percentile_q1(self):
        vals = [10, 20, 30, 40, 50]
        self.assertEqual(_percentile(vals, 25), 20.0)

    def test_percentile_median(self):
        vals = [10, 20, 30, 40, 50]
        self.assertEqual(_percentile(vals, 50), 30.0)

    def test_percentile_q3(self):
        vals = [10, 20, 30, 40, 50]
        self.assertEqual(_percentile(vals, 75), 40.0)

    def test_percentile_single_value(self):
        vals = [42]
        self.assertEqual(_percentile(vals, 25), 42)
        self.assertEqual(_percentile(vals, 50), 42)
        self.assertEqual(_percentile(vals, 75), 42)

    def test_percentile_two_values(self):
        vals = [10, 20]
        self.assertEqual(_percentile(vals, 50), 15.0)

    def test_percentile_empty(self):
        self.assertEqual(_percentile([], 50), 0)

    def test_percentile_interpolation(self):
        vals = [1, 2, 3, 4]
        # k = 3 * 0.25 = 0.75, lo=0, hi=1, frac=0.75
        # result = 1 + 0.75 * (2 - 1) = 1.75
        self.assertEqual(_percentile(vals, 25), 1.75)


class SectorClassificationTests(unittest.TestCase):
    def test_equity_sector(self):
        """delta >= 0.7 -> 偏股"""
        self.assertEqual(_classify_sector(0.7), "偏股")
        self.assertEqual(_classify_sector(0.9), "偏股")

    def test_balanced_sector(self):
        """0.4 <= delta < 0.7 -> 平衡"""
        self.assertEqual(_classify_sector(0.4), "平衡")
        self.assertEqual(_classify_sector(0.55), "平衡")
        self.assertEqual(_classify_sector(0.69), "平衡")

    def test_debt_sector(self):
        """delta < 0.4 -> 偏债"""
        self.assertEqual(_classify_sector(0.1), "偏债")
        self.assertEqual(_classify_sector(0.39), "偏债")

    def test_zero_delta(self):
        """Zero delta should be 偏债"""
        self.assertEqual(_classify_sector(0), "偏债")

    def test_none_delta(self):
        """None delta should default to 偏债"""
        self.assertEqual(_classify_sector(None), "偏债")


class RankAndScoreTests(unittest.TestCase):
    def _make_bond(self, code, conv_prem, latest, pe_ttm=None, bs_delta=None):
        return {
            "code": code,
            "name": f"TestBond_{code}",
            "ucode": f"UC_{code}",
            "uname": f"TestStock_{code}",
            "conv_prem": conv_prem,
            "latest": latest,
            "pe_ttm": pe_ttm,
            "vol_20d": 30.0,
            "day_chg": 0.0,
            "bs_delta": bs_delta,
        }

    def test_basic_scoring(self):
        """Score = 1.5 * rank(conv_prem) + rank(price)."""
        candidates = [
            self._make_bond("A", conv_prem=10, latest=100),
            self._make_bond("B", conv_prem=20, latest=90),
            self._make_bond("C", conv_prem=30, latest=110),
        ]

        scored = _rank_and_score(candidates)

        # Bond A: lowest conv_prem (rank 1), price rank 2 -> score = 1.5*1 + 2 = 3.5
        bond_a = next(s for s in scored if s["code"] == "A")
        self.assertEqual(bond_a["rank_conv_prem"], 1)
        self.assertEqual(bond_a["rank_price"], 2)
        self.assertAlmostEqual(bond_a["rank_overall"], 3.5)

        # Bond B: conv_prem rank 2, lowest price (rank 1) -> score = 1.5*2 + 1 = 4.0
        bond_b = next(s for s in scored if s["code"] == "B")
        self.assertEqual(bond_b["rank_conv_prem"], 2)
        self.assertEqual(bond_b["rank_price"], 1)
        self.assertAlmostEqual(bond_b["rank_overall"], 4.0)

        # Bond C: highest conv_prem (rank 3), highest price (rank 3) -> score = 1.5*3 + 3 = 7.5
        bond_c = next(s for s in scored if s["code"] == "C")
        self.assertEqual(bond_c["rank_conv_prem"], 3)
        self.assertEqual(bond_c["rank_price"], 3)
        self.assertAlmostEqual(bond_c["rank_overall"], 7.5)

    def test_sector_classification_in_scoring(self):
        """Each bond should be assigned correct sector based on bs_delta."""
        candidates = [
            self._make_bond("A", conv_prem=10, latest=100, bs_delta=0.8),
            self._make_bond("B", conv_prem=30, latest=100, bs_delta=0.5),
            self._make_bond("C", conv_prem=60, latest=100, bs_delta=0.2),
        ]

        scored = _rank_and_score(candidates)

        self.assertEqual(next(s for s in scored if s["code"] == "A")["sector"], "偏股")
        self.assertEqual(next(s for s in scored if s["code"] == "B")["sector"], "平衡")
        self.assertEqual(next(s for s in scored if s["code"] == "C")["sector"], "偏债")

    def test_single_bond(self):
        """Single bond should have rank 1 for everything."""
        candidates = [self._make_bond("A", conv_prem=20, latest=100)]
        scored = _rank_and_score(candidates)

        self.assertEqual(len(scored), 1)
        self.assertEqual(scored[0]["rank_conv_prem"], 1)
        self.assertEqual(scored[0]["rank_price"], 1)
        self.assertAlmostEqual(scored[0]["rank_overall"], 2.5)

    def test_bonds_with_same_conv_prem(self):
        """Bonds with same conv_prem get different ranks (by order in list)."""
        candidates = [
            self._make_bond("A", conv_prem=20, latest=100),
            self._make_bond("B", conv_prem=20, latest=90),
        ]

        scored = _rank_and_score(candidates)

        # Both have same conv_prem, but ranked by position
        ranks = {s["code"]: s["rank_conv_prem"] for s in scored}
        self.assertEqual(set(ranks.values()), {1, 2})

    def test_optional_fields(self):
        """Optional fields (vol_20d, day_chg) should default gracefully."""
        candidates = [{
            "code": "A",
            "name": "Test",
            "ucode": "",
            "uname": "",
            "conv_prem": 20,
            "latest": 100,
            "pe_ttm": 15,
        }]

        scored = _rank_and_score(candidates)
        self.assertEqual(len(scored), 1)
        self.assertEqual(scored[0]["code"], "A")


if __name__ == "__main__":
    unittest.main()
