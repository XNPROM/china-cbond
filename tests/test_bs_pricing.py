"""Unit tests for bs_pricing.py — Black-Scholes pricing model.

Tests cover:
- BS call option pricing correctness
- Greek letters (delta, gamma, theta, vega) computation
- Edge cases: zero/negative inputs, missing data
- Integration: pricing results match expected ranges
"""
import pathlib
import sys
import math
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from bs_pricing import bs_call, _norm_cdf, _norm_pdf


class NormCdfTests(unittest.TestCase):
    def test_cdf_at_zero(self):
        self.assertAlmostEqual(_norm_cdf(0), 0.5, places=10)

    def test_cdf_at_one(self):
        # N(1) ≈ 0.8413
        self.assertAlmostEqual(_norm_cdf(1), 0.8413, places=3)

    def test_cdf_at_negative(self):
        # N(-1) ≈ 0.1587
        self.assertAlmostEqual(_norm_cdf(-1), 0.1587, places=3)

    def test_cdf_at_large_positive(self):
        self.assertAlmostEqual(_norm_cdf(5), 1.0, places=5)

    def test_cdf_at_large_negative(self):
        self.assertAlmostEqual(_norm_cdf(-5), 0.0, places=5)


class NormPdfTests(unittest.TestCase):
    def test_pdf_at_zero(self):
        # phi(0) = 1/sqrt(2*pi) ≈ 0.3989
        self.assertAlmostEqual(_norm_pdf(0), 0.3989, places=3)

    def test_pdf_at_one(self):
        # phi(1) ≈ 0.2420
        self.assertAlmostEqual(_norm_pdf(1), 0.2420, places=3)

    def test_pdf_is_symmetric(self):
        self.assertAlmostEqual(_norm_pdf(1), _norm_pdf(-1), places=10)


class BSCallTests(unittest.TestCase):
    def test_typical_cbond_pricing(self):
        """Test with typical convertible bond parameters."""
        S = 100.0      # conversion value
        K = 110.0      # strike (maturity call price)
        sigma = 0.35   # 35% annualized vol
        r = 0.025      # 2.5% risk-free rate
        T = 2.0        # 2 years to maturity

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        # Call option should have positive value
        self.assertGreater(call, 0)

        # Delta should be between 0 and 1
        self.assertGreater(delta, 0)
        self.assertLess(delta, 1)

        # Gamma should be positive
        self.assertGreater(gamma, 0)

        # Theta should be negative (time decay)
        self.assertLess(theta, 0)

        # Vega should be positive
        self.assertGreater(vega, 0)

    def test_deep_in_the_money(self):
        """When S >> K, delta ≈ 1, call ≈ S - K*exp(-rT)."""
        S = 200.0
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = 2.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        # Delta should be very close to 1
        self.assertGreater(delta, 0.95)

        # Call should be approximately S - K*exp(-rT)
        intrinsic = S - K * math.exp(-r * T)
        self.assertAlmostEqual(call, intrinsic, delta=5)

    def test_deep_out_of_the_money(self):
        """When S << K, delta ≈ 0, call ≈ 0."""
        S = 50.0
        K = 150.0
        sigma = 0.30
        r = 0.025
        T = 2.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        # Delta should be very close to 0
        self.assertLess(delta, 0.05)

        # Call value should be very small
        self.assertLess(call, 5)

    def test_at_the_money(self):
        """When S ≈ K, delta ≈ 0.5."""
        S = 100.0
        K = 100.0
        sigma = 0.30
        r = 0.0
        T = 1.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        # Delta should be approximately 0.5
        self.assertAlmostEqual(delta, 0.5, delta=0.1)

    def test_zero_volatility(self):
        """When sigma = 0, should return intrinsic value."""
        S = 120.0
        K = 100.0
        sigma = 0.0
        r = 0.025
        T = 2.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        self.assertEqual(call, 20.0)  # S - K
        self.assertEqual(delta, 0.0)
        self.assertEqual(gamma, 0.0)
        self.assertEqual(theta, 0.0)
        self.assertEqual(vega, 0.0)

    def test_zero_time_to_maturity(self):
        """When T = 0, should return intrinsic value."""
        S = 120.0
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = 0.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        self.assertEqual(call, 20.0)

    def test_very_short_maturity(self):
        """When T is very small (< 0.01), should use intrinsic."""
        S = 120.0
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = 0.005

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        self.assertEqual(call, 20.0)

    def test_negative_time(self):
        """Negative time should use intrinsic value."""
        S = 120.0
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = -1.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        self.assertEqual(call, 20.0)

    def test_zero_stock_price(self):
        """S = 0 should return 0 call value."""
        S = 0.0
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = 2.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        self.assertEqual(call, 0.0)

    def test_zero_strike(self):
        """K = 0 should use fallback and handle gracefully."""
        S = 100.0
        K = 0.0
        sigma = 0.30
        r = 0.025
        T = 2.0

        call, delta, gamma, theta, vega = bs_call(S, K, sigma, r, T)

        # Should return intrinsic when K = 0
        self.assertEqual(call, 100.0)

    def test_vega_per_1pct(self):
        """Vega should be scaled per 1% vol change."""
        S = 100.0
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = 2.0

        _, _, _, _, vega = bs_call(S, K, sigma, r, T)

        # Vega should be positive and reasonable
        self.assertGreater(vega, 0)
        # For ATM option, vega ≈ S * sqrt(T) * phi(d1) / 100
        # Should be in range [0.1, 5.0] for typical parameters
        self.assertLess(vega, 5.0)

    def test_gamma_values(self):
        """Gamma should be highest for ATM options."""
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = 2.0

        _, _, gamma_atm, _, _ = bs_call(100.0, K, sigma, r, T)
        _, _, gamma_itm, _, _ = bs_call(150.0, K, sigma, r, T)
        _, _, gamma_otm, _, _ = bs_call(50.0, K, sigma, r, T)

        # ATM gamma should be highest
        self.assertGreater(gamma_atm, gamma_itm)
        self.assertGreater(gamma_atm, gamma_otm)

    def test_theta_decay(self):
        """Theta should be negative (time decay reduces option value)."""
        S = 100.0
        K = 100.0
        sigma = 0.30
        r = 0.025
        T = 1.0

        _, _, _, theta, _ = bs_call(S, K, sigma, r, T)

        self.assertLess(theta, 0)


if __name__ == "__main__":
    unittest.main()
