import sys
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from web_app import app  # noqa: E402


class QualityApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = app.test_client()

    def test_quality_payload_contains_fast_mode_linkedin_kpi(self) -> None:
        response = self.client.get("/api/quality")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIsInstance(payload, dict)
        kpis = payload.get("kpis", {})
        self.assertIn("linkedin_not_attempted_pct", kpis)
        self.assertIn("linkedin_match_coverage_pct", kpis)
        self.assertIn("email_deliverability_rate_pct", kpis)
        self.assertIn("linkedin_acceptance_rate_pct", kpis)
        self.assertIn("send_readiness_score_pct", kpis)
        self.assertIn("spam_risk_score_pct", kpis)
        self.assertIsInstance(kpis["linkedin_not_attempted_pct"], (int, float))
        self.assertGreaterEqual(kpis["linkedin_not_attempted_pct"], 0.0)
        self.assertLessEqual(kpis["linkedin_not_attempted_pct"], 100.0)
        for key in ["linkedin_match_coverage_pct", "send_readiness_score_pct", "spam_risk_score_pct"]:
            self.assertIsInstance(kpis[key], (int, float))
            self.assertGreaterEqual(kpis[key], 0.0)
            self.assertLessEqual(kpis[key], 100.0)

        for key in ["email_deliverability_rate_pct", "linkedin_acceptance_rate_pct"]:
            value = kpis[key]
            if value is not None:
                self.assertIsInstance(value, (int, float))
                self.assertGreaterEqual(value, 0.0)
                self.assertLessEqual(value, 100.0)


if __name__ == "__main__":
    unittest.main()
