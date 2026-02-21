import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from web_app import _compute_explainability  # noqa: E402


class ExplainabilityTests(unittest.TestCase):
    def test_breakdown_fields_and_score(self) -> None:
        row = {
            "relevance_score": 80,
            "enrichment_confidence_score": 4,
            "seniority": "C-Suite / Founder",
            "icp_match": True,
            "account_priority_score": 94,
            "segment_cluster": "Fintech Founder",
            "final_route": "Senior AE",
            "outreach_sequence": "VIP_SEQUENCE_POST_EVENT",
            "route_reason": "test reason",
        }
        payload = _compute_explainability(row)
        self.assertEqual(payload["priority_score"], 94.0)
        self.assertEqual(len(payload["breakdown"]), 5)
        labels = [item["label"] for item in payload["breakdown"]]
        self.assertTrue(any("Seniority" in label for label in labels))
        self.assertTrue(any("Evidence Score" in label for label in labels))
        self.assertEqual(payload["final_route"], "Senior AE")
        self.assertEqual(payload["sequence"], "VIP_SEQUENCE_POST_EVENT")


if __name__ == "__main__":
    unittest.main()
