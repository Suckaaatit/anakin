import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from route import assign_route  # noqa: E402


class RouteAssignmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_min = os.getenv("MIN_ROUTE_CONFIDENCE")
        os.environ["MIN_ROUTE_CONFIDENCE"] = "2"

    def tearDown(self) -> None:
        if self.old_min is None:
            os.environ.pop("MIN_ROUTE_CONFIDENCE", None)
        else:
            os.environ["MIN_ROUTE_CONFIDENCE"] = self.old_min

    def test_low_relevance_goes_not_relevant(self) -> None:
        routed = assign_route(
            seniority="Manager",
            relevance_score=22,
            event_role="Speaker",
            persona_archetype="Operator / Scaler",
            industry="other_tech",
            confidence_score=2,
        )
        self.assertEqual(routed["final_route"], "Not Relevant")
        self.assertEqual(routed["outreach_sequence"], "SKIP")
        self.assertEqual(routed["outreach_approved"], "NO")

    def test_high_seniority_high_score_escalates(self) -> None:
        routed = assign_route(
            seniority="C-Suite / Founder",
            relevance_score=91,
            event_role="Speaker",
            persona_archetype="Visionary Founder",
            industry="fintech",
            confidence_score=4,
        )
        self.assertEqual(routed["final_route"], "Senior AE")
        self.assertTrue(str(routed["outreach_sequence"]).startswith("VIP_SEQUENCE_"))
        self.assertEqual(routed["outreach_approved"], "PENDING_REVIEW")

    def test_low_confidence_requires_manual_review(self) -> None:
        routed = assign_route(
            seniority="Director / Head",
            relevance_score=65,
            event_role="Speaker",
            persona_archetype="Operator / Scaler",
            industry="saas_b2b",
            confidence_score=1,
        )
        self.assertEqual(routed["final_route"], "SDR")
        self.assertEqual(routed["outreach_sequence"], "MANUAL_REVIEW_LOW_CONFIDENCE")
        self.assertEqual(routed["outreach_approved"], "NO")
        self.assertIn("Low enrichment confidence", routed["route_reason"])


if __name__ == "__main__":
    unittest.main()

