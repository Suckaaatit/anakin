import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from persona import generate_personas  # noqa: E402


class PersonaGatingTests(unittest.TestCase):
    def test_skips_rows_when_enrichment_not_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            input_path = Path(tmp) / "enriched.csv"
            output_path = Path(tmp) / "personas.csv"
            df = pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "Alice Example",
                        "title": "Founder",
                        "company": "ExampleCo",
                        "enrichment_status": "ENRICHED",
                        "enrichment_confidence_score": 4,
                        "seniority": "C-Suite / Founder",
                        "industry": "fintech",
                        "session_topic": "Pricing intelligence",
                    },
                    {
                        "id": 2,
                        "name": "Bob Example",
                        "title": "CTO",
                        "company": "ExampleTwo",
                        "enrichment_status": "ERROR",
                        "enrichment_confidence_score": 5,
                        "seniority": "C-Suite / Founder",
                        "industry": "saas_b2b",
                        "session_topic": "Data automation",
                    },
                ]
            )
            df.to_csv(input_path, index=False, encoding="utf-8-sig")

            result_df = generate_personas(
                input_csv=str(input_path),
                output_csv=str(output_path),
                fast_mode=True,
            )

            self.assertEqual(len(result_df), 2)
            by_id = {str(row["id"]): row for _, row in result_df.iterrows()}
            self.assertEqual(by_id["1"]["persona_status"], "GENERATED")
            self.assertEqual(by_id["2"]["persona_status"], "SKIPPED_NOT_ENRICHED")
            self.assertEqual(by_id["2"]["llm_error"], "UPSTREAM_ENRICHMENT_NOT_COMPLETE")


if __name__ == "__main__":
    unittest.main()
