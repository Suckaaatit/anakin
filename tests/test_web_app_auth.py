import importlib
import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))


class WebAppAuthTests(unittest.TestCase):
    def test_control_endpoints_require_token_when_configured(self) -> None:
        old_token = os.environ.get("WEB_APP_TOKEN")
        os.environ["WEB_APP_TOKEN"] = "demo123"
        try:
            import web_app  # noqa: E402

            module = importlib.reload(web_app)
            client = module.app.test_client()

            unauth = client.post("/api/queue/action", json={"id": "999999", "decision": "approve"})
            self.assertEqual(unauth.status_code, 401)

            auth = client.post(
                "/api/queue/action",
                json={"id": "999999", "decision": "approve"},
                headers={"X-API-Token": "demo123"},
            )
            # Auth passes, then business validation returns not-found for unknown id.
            self.assertEqual(auth.status_code, 404)
        finally:
            if old_token is None:
                os.environ.pop("WEB_APP_TOKEN", None)
            else:
                os.environ["WEB_APP_TOKEN"] = old_token


if __name__ == "__main__":
    unittest.main()

