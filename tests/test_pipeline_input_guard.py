import os
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from pipeline import _validate_stage_input  # noqa: E402


class PipelineInputGuardTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: int) -> None:
        with open(path, "w", encoding="utf-8-sig") as handle:
            handle.write("id,name\n")
            for i in range(rows):
                handle.write(f"{i+1},Name {i+1}\n")

    def test_missing_file_fails(self) -> None:
        missing = ROOT / "tests" / "does_not_exist.csv"
        ok, reason = _validate_stage_input(
            stage="persona",
            input_path=missing,
            run_started_epoch=time.time(),
            allow_stale_first_stage=False,
            max_age_minutes=5,
        )
        self.assertFalse(ok)
        self.assertIn("missing required input file", reason)

    def test_empty_csv_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.csv"
            self._write_csv(path, rows=0)
            ok, reason = _validate_stage_input(
                stage="persona",
                input_path=path,
                run_started_epoch=time.time(),
                allow_stale_first_stage=False,
                max_age_minutes=5,
            )
            self.assertFalse(ok)
            self.assertIn("zero rows", reason)

    def test_resume_stale_input_fails_with_age_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.csv"
            self._write_csv(path, rows=3)
            old_epoch = time.time() - (60 * 60)  # 60 minutes old
            os.utime(path, (old_epoch, old_epoch))
            ok, reason = _validate_stage_input(
                stage="persona",
                input_path=path,
                run_started_epoch=time.time(),
                allow_stale_first_stage=True,
                max_age_minutes=10,
                enforce_age_for_first_stage=True,
            )
            self.assertFalse(ok)
            self.assertIn("input appears stale", reason)

    def test_downstream_stale_input_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.csv"
            self._write_csv(path, rows=3)
            run_started = time.time()
            old_epoch = run_started - 30
            os.utime(path, (old_epoch, old_epoch))
            ok, reason = _validate_stage_input(
                stage="route",
                input_path=path,
                run_started_epoch=run_started,
                allow_stale_first_stage=False,
                max_age_minutes=10,
            )
            self.assertFalse(ok)
            self.assertIn("predates this pipeline run", reason)

    def test_fresh_input_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.csv"
            self._write_csv(path, rows=3)
            run_started = time.time() - 2
            now = time.time()
            os.utime(path, (now, now))
            ok, reason = _validate_stage_input(
                stage="route",
                input_path=path,
                run_started_epoch=run_started,
                allow_stale_first_stage=False,
                max_age_minutes=10,
            )
            self.assertTrue(ok)
            self.assertIn("input validated", reason)


if __name__ == "__main__":
    unittest.main()

