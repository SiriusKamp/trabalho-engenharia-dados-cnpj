from pathlib import Path
import shutil
import unittest

from cnpj_lakehouse.pipeline import normalize_text, run_pipeline


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DATA = ROOT / "data" / "sample"


class PipelineTests(unittest.TestCase):
    def test_normalize_text_removes_outer_spaces_and_uppercases(self):
        self.assertEqual(normalize_text("  acme comercio ltda "), "ACME COMERCIO LTDA")


    def test_pipeline_creates_gold_questions_and_is_idempotent(self):
        output_dir = ROOT / ".test-artifacts-pipeline"
        shutil.rmtree(output_dir, ignore_errors=True)
        first = run_pipeline(SAMPLE_DATA, output_dir)
        second = run_pipeline(SAMPLE_DATA, output_dir)

        self.assertEqual(first["gold_rows"], second["gold_rows"])
        self.assertEqual(first["gold_rows"]["top_ufs"][0]["uf"], "SP")
        self.assertEqual(first["gold_rows"]["top_ufs"][0]["total_estabelecimentos"], 2)
        self.assertEqual(first["gold_rows"]["porte"][0]["porte"], "01")
        self.assertTrue((output_dir / "quality.json").exists())
        self.assertTrue((output_dir / "metrics.json").exists())
        self.assertTrue((output_dir / "audit.json").exists())
        shutil.rmtree(output_dir, ignore_errors=True)
