from pathlib import Path
import shutil
import unittest

from cnpj_lakehouse.security import mask_document
from cnpj_lakehouse.pipeline import run_pipeline


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DATA = ROOT / "data" / "sample"


class SecurityTests(unittest.TestCase):
    def test_mask_document_preserves_only_a_safe_suffix(self):
        self.assertEqual(mask_document("12345678901"), "*******8901")
        self.assertEqual(mask_document(""), "")


    def test_gold_does_not_contain_unmasked_partner_documents(self):
        output_dir = ROOT / ".test-artifacts-security"
        shutil.rmtree(output_dir, ignore_errors=True)
        result = run_pipeline(SAMPLE_DATA, output_dir)
        self.assertNotIn("12345678901", str(result["gold_rows"]))
        shutil.rmtree(output_dir, ignore_errors=True)
