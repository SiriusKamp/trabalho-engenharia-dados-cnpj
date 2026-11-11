import unittest

from cnpj_lakehouse.quality import validate_silver


class QualityTests(unittest.TestCase):
    def test_validates_required_keys_and_masked_documents(self):
        report = validate_silver(
            [
                {"cnpj_basico": "10000001", "uf": "SP"},
                {"cnpj_basico": "10000001", "cnpj_cpf_socio": "*******8901"},
            ]
        )
        self.assertTrue(report["valid"])
        self.assertEqual(report["failed"], [])

    def test_rejects_empty_keys_invalid_uf_and_raw_document(self):
        report = validate_silver(
            [
                {"cnpj_basico": "", "uf": "Sao Paulo"},
                {"cnpj_basico": "10000001", "cnpj_cpf_socio": "12345678901"},
            ]
        )
        self.assertFalse(report["valid"])
        self.assertIn("cnpj_basico_non_empty", report["failed"])
        self.assertIn("uf_two_chars", report["failed"])
        self.assertIn("documents_masked", report["failed"])
