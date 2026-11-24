"""Contrato do dashboard com o consumidor URL do Infinity, sem rede/Grafana."""

import json
from pathlib import Path
import shutil
import subprocess
import unittest


DASHBOARD = Path(__file__).resolve().parents[1] / "Grafana" / "dashboard.json"

# Reproduz apenas a leitura que decide a migração de POST, antes do HTTP:
# https://github.com/grafana/grafana-infinity-datasource/blob/main/src/migrate.ts#L17
# O parser backend já está definido nestes targets; não há default que repare
# url_options antes desta leitura. Não é uma execução do plugin completo.
CONSUMIDOR_URL = r"""
const fs = require('node:fs');
const query = JSON.parse(fs.readFileSync(0, 'utf8'));
const newQuery = { ...query };
const needsPostMigration = newQuery.source === 'url' && newQuery.url_options.method === 'POST';
process.stdout.write(JSON.stringify({method: newQuery.url_options.method, needsPostMigration}));
"""


class GrafanaTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Consumidor JavaScript do Infinity requer Node; execute este teste no host")
    def test_targets_url_sao_consumidos_como_get_sem_falha_na_migracao(self):
        dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        targets = [
            (panel["id"], target)
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
            if target.get("source") == "url"
        ]
        self.assertIn(16, [panel_id for panel_id, _ in targets])
        for panel_id, target in targets:
            with self.subTest(panel=panel_id, refId=target["refId"]):
                result = subprocess.run(
                    ["node", "-e", CONSUMIDOR_URL],
                    input=json.dumps(target),
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    json.loads(result.stdout),
                    {"method": "GET", "needsPostMigration": False},
                )


if __name__ == "__main__":
    unittest.main()
