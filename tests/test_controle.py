"""Protege seleção mensal, gravação concorrente e recuperação de publicação."""

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
import shutil
import unittest
from uuid import uuid4

from Scripts.Pipeline.BaixarArquivos import meses_calendario, selecionar_competencias
from Scripts.Pipeline.Metadados import Metadados, publicar_diretorio


class ControleTests(unittest.TestCase):
    def setUp(self):
        self.raiz = Path(__file__).parent / (".teste-" + uuid4().hex)
        self.raiz.mkdir()
        self.addCleanup(shutil.rmtree, self.raiz, True)

    def test_janela_inclui_mes_atual_e_atravessa_ano(self):
        self.assertEqual(meses_calendario(4, date(2026, 1, 5)), ["2025-10", "2025-11", "2025-12", "2026-01"])
        self.assertEqual(len(meses_calendario(36, date(2026, 11, 16))), 36)
        with self.assertRaises(ValueError):
            meses_calendario(0)

    def test_mes_indisponivel_nao_e_substituido_por_outro_mais_antigo(self):
        plano = selecionar_competencias(4, ["2026-07", "2026-08", "2026-09", "2026-10"], date(2026, 11, 16))
        self.assertEqual(plano["competencias"], ["2026-08", "2026-09", "2026-10"])
        self.assertEqual(plano["indisponiveis"], ["2026-11"])

    def test_atualizacoes_concorrentes_nao_perdem_arquivos(self):
        meta = Metadados(self.raiz)
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda i: meta.atualizar("Raw", f"2026-11/Empresas{i}.zip", status="baixado"), range(12)))
        self.assertEqual(len(meta.ler("Raw")["arquivos"]), 12)
        self.assertFalse((self.raiz / "Raw" / "Metadados.json.tmp").exists())

    def test_publicacao_recupera_versao_anterior_apos_interrupcao(self):
        destino = self.raiz / "publicado"
        backup = self.raiz / "publicado.anterior"
        backup.mkdir()
        (backup / "dado.txt").write_text("anterior", encoding="utf-8")
        staging = self.raiz / "publicado.novo"
        staging.mkdir()
        (staging / "dado.txt").write_text("novo", encoding="utf-8")
        publicar_diretorio(staging, destino)
        self.assertEqual((destino / "dado.txt").read_text(encoding="utf-8"), "novo")
        self.assertFalse(backup.exists())
