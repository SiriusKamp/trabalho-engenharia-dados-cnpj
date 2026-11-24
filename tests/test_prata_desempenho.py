"""Contagem da entrada durante a transformação, sem um cache extra do CSV."""

import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest

from test_spark import CAMPOS, estabelecimento


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "PySpark não instalado")
class TestAuditoriaPrata(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from Scripts.Pipeline.PipelinePrata import criar_spark

        cls.spark = criar_spark()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def processar(self, entidade, lotes):
        from Scripts.Pipeline.PipelinePrata import processar_arquivo

        temporario = self.enterContext(tempfile.TemporaryDirectory())
        raiz = Path(temporario) / "DATA"
        arquivos = []
        for indice, linhas in enumerate(lotes):
            arquivo = Path(temporario) / f"bronze-{indice}.csv"
            with arquivo.open("w", encoding="iso8859-1", newline="") as destino:
                escritor = csv.writer(destino, delimiter=";", quoting=csv.QUOTE_ALL)
                escritor.writerows([[linha.get(c, "") for c in CAMPOS[entidade]] for linha in linhas])
            arquivos.append(arquivo)
        resultado = processar_arquivo(
            self.spark, arquivos, entidade, "2026-10", f"{entidade}0.zip", raiz, "teste", "teste")
        return resultado, raiz

    def test_conta_entrada_de_varios_csvs_com_multilinha_e_duplicidades(self):
        from Scripts.Pipeline.PipelinePrata import ler_tabela

        linha = estabelecimento(complemento="ANDAR 16\nTORRE 2")
        resultado, raiz = self.processar("estabelecimentos", [
            [linha, linha, estabelecimento("00000002", nome_fantasia="")],
            [estabelecimento("00000002", nome_fantasia="Loja"), estabelecimento("00000003")],
        ])
        self.assertEqual(resultado.get("linhas_lidas"), 5)
        self.assertEqual(resultado["linhas"], 3)
        self.assertEqual(resultado["duplicidades"], 2)
        linhas = {r.cnpj_basico: r for r in ler_tabela(self.spark, raiz, "estabelecimentos", "2026-10").collect()}
        self.assertEqual(linhas["00000001"].complemento, "ANDAR 16\nTORRE 2")
        self.assertEqual(linhas["00000002"].nome_fantasia, "Loja")

    def test_entrada_vazia_publica_snapshot_vazio_e_contagem_zero(self):
        resultado, _ = self.processar("empresas", [[]])
        self.assertEqual(resultado.get("linhas_lidas"), 0)
        self.assertEqual(resultado["linhas"], 0)
        self.assertEqual(resultado["duplicidades"], 0)


if __name__ == "__main__":
    unittest.main()
