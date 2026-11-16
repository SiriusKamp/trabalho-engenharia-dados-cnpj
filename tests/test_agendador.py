"""Verifica ordem mensal e recuperação sem precisar iniciar Spark."""

from pathlib import Path
import shutil
import unittest
from unittest.mock import patch
from uuid import uuid4
from zipfile import BadZipFile, ZipFile

from Scripts.Agendador import executar, manutencao, processar_competencia, reativar
from Scripts.Pipeline.BaixarArquivos import versao_remota
from Scripts.Pipeline.Metadados import Metadados


class AgendadorTests(unittest.TestCase):
    def setUp(self):
        self.raiz = Path(__file__).parent / (".teste-" + uuid4().hex)
        self.raiz.mkdir()
        self.addCleanup(shutil.rmtree, self.raiz, True)

    def test_competencia_seguinte_so_comeca_apos_sucesso(self):
        plano = {"competencias": ["2026-10", "2026-11"], "indisponiveis": [], "arquivos": {"2026-10": [], "2026-11": []}}
        processadas = []
        def processar(competencia, *args, **kwargs):
            processadas.append(competencia)
            if competencia == "2026-10":
                raise ValueError("Falha controlada")
        with patch("Scripts.Agendador.catalogar", return_value=plano), patch("Scripts.Agendador.processar_competencia", side_effect=processar):
            resultado = executar(4, self.raiz)
        self.assertEqual(processadas, ["2026-10"])
        self.assertEqual(resultado["status"], "erro")
        self.assertTrue(Metadados(self.raiz).estado()["manutencao_ativa"])
        self.assertFalse(Metadados(self.raiz).estado()["executando"])

    def test_simulacao_nao_cria_manifestos_nem_inicia_spark(self):
        plano = {"competencias": ["2026-11"], "indisponiveis": [], "arquivos": {"2026-11": []}}
        with patch("Scripts.Agendador.catalogar", return_value=plano):
            resultado = executar(4, self.raiz / "inexistente", simular=True)
        self.assertEqual(resultado["competencias"], ["2026-11"])
        self.assertFalse((self.raiz / "inexistente").exists())

    def test_reativacao_persiste_e_manutencao_inativa_nao_chama_fonte(self):
        meta = Metadados(self.raiz)
        meta.estado(manutencao_ativa=False)
        with patch("Scripts.Agendador.catalogar", side_effect=AssertionError("Consulta indevida")):
            self.assertEqual(manutencao(self.raiz)["status"], "inativo")
        reativar(self.raiz)
        self.assertTrue(Metadados(self.raiz).estado()["manutencao_ativa"])

    def test_manutencao_repete_consulta_que_falhou_mesmo_com_catalogo_antigo(self):
        plano = {"competencias": [], "indisponiveis": ["2026-11"], "arquivos": {}}
        for catalogo_antigo in (None, {"competencias": [], "indisponiveis": [], "arquivos": {}}):
            with self.subTest(catalogo_antigo=catalogo_antigo):
                Metadados(self.raiz).estado(catalogo=catalogo_antigo)
                with patch("Scripts.Agendador.catalogar", side_effect=ConnectionError("Fonte indisponível")):
                    self.assertEqual(executar(4, self.raiz)["status"], "erro")
                with patch("Scripts.Agendador.catalogar", return_value=plano) as consulta:
                    resultado = manutencao(self.raiz)
                consulta.assert_called_once_with(4)
                self.assertEqual(resultado["status"], "sucesso")
                self.assertEqual(resultado["indisponiveis"], ["2026-11"])
                self.assertFalse(Metadados(self.raiz).estado()["manutencao_ativa"])

    def test_pendencias_fora_da_nova_janela_terminam_antes_do_mes_novo(self):
        antigo = {"competencias": ["2026-10", "2026-11"], "indisponiveis": [],
                  "arquivos": {"2026-10": [], "2026-11": []}}
        novo = {"competencias": ["2026-12"], "indisponiveis": [], "arquivos": {"2026-12": []}}
        with patch("Scripts.Agendador.catalogar", return_value=antigo), patch(
                "Scripts.Agendador.processar_competencia", side_effect=ValueError("Falha controlada")):
            executar(2, self.raiz)
        with patch("Scripts.Agendador.catalogar", return_value=novo), patch(
                "Scripts.Agendador.processar_competencia") as processar:
            resultado = executar(1, self.raiz)
        self.assertEqual([chamada.args[0] for chamada in processar.call_args_list],
                         ["2026-10", "2026-11", "2026-12"])
        self.assertEqual(resultado["status"], "sucesso")
        self.assertEqual(Metadados(self.raiz).estado()["pendentes"], [])

    def preparar_competencia(self):
        meta = Metadados(self.raiz)
        itens = [{"competencia": "2026-11", "nome": nome, "etag": "versao-1", "bytes": 10}
                 for nome in ("Empresas0.zip", "Estabelecimentos0.zip", "Socios0.zip")]
        for item in itens:
            caminho = f"Prata/{item['nome'][:-4]}"
            (self.raiz / caminho).mkdir(parents=True)
            meta.atualizar("Prata", "2026-11/" + item["nome"], status="sucesso",
                           versao=versao_remota(item), caminho=caminho, publicado_em="2026-11-01")
        (self.raiz / "Ouro/competencia=2026-11").mkdir(parents=True)
        return meta, itens

    def test_ouro_republica_quando_prata_muda_sem_mudar_versao_remota(self):
        meta, itens = self.preparar_competencia()
        with patch("Scripts.Pipeline.PipelinePrata.criar_spark"), patch(
                "Scripts.Pipeline.PipelineOuro.processar_ouro",
                return_value={"caminho": "Ouro/competencia=2026-11"}) as ouro:
            processar_competencia("2026-11", itens, self.raiz, meta, "exec-1", "teste")
            processar_competencia("2026-11", itens, self.raiz, meta, "exec-2", "teste")
            self.assertEqual(ouro.call_count, 1)
            meta.atualizar("Prata", "2026-11/Empresas0.zip", publicado_em="2026-11-02")
            processar_competencia("2026-11", itens, self.raiz, meta, "exec-3", "teste")
            self.assertEqual(ouro.call_count, 2)

    def test_retomada_descarta_bronze_de_origem_ja_publicada(self):
        meta, itens = self.preparar_competencia()
        destino = self.raiz / "Bronze/2026-11/Empresas0"
        destino.mkdir(parents=True)
        (destino / "CADASTRO.DADOS").write_text("csv já processado")
        meta.atualizar("Bronze", "2026-11/Empresas0.zip", status="extraido")
        with patch("Scripts.Pipeline.PipelinePrata.criar_spark"), patch(
                "Scripts.Pipeline.PipelinePrata.processar_arquivo") as prata, patch(
                "Scripts.Pipeline.PipelineOuro.processar_ouro", return_value={"caminho": "Ouro/competencia=2026-11"}):
            processar_competencia("2026-11", itens, self.raiz, meta, "exec-1", "teste")
        prata.assert_not_called()
        self.assertFalse(destino.exists())
        self.assertEqual(meta.ler("Bronze")["arquivos"]["2026-11/Empresas0.zip"]["status"], "descartado")

    def test_crc_invalido_forca_nova_baixa_na_retomada(self):
        meta, itens = self.preparar_competencia()
        chave = "2026-11/Empresas0.zip"
        meta.atualizar("Prata", chave, status="erro")
        zip_path = self.raiz / "Raw" / chave
        zip_path.parent.mkdir(parents=True)
        with ZipFile(zip_path, "w") as arquivo:
            arquivo.writestr("CADASTRO.DADOS", b"CONTEUDO ORIGINAL")
        zip_path.write_bytes(zip_path.read_bytes().replace(b"CONTEUDO ORIGINAL", b"CONTEUDO ALTERADO"))
        meta.atualizar("Raw", chave, status="baixado", versao=versao_remota(itens[0]))
        with self.assertRaises(BadZipFile):
            processar_competencia("2026-11", itens, self.raiz, meta, "exec-1", "teste")

        def baixar(item, destino):
            with ZipFile(destino, "w") as arquivo:
                arquivo.writestr("CADASTRO.DADOS", b"CONTEUDO ORIGINAL")
            return {"bytes": destino.stat().st_size}

        with patch("Scripts.Agendador.baixar_arquivo", side_effect=baixar) as baixa, patch(
                "Scripts.Pipeline.PipelinePrata.criar_spark"), patch(
                "Scripts.Pipeline.PipelinePrata.processar_arquivo", return_value={"caminho": "Prata/Empresas0"}), patch(
                "Scripts.Pipeline.PipelineOuro.processar_ouro", return_value={"caminho": "Ouro/competencia=2026-11"}):
            processar_competencia("2026-11", itens, self.raiz, meta, "exec-2", "teste")
        baixa.assert_called_once()
