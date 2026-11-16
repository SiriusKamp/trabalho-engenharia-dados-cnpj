"""Contrato HTTP com dados temporários, sem rede externa nem sessão Spark."""

import importlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch


ENGENHARIA = ("engenharia", "Engenharia123!")
ANALISE = ("analise", "Analise123!")
MONITOR = ("monitor", "Monitor123!")
AMBIENTE = {
    "ENGENHARIA_USUARIO": ENGENHARIA[0], "ENGENHARIA_SENHA": ENGENHARIA[1],
    "ANALISE_USUARIO": ANALISE[0], "ANALISE_SENHA": ANALISE[1],
    "MONITORAMENTO_USUARIO": MONITOR[0], "MONITORAMENTO_SENHA": MONITOR[1],
    "AGENDAMENTO_ATIVO": "0",
}


class TestAPI(unittest.TestCase):
    def setUp(self):
        # Antes da implementação, falha pela funcionalidade ausente (RED).
        self.assertIsNotNone(importlib.util.find_spec("Scripts.API"),
                             "Scripts.API.criar_app ainda não foi implementada")
        from fastapi.testclient import TestClient
        from Scripts.API import criar_app

        self.ambiente = patch.dict(os.environ, AMBIENTE)
        self.ambiente.start()
        self.addCleanup(self.ambiente.stop)
        self.temporario = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporario.cleanup)
        self.raiz = Path(self.temporario.name) / "DATA"
        for camada in ("Raw", "Bronze", "Prata", "Ouro"):
            (self.raiz / camada).mkdir(parents=True)
            self.gravar(camada, {"arquivos": {}, "execucoes": []})
        self.app = criar_app(self.raiz)
        self.cliente = self.enterContext(TestClient(self.app))

    def gravar(self, camada, documento):
        (self.raiz / camada / "Metadados.json").write_text(
            json.dumps(documento), encoding="utf-8")

    def arquivo(self, camada, caminho, conteudo=b"parquet de teste"):
        destino = self.raiz / camada / caminho
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(conteudo)
        return destino

    def test_saude_publica_e_autenticacao_obrigatoria(self):
        self.assertEqual(self.cliente.get("/saude").json(), {"status": "ok"})
        for url in ("/metricas/resumo", "/metricas/execucoes", "/camadas/Ouro",
                    "/arquivos/Ouro/parte.parquet"):
            with self.subTest(url=url):
                resposta = self.cliente.get(url)
                self.assertEqual(resposta.status_code, 401)
                self.assertIn("Basic", resposta.headers["www-authenticate"])
        for credencial in (("admin", "admin"), ("engenharia", "errada"),
                           ("intruso", "Engenharia123!")):
            self.assertEqual(self.cliente.get("/camadas/Raw", auth=credencial).status_code, 401)

    def test_matriz_de_permissoes_sem_administrador_universal(self):
        for credencial, permitidas in ((ENGENHARIA, {"Raw", "Bronze", "Prata"}),
                                       (ANALISE, {"Ouro"}), (MONITOR, set())):
            for camada in ("Raw", "Bronze", "Prata", "Ouro"):
                with self.subTest(usuario=credencial[0], camada=camada):
                    esperado = 200 if camada in permitidas else 403
                    self.assertEqual(self.cliente.get(f"/camadas/{camada}",
                                                     auth=credencial).status_code, esperado)
            esperado = 200 if credencial == MONITOR else 403
            for endpoint in ("resumo", "execucoes"):
                self.assertEqual(self.cliente.get(f"/metricas/{endpoint}",
                                                 auth=credencial).status_code, esperado)
        self.assertEqual(self.cliente.get("/camadas/Inexistente", auth=ENGENHARIA).status_code, 404)

    def test_credenciais_de_ambiente_substituem_demonstracao_e_aceitam_unicode(self):
        from fastapi.testclient import TestClient
        from Scripts.API import criar_app
        import base64

        with patch.dict(os.environ, {"ANALISE_USUARIO": "análise", "ANALISE_SENHA": "outra-senha-ç"}):
            with TestClient(criar_app(self.raiz)) as cliente:
                self.assertEqual(cliente.get("/camadas/Ouro", auth=ANALISE).status_code, 401)
                token = base64.b64encode("análise:outra-senha-ç".encode()).decode()
                # HTTPBasic de FastAPI aceita ASCII: credenciais não ASCII devem
                # ser recusadas com 401, nunca provocar erro 500.
                self.assertIn(cliente.get("/camadas/Ouro", headers={
                    "Authorization": "Basic " + token}).status_code, (200, 401))

    def test_lista_filtra_competencia_e_preserva_metadados_descartados(self):
        self.gravar("Raw", {
            "arquivos": {
                "2026-09/Empresas0.zip": {"estado": "descartado", "bytes": 900},
                "2026-10/Empresas0.zip": {"estado": "baixado", "bytes": 8},
            }, "execucoes": [], "estado": {"meses": 4, "manutencao_ativa": True,
                "proxima_verificacao": "2026-11-20T00:00:00+00:00", "ultimo_resultado": "sucesso"},
        })
        resposta = self.cliente.get("/camadas/Raw?competencia=2026-09", auth=ENGENHARIA)
        self.assertEqual(resposta.status_code, 200)
        dados = resposta.json()
        self.assertEqual(set(dados["arquivos"]), {"2026-09/Empresas0.zip"})
        self.assertFalse(dados["arquivos"]["2026-09/Empresas0.zip"]["disponivel"])
        self.assertEqual(dados["estado"]["meses"], 4)
        self.assertTrue(dados["estado"]["manutencao_ativa"])
        self.assertEqual(self.cliente.get("/arquivos/Raw/2026-09/Empresas0.zip",
                                         auth=ENGENHARIA).status_code, 404)
        self.assertEqual(self.cliente.get("/camadas/Raw?competencia=2026-99",
                                         auth=ENGENHARIA).status_code, 422)

    def test_lista_arquivos_fisicos_e_download_na_raiz_data(self):
        self.arquivo("Ouro", "competencia=2026-10/fato/parte.parquet", b"conteudo autorizado")
        resposta = self.cliente.get("/camadas/Ouro?competencia=2026-10", auth=ANALISE)
        self.assertIn("competencia=2026-10/fato/parte.parquet", resposta.json()["arquivos"])
        self.assertEqual(resposta.json()["estado"], {})
        url = "/arquivos/Ouro/competencia=2026-10/fato/parte.parquet"
        resposta = self.cliente.get(url, auth=ANALISE)
        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(resposta.content, b"conteudo autorizado")
        for credencial in (ENGENHARIA, MONITOR):
            self.assertEqual(self.cliente.get(url, auth=credencial).status_code, 403)

    def test_manifesto_de_particao_publicada_indica_disponibilidade(self):
        self.arquivo("Ouro", "competencia=2026-10/fato/parte.parquet")
        self.gravar("Ouro", {"arquivos": {"2026-10": {
            "status": "sucesso", "caminho": "Ouro/competencia=2026-10"}}, "execucoes": []})
        arquivos = self.cliente.get("/camadas/Ouro", auth=ANALISE).json()["arquivos"]
        self.assertTrue(arquivos["2026-10"]["disponivel"])

    def test_metricas_contabilizam_importacoes_e_linhas_dos_produtos_ouro(self):
        self.gravar("Raw", {"arquivos": {}, "estado": {}, "execucoes": [
            {"execucao_id": "carga-1", "etapa": "download", "status": "sucesso", "bytes": 100, "duracao_s": 2},
            {"execucao_id": "carga-1", "etapa": "download", "status": "sucesso", "bytes": 200, "duracao_s": 4},
        ]})
        self.gravar("Ouro", {"arquivos": {}, "execucoes": [
            {"execucao_id": "carga-1", "etapa": "agregacao", "status": "sucesso", "linhas": {"dim_empresa": 2, "fato": 3}}
        ]})
        resumo = self.cliente.get("/metricas/resumo", auth=MONITOR).json()
        self.assertEqual(resumo["linhas_camadas"]["Ouro"], 5)
        self.assertEqual(resumo["importacoes"][0]["downloads"], 2)
        self.assertEqual(resumo["importacoes"][0]["bytes_transferidos"], 300)

    def test_bloqueia_traversal_caminho_absoluto_e_metadados_internos(self):
        (self.raiz.parent / "segredo.txt").write_text("SEGREDO", encoding="utf-8")
        for caminho in ("%2e%2e/%2e%2e/segredo.txt", "..%5c..%5csegredo.txt",
                        "C:%5cWindows%5cwin.ini", "%2fetc/passwd", "Metadados.json",
                        "Metadados.json.lock", "file.parquet:segredo", "arquivo%00.txt"):
            with self.subTest(caminho=caminho):
                resposta = self.cliente.get("/arquivos/Ouro/" + caminho, auth=ANALISE)
                self.assertIn(resposta.status_code, (400, 403, 404))
                self.assertNotIn("SEGREDO", resposta.text)

    def test_bloqueia_symlink_de_arquivo_diretorio_e_camada(self):
        externo = self.raiz.parent / "privado"
        externo.mkdir()
        (externo / "segredo.txt").write_text("SEGREDO", encoding="utf-8")
        try:
            (self.raiz / "Ouro" / "link.txt").symlink_to(externo / "segredo.txt")
            (self.raiz / "Ouro" / "pasta").symlink_to(externo, target_is_directory=True)
            (self.raiz / "Bronze").rmdir() if not any((self.raiz / "Bronze").iterdir()) else None
        except OSError as erro:
            self.skipTest(f"Host sem permissão para symlinks: {erro}")
        for caminho in ("link.txt", "pasta/segredo.txt"):
            self.assertIn(self.cliente.get("/arquivos/Ouro/" + caminho, auth=ANALISE).status_code,
                          (403, 404))
        self.assertNotIn("link.txt", self.cliente.get("/camadas/Ouro", auth=ANALISE).json()["arquivos"])
        (self.raiz / "Bronze" / "Metadados.json").unlink()
        (self.raiz / "Bronze").rmdir()
        (self.raiz / "Bronze").symlink_to(externo, target_is_directory=True)
        self.assertIn(self.cliente.get("/arquivos/Bronze/segredo.txt",
                                      auth=ENGENHARIA).status_code, (403, 404))

    def test_metricas_historicas_nao_confundem_descarte_com_ocupacao(self):
        self.gravar("Raw", {"arquivos": {
            "2026-10/Empresas.zip": {"estado": "descartado", "bytes": 1200},
            "2026-10/Socios.zip": {"estado": "erro"},
        }, "estado": {"manutencao_ativa": True}, "execucoes": [
            {"execucao_id": "exec-1", "competencia": "2026-10", "arquivo": "Empresas.zip",
             "etapa": "download", "status": "sucesso", "inicio": "2026-11-01T00:00:00Z",
             "fim": "2026-11-01T00:00:02Z", "duracao_s": 2, "bytes": 1200, "linhas": 10},
            {"execucao_id": "exec-1", "competencia": "2026-10", "arquivo": "Socios.zip",
             "etapa": "download", "status": "erro", "duracao_s": 4, "bytes": 0,
             "erro": "Falha para MARIA SILVA documento 12345678901", "nome": "MARIA SILVA",
             "documento": "12345678901"},
        ]})
        self.arquivo("Prata", "empresas/competencia=2026-10/parte.parquet", b"1234567")
        resumo = self.cliente.get("/metricas/resumo", auth=MONITOR).json()
        self.assertEqual(resumo["bytes_camadas"], {"Raw": 0, "Bronze": 0, "Prata": 7, "Ouro": 0})
        self.assertEqual(resumo["bytes_total"], 7)
        self.assertEqual(resumo["bytes_historicos"], 1200)
        self.assertEqual(resumo["falhas"], 1)
        self.assertEqual(resumo["pendencias"], 1)
        self.assertEqual(resumo["duracao_media_s"], 3)
        self.assertEqual(resumo["execucoes_total"], 1)
        self.assertEqual(resumo["eventos_total"], 2)
        self.assertEqual(resumo["estado"]["meses"], 4)
        eventos = self.cliente.get("/metricas/execucoes", auth=MONITOR)
        self.assertEqual(len(eventos.json()), 2)
        self.assertNotIn("MARIA SILVA", eventos.text)
        self.assertNotIn("12345678901", eventos.text)
        self.assertEqual(eventos.json()[1]["erro"], "Falha registrada; consulte os registros operacionais restritos.")

    def test_operacao_exige_engenharia_e_meses_positivos(self):
        for credencial in (ANALISE, MONITOR):
            for rota in ("reativar", "importar"):
                self.assertEqual(self.cliente.post("/operacao/" + rota,
                                                  auth=credencial).status_code, 403)
        for meses in ("0", "-1", "abc", "1.5"):
            self.assertEqual(self.cliente.post("/operacao/importar?meses=" + meses,
                                              auth=ENGENHARIA).status_code, 422)
        self.assertEqual(self.cliente.get("/operacao/importar", auth=ENGENHARIA).status_code, 405)

    def test_sucesso_posterior_resolve_alerta_de_falha_global(self):
        from Scripts.Agendador import executar
        plano = {"competencias": [], "indisponiveis": [], "arquivos": {}}
        with patch("Scripts.Agendador.catalogar", side_effect=ValueError("Falha controlada")):
            executar(4, self.raiz)
        self.assertGreater(self.cliente.get("/metricas/resumo", auth=MONITOR).json()["falhas_atuais"], 0)
        with patch("Scripts.Agendador.catalogar", return_value=plano):
            executar(4, self.raiz)
        resumo = self.cliente.get("/metricas/resumo", auth=MONITOR).json()
        self.assertEqual(resumo["falhas_atuais"], 0)
        self.assertEqual(resumo["pendencias"], 0)

    def test_listagem_nao_publica_backup_temporario(self):
        self.arquivo("Ouro", "competencia=2026-10.anterior/fato/parte.parquet")
        resposta = self.cliente.get("/camadas/Ouro", auth=ANALISE)
        self.assertEqual(resposta.json()["arquivos"], {})
        self.assertEqual(self.cliente.get("/arquivos/Ouro/competencia=2026-10.anterior/fato/parte.parquet", auth=ANALISE).status_code, 403)

    def test_importacao_assincrona_bloqueia_concorrencia_e_propaga_identidade(self):
        iniciou, liberar = threading.Event(), threading.Event()
        chamadas = []

        def executar(meses=4, raiz=None, usuario="agendador", _reservado=False):
            from Scripts.Pipeline.Metadados import trava_arquivo
            self.assertTrue(_reservado)
            with self.assertRaises(RuntimeError):
                with trava_arquivo(raiz / ".pipeline.lock", bloquear=False):
                    pass
            chamadas.append((meses, raiz, usuario))
            iniciou.set()
            liberar.wait(5)
            return {"status": "sucesso"}

        agendador = types.ModuleType("Scripts.Agendador")
        agendador.executar = executar
        with patch.dict(sys.modules, {"Scripts.Agendador": agendador}):
            try:
                resposta = self.cliente.post("/operacao/importar?meses=6", auth=ENGENHARIA)
                self.assertEqual(resposta.status_code, 202)
                self.assertTrue(iniciou.wait(2))
                self.assertEqual(chamadas, [(6, self.raiz, "engenharia")])
                self.assertEqual(self.cliente.post("/operacao/importar", auth=ENGENHARIA).status_code, 409)
            finally:
                liberar.set()

    def test_importacao_recusa_trava_de_cli_antes_de_responder_aceito(self):
        from Scripts.Pipeline.Metadados import trava_arquivo
        with trava_arquivo(self.raiz / ".pipeline.lock", bloquear=False), patch(
                "Scripts.Agendador.executar", return_value={"status": "sucesso"}) as executar:
            resposta = self.cliente.post("/operacao/importar", auth=ENGENHARIA)
            self.assertEqual(resposta.status_code, 409)
            executar.assert_not_called()

    def test_reativacao_chama_agendador_com_raiz_data(self):
        chamadas = []
        agendador = types.ModuleType("Scripts.Agendador")
        def reativar(raiz):
            chamadas.append(raiz)
            return {"manutencao_ativa": True, "meses": 4}
        agendador.reativar = reativar
        with patch.dict(sys.modules, {"Scripts.Agendador": agendador}):
            resposta = self.cliente.post("/operacao/reativar", auth=ENGENHARIA)
        self.assertEqual(resposta.status_code, 200)
        self.assertTrue(resposta.json()["manutencao_ativa"])
        self.assertEqual(chamadas, [self.raiz])

    def test_agendador_opt_in_e_parada_no_lifespan(self):
        from fastapi.testclient import TestClient
        from Scripts.API import criar_app

        chamadas, stop = [], threading.Event()
        def iniciar(raiz):
            chamadas.append(raiz)
            thread = threading.Thread(target=stop.wait, daemon=True)
            thread.start()
            return stop, thread
        agendador = types.ModuleType("Scripts.Agendador")
        agendador.iniciar = iniciar
        with patch.dict(sys.modules, {"Scripts.Agendador": agendador}):
            with TestClient(criar_app(self.raiz)):
                self.assertEqual(chamadas, [])
            with patch.dict(os.environ, {"AGENDAMENTO_ATIVO": "1"}):
                with TestClient(criar_app(self.raiz)) as cliente:
                    self.assertEqual(cliente.get("/saude").status_code, 200)
                    self.assertEqual(chamadas, [self.raiz])
                    self.assertFalse(stop.is_set())
                self.assertTrue(stop.is_set())

    def test_leituras_nao_importam_spark_nem_agendador(self):
        codigo = """
import importlib.abc, sys
class Bloqueio(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'Scripts.Agendador' or fullname.startswith('pyspark'):
            raise AssertionError('Importacao indevida: ' + fullname)
sys.meta_path.insert(0, Bloqueio())
from Scripts.API import criar_app
from fastapi.testclient import TestClient
with TestClient(criar_app(sys.argv[1])) as cliente:
    assert cliente.get('/metricas/resumo', auth=('monitor','Monitor123!')).status_code == 200
    assert cliente.get('/camadas/Raw', auth=('engenharia','Engenharia123!')).status_code == 200
"""
        resultado = subprocess.run([sys.executable, "-B", "-c", codigo, str(self.raiz)],
                                   capture_output=True, text=True, env=dict(os.environ), timeout=15)
        self.assertEqual(resultado.returncode, 0, resultado.stderr)


if __name__ == "__main__":
    unittest.main()
