"""HTTP/WebDAV local -> ZIP -> CSV -> Spark -> Parquet, com retomada real."""

import csv
from datetime import date
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import tempfile
from threading import Thread
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from test_spark import CAMPOS, empresa, estabelecimento, socio


def montar_zip(entidade, linhas):
    csv_texto = StringIO(newline="")
    writer = csv.writer(csv_texto, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerows([[linha.get(campo, "") for campo in CAMPOS[entidade]] for linha in linhas])
    buffer = BytesIO()
    with ZipFile(buffer, "w") as arquivo:
        arquivo.writestr("CADASTRO.DADOS", csv_texto.getvalue().encode("iso-8859-1"))
    return buffer.getvalue()


class IntegracaoTests(unittest.TestCase):
    def test_importacao_descarta_intermediarios_nao_rebaixa_e_retoma_prata(self):
        from Scripts.Agendador import executar, manutencao
        from Scripts.Pipeline.BaixarArquivos import meses_calendario
        from Scripts.Pipeline.Metadados import Metadados
        from Scripts.Pipeline.PipelinePrata import processar_arquivo

        competencia = meses_calendario(1)[0]
        arquivos = {
            "Empresas0.zip": montar_zip("empresas", [empresa()]),
            "Estabelecimentos0.zip": montar_zip("estabelecimentos", [estabelecimento()]),
            "Socios0.zip": montar_zip("socios", [socio()]),
        }
        baixados = []

        class FonteLocal(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_PROPFIND(self):
                if self.path == "/dav/":
                    itens = f'<d:response><d:href>/dav/{competencia}/</d:href><d:propstat><d:prop><d:displayname>{competencia}</d:displayname><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>'
                else:
                    itens = "".join(f'<d:response><d:href>/dav/{competencia}/{nome}</d:href><d:propstat><d:prop><d:displayname>{nome}</d:displayname><d:resourcetype/><d:getcontentlength>{len(conteudo)}</d:getcontentlength><d:getetag>"{hashlib.sha256(conteudo).hexdigest()}"</d:getetag><d:getlastmodified>Mon, 16 Nov 2026 10:00:00 GMT</d:getlastmodified></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>' for nome, conteudo in arquivos.items())
                resposta = ('<d:multistatus xmlns:d="DAV:">' + itens + '</d:multistatus>').encode()
                self.send_response(207)
                self.send_header("Content-Length", str(len(resposta)))
                self.end_headers()
                self.wfile.write(resposta)

            def do_GET(self):
                nome = self.path.rsplit("/", 1)[-1]
                conteudo = arquivos[nome]
                baixados.append(nome)
                self.send_response(200)
                self.send_header("Content-Length", str(len(conteudo)))
                self.end_headers()
                self.wfile.write(conteudo)

        with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(("127.0.0.1", 0), FonteLocal) as fonte:
            thread = Thread(target=fonte.serve_forever, daemon=True)
            thread.start()
            try:
                raiz = Path(tmp) / "DATA"
                with patch.dict(os.environ, {"CNPJ_DAV_URL": f"http://127.0.0.1:{fonte.server_port}/dav/"}):
                    resultado = executar(1, raiz)
                    self.assertEqual(resultado["status"], "sucesso", resultado)
                    self.assertEqual(len(baixados), 3)
                    self.assertFalse(list((raiz / "Raw").rglob("*.zip")))
                    self.assertFalse(list((raiz / "Bronze").rglob("*.DADOS")))
                    meta = Metadados(raiz)
                    self.assertEqual(meta.ler("Ouro")["arquivos"][competencia]["status"], "sucesso")
                    self.assertEqual(executar(1, raiz)["status"], "sucesso")
                    self.assertEqual(len(baixados), 3, "Intermediários descartados não devem provocar nova baixa")

                    arquivos["Empresas0.zip"] = montar_zip("empresas", [empresa(capital_social="2000,00")])
                    with patch("Scripts.Pipeline.PipelinePrata.processar_arquivo", side_effect=ValueError("Falha controlada")):
                        erro = executar(1, raiz)
                    self.assertEqual(erro["status"], "erro")
                    self.assertEqual(len(baixados), 4)
                    self.assertTrue((raiz / "Bronze" / competencia / "Empresas0").is_dir())
                    self.assertTrue(meta.estado()["manutencao_ativa"])
                    retomada = manutencao(raiz)
                    self.assertEqual(retomada["status"], "sucesso", retomada)
                    self.assertEqual(len(baixados), 4, "Retomar Prata deve reaproveitar a Bronze existente")
                    self.assertFalse((raiz / "Bronze" / competencia / "Empresas0").exists())
                    self.assertFalse(meta.estado()["manutencao_ativa"])
                    self.assertGreater(meta.ler("Prata")["arquivos"][competencia + "/Empresas0.zip"]["bytes"], 0)
            finally:
                fonte.shutdown()
                thread.join(timeout=5)
