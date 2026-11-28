"""Exercita o formato WebDAV real e a extração dos ZIPs, sem baixar a base."""

from io import BytesIO
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch
from uuid import uuid4
from zipfile import ZipFile

from Scripts.Pipeline.BaixarArquivos import interpretar_webdav, versao_remota, extrair_arquivo, listar


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.raiz = Path(__file__).parent / (".teste-" + uuid4().hex)
        self.raiz.mkdir()
        self.addCleanup(shutil.rmtree, self.raiz, True)

    def test_propstat_404_nao_esconde_propriedades_200(self):
        xml = '''<d:multistatus xmlns:d="DAV:"><d:response>
        <d:href>/public.php/dav/files/token/2026-11/Empresas0.zip</d:href>
        <d:propstat><d:prop><d:getcontentlength/></d:prop><d:status>HTTP/1.1 404 Not Found</d:status></d:propstat>
        <d:propstat><d:prop><d:displayname>Empresas0.zip</d:displayname><d:getcontentlength>100</d:getcontentlength><d:getetag>"abc"</d:getetag><d:getlastmodified>Mon, 16 Nov 2026 10:00:00 GMT</d:getlastmodified><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>
        </d:response></d:multistatus>'''
        itens = interpretar_webdav(xml, "https://fonte.test/public.php/dav/files/token/")
        self.assertEqual(itens[0]["nome"], "Empresas0.zip")
        self.assertEqual(itens[0]["bytes"], 100)
        self.assertFalse(itens[0]["diretorio"])
        self.assertEqual(itens[0]["modificado_em"], "2026-11-16T10:00:00+00:00")

    def test_etag_participa_da_versao_mesmo_com_data_e_tamanho_iguais(self):
        a = {"modificado_em": "2026-11-16T10:00:00+00:00", "bytes": 100, "etag": "a"}
        b = {**a, "etag": "b"}
        self.assertNotEqual(versao_remota(a), versao_remota(b))

    def test_extracao_preserva_sufixo_oficial_e_rejeita_traversal(self):
        valido = self.raiz / "origem.zip"
        with ZipFile(valido, "w") as zip_file:
            zip_file.writestr("arquivo.EMPRECSV", '"10000001";"Empresa"')
        resultado = extrair_arquivo(valido, self.raiz / "bronze")
        self.assertEqual(len(resultado["arquivos"]), 1)
        self.assertTrue((self.raiz / "bronze" / "arquivo.EMPRECSV").exists())
        perigoso = self.raiz / "perigoso.zip"
        with ZipFile(perigoso, "w") as zip_file:
            zip_file.writestr("../fora.txt", "conteudo")
        with self.assertRaises(ValueError):
            extrair_arquivo(perigoso, self.raiz / "bronze")
        self.assertTrue((self.raiz / "bronze" / "arquivo.EMPRECSV").exists())
        self.assertFalse((self.raiz / "fora.txt").exists())

    def test_zip_invalido_nao_substitui_bronze_valida(self):
        destino = self.raiz / "bronze"
        destino.mkdir()
        (destino / "original").write_text("preservado", encoding="utf-8")
        invalido = self.raiz / "ruim.zip"
        invalido.write_bytes(b"resposta HTML no lugar do ZIP")
        with self.assertRaises(Exception):
            extrair_arquivo(invalido, destino)
        self.assertTrue((destino / "original").exists())

    def test_listagem_webdav_envia_xml_e_repete_resposta_vazia(self):
        import requests
        respostas = [b"", b'<d:multistatus xmlns:d="DAV:"/>']
        def fonte(method, url, **opcoes):
            self.assertEqual(method, "PROPFIND")
            self.assertEqual(opcoes["headers"].get("Content-Type"), "application/xml")
            self.assertIn("User-Agent", opcoes["headers"])
            resposta = requests.Response()
            resposta.status_code = 207
            resposta._content = respostas.pop(0)
            resposta.encoding = "utf-8"
            return resposta
        with patch("Scripts.Pipeline.BaixarArquivos.requests.request", side_effect=fonte), patch("Scripts.Pipeline.BaixarArquivos.time.sleep"):
            self.assertEqual(listar("https://fonte.test/dav/"), [])
        self.assertEqual(respostas, [])
