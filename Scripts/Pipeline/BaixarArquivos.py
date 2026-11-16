"""Descobre os arquivos no WebDAV público, baixa e extrai com validação."""

from datetime import date, datetime
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import time
from urllib.parse import unquote, urljoin, urlparse
from xml.etree import ElementTree as ET
from zipfile import ZipFile, is_zipfile
from zoneinfo import ZoneInfo

import requests

from Scripts.Pipeline.Metadados import publicar_diretorio


DAV_PADRAO = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/"
# WebDAV permite listar nomes e versões sem transferir os ZIPs. PROPFIND pede
# estas propriedades; getetag é um identificador de versão fornecido pelo servidor.
XML_PROPFIND = '''<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop>
<d:displayname/><d:getlastmodified/><d:getcontentlength/><d:getetag/><d:resourcetype/>
</d:prop></d:propfind>'''
NS = {"d": "DAV:"}


def meses_calendario(quantidade: int, hoje: date | None = None) -> list[str]:
    """Inclui o mês atual. Ex.: em novembro, 4 significa agosto a novembro."""
    if isinstance(quantidade, bool) or not isinstance(quantidade, int) or quantidade < 1:
        raise ValueError("A quantidade de meses deve ser um inteiro positivo")
    hoje = hoje or datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    # Converter ano/mês para um índice permite voltar meses atravessando a virada do ano.
    indice = hoje.year * 12 + hoje.month - 1
    if quantidade > indice:
        raise ValueError("Quantidade de meses anterior ao calendário suportado")
    return [f"{mes // 12:04d}-{mes % 12 + 1:02d}" for mes in range(indice - quantidade + 1, indice + 1)]


def selecionar_competencias(meses: int, disponiveis: list[str], hoje: date | None = None) -> dict:
    # Um mês ausente é informado, sem ampliar a janela para buscar outro mais antigo.
    solicitadas = meses_calendario(meses, hoje)
    return {"competencias": [m for m in solicitadas if m in disponiveis],
            "indisponiveis": [m for m in solicitadas if m not in disponiveis]}


def interpretar_webdav(conteudo: str, base: str) -> list[dict]:
    # Transforma a resposta XML em dicionários usados pelo restante do pipeline.
    # As URLs também são validadas para continuarem no compartilhamento autorizado.
    itens = []
    origem = urlparse(base)
    for resposta in ET.fromstring(conteudo).findall("d:response", NS):
        propriedades = {}
        # Uma resposta pode ter propriedades 200 e 404: não usar só o primeiro propstat.
        for grupo in resposta.findall("d:propstat", NS):
            if " 200 " in (grupo.findtext("d:status", "", NS) + " "):
                prop = grupo.find("d:prop", NS)
                if prop is not None:
                    propriedades.update({no.tag: no for no in prop})
        href = resposta.findtext("d:href", "", NS)
        url = urljoin(base, href)
        parsed = urlparse(url)
        if (parsed.scheme, parsed.netloc) != (origem.scheme, origem.netloc) or not parsed.path.startswith(origem.path):
            raise ValueError("A listagem retornou endereço fora do compartilhamento autorizado")
        def texto(campo):
            no = propriedades.get("{DAV:}" + campo)
            return (no.text or "") if no is not None else ""
        nome = texto("displayname") or unquote(parsed.path.rstrip("/").split("/")[-1])
        if "/" in nome or "\\" in nome or nome in (".", ".."):
            raise ValueError("Nome de arquivo remoto inválido")
        tipo = propriedades.get("{DAV:}resourcetype")
        modificado = texto("getlastmodified")
        itens.append({"nome": nome, "url": url, "diretorio": tipo is not None and tipo.find("d:collection", NS) is not None,
                      "modificado_em": parsedate_to_datetime(modificado).isoformat() if modificado else None,
                      "bytes": int(texto("getcontentlength") or 0), "etag": texto("getetag")})
    return itens


def listar(base: str) -> list[dict]:
    # Depth=1 lista os filhos imediatos. Os dois limites de timeout representam
    # conexão e espera de resposta; a repetição cobre falhas transitórias da fonte.
    headers = {"Depth": "1", "X-Requested-With": "XMLHttpRequest",
               "Content-Type": "application/xml", "User-Agent": "cnpj-lakehouse-student/1.0"}
    for tentativa in range(1, 4):
        try:
            with requests.request("PROPFIND", base, headers=headers, data=XML_PROPFIND.encode(), timeout=(15, 120)) as resposta:
                resposta.raise_for_status()
                return interpretar_webdav(resposta.text, base)
        except (requests.RequestException, ET.ParseError) as erro:
            permanente = isinstance(erro, requests.HTTPError) and erro.response is not None and erro.response.status_code < 500 and erro.response.status_code != 429
            if permanente or tentativa == 3:
                raise RuntimeError("Não foi possível obter uma listagem WebDAV válida da fonte") from erro
            time.sleep(tentativa)
    raise RuntimeError("Listagem WebDAV não concluída")


def catalogar(meses: int, base: str | None = None) -> dict:
    # Primeiro descobrir as pastas mensais; depois listar os arquivos de cada mês.
    # O catálogo contém URLs/metadados, não o conteúdo dos ZIPs.
    base = (base or os.getenv("CNPJ_DAV_URL", DAV_PADRAO)).rstrip("/") + "/"
    pastas = {p["nome"]: p for p in listar(base) if p["diretorio"] and re.fullmatch(r"\d{4}-\d{2}", p["nome"])}
    plano = selecionar_competencias(meses, list(pastas))
    plano["arquivos"] = {}
    for competencia in plano["competencias"]:
        itens = [a for a in listar(pastas[competencia]["url"]) if not a["diretorio"]]
        plano["arquivos"][competencia] = [{**a, "competencia": competencia} for a in itens]
    return plano


def versao_remota(item: dict) -> str:
    # Esta assinatura compara metadados remotos, inclusive mudanças com mesma data.
    # É diferente do checksum do conteúdo, calculado durante o download abaixo.
    assinatura = {k: item.get(k) for k in ("modificado_em", "etag", "bytes")}
    return hashlib.sha256(json.dumps(assinatura, sort_keys=True).encode()).hexdigest()


def baixar_arquivo(item: dict, destino: Path) -> dict:
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_suffix(destino.suffix + ".part")
    # .part nunca é apresentado como ZIP concluído. O relógio monotônico mede
    # duração; os horários de auditoria são registrados pelo coordenador.
    inicio = time.monotonic()
    for tentativa in range(1, 4):
        try:
            digest = hashlib.sha256()
            tamanho = 0
            with requests.get(item["url"], stream=True, timeout=(15, 120)) as resposta:
                resposta.raise_for_status()
                with parcial.open("wb") as arquivo:
                    # Gravar blocos de 1 MiB mantém o consumo de RAM pequeno,
                    # mesmo quando um ZIP tem vários gigabytes.
                    for bloco in resposta.iter_content(1024 * 1024):
                        if bloco:
                            arquivo.write(bloco)
                            digest.update(bloco)
                            tamanho += len(bloco)
            if item.get("bytes") and tamanho != item["bytes"]:
                raise ValueError("Tamanho baixado difere do informado pela fonte")
            if not is_zipfile(parcial):
                raise ValueError("O arquivo baixado não é um ZIP válido")
            # A assinatura SHA-256 permite auditar exatamente os bytes recebidos.
            # A validação completa de CRC acontecerá durante a extração.
            parcial.replace(destino)
            return {"checksum": digest.hexdigest(), "bytes": tamanho, "tentativas": tentativa,
                    "duracao_s": round(time.monotonic() - inicio, 3)}
        except Exception as erro:
            parcial.unlink(missing_ok=True)
            # Problemas temporários de rede podem repetir; erro de validação ou
            # HTTP permanente encerra esta tentativa e fica registrado na auditoria.
            permanente = isinstance(erro, ValueError) or (isinstance(erro, requests.HTTPError) and erro.response is not None and erro.response.status_code < 500 and erro.response.status_code != 429)
            if permanente or tentativa == 3:
                raise
            time.sleep(tentativa)
    raise RuntimeError("Download não concluído")


def extrair_arquivo(zip_path: Path, destino: Path) -> dict:
    """A extração verifica CRC e só publica quando todos os membros terminam."""
    destino = Path(destino)
    staging = destino.with_name(destino.name + ".novo")
    # Extrair primeiro em .novo protege uma Bronze válida de uma extração incompleta.
    if staging.exists():
        shutil.rmtree(staging)
    inicio = time.monotonic()
    try:
        with ZipFile(zip_path) as zip_file:
            for membro in zip_file.infolist():
                # Rejeitar caminhos que tentariam sair da pasta de extração (zip slip)
                # e links que pudessem redirecionar a gravação para outro lugar.
                nome = membro.filename.replace("\\", "/")
                caminho = PurePosixPath(nome)
                if caminho.is_absolute() or ".." in caminho.parts or ":" in nome or stat.S_ISLNK(membro.external_attr >> 16):
                    raise ValueError("ZIP contém caminho inválido")
            staging.mkdir(parents=True, exist_ok=True)
            # O zipfile confere o CRC ao ler cada membro. Qualquer falha impede a publicação.
            zip_file.extractall(staging)
        arquivos = [p for p in staging.rglob("*") if p.is_file()]
        if not arquivos:
            raise ValueError("ZIP sem arquivos de dados")
        tamanho = sum(p.stat().st_size for p in arquivos)
        relativos = [p.relative_to(staging) for p in arquivos]
        publicar_diretorio(staging, destino)
        return {"arquivos": [destino / p for p in relativos], "bytes": tamanho,
                "duracao_s": round(time.monotonic() - inicio, 3)}
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
