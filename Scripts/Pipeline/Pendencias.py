"""Consulta pendências conhecidas sem rede, escrita em metadados ou sessão Spark."""

import hashlib
import json
from pathlib import Path

from Scripts.Pipeline.BaixarArquivos import versao_remota
from Scripts.Pipeline.Esquemas import entidade_arquivo


def assinatura_prata(registros, itens, competencia):
    """A Ouro depende da versão remota e da publicação local de cada origem."""
    dependencias = {item["nome"]: {campo: registros.get(f"{competencia}/{item['nome']}", {}).get(campo)
                                 for campo in ("versao", "publicado_em")}
                    for item in itens}
    assinatura = hashlib.sha256(json.dumps(dependencias, sort_keys=True).encode()).hexdigest()
    return dependencias, assinatura


def listar_pendencias(raiz, manifestos, meses=None):
    from Scripts.Pipeline.BaixarArquivos import meses_calendario

    raiz = Path(raiz).resolve()
    registros = {camada: dados.get("arquivos", {}) for camada, dados in manifestos.items()}
    estado = manifestos["Raw"].get("estado", {})
    meses = meses if meses is not None else estado.get("meses", 4)
    solicitadas = meses_calendario(meses)
    catalogo = (estado.get("catalogo") or {}).get("arquivos", {})
    conhecidos = {f"{mes}/{item['nome']}": {**item, "competencia": mes}
                  for mes, itens in catalogo.items() for item in itens if entidade_arquivo(item["nome"])}

    # Mesmo sem catálogo, uma falha antiga registrada não deve desaparecer da consulta.
    for camada in ("Raw", "Bronze", "Prata"):
        for chave in registros[camada]:
            if "/" not in chave or chave in conhecidos:
                continue
            competencia, nome = chave.split("/", 1)
            # Havendo inventário conhecido do mês, ele define suas origens ativas.
            # Uma origem retirada continua na auditoria, sem invalidar a Ouro.
            if competencia in catalogo or not entidade_arquivo(nome):
                continue
            versao = next((registros[c].get(chave, {}).get("versao")
                           for c in ("Raw", "Bronze", "Prata") if registros[c].get(chave, {}).get("versao")), None)
            conhecidos[chave] = {"competencia": competencia, "nome": nome, "_versao": versao}

    def diretorio_existe(caminho):
        if not caminho:
            return False
        destino = (raiz / caminho).resolve()
        return destino.is_relative_to(raiz) and destino.is_dir()

    arquivos, por_mes = [], {mes: [] for mes in catalogo}
    for chave, item in sorted(conhecidos.items()):
        competencia, nome = item["competencia"], item["nome"]
        por_mes.setdefault(competencia, []).append(item)
        versao = item["_versao"] if "_versao" in item else versao_remota(item)
        prata = registros["Prata"].get(chave, {})
        if (prata.get("status") == "sucesso" and (versao is None or prata.get("versao") == versao)
                and diretorio_existe(prata.get("caminho"))):
            continue
        bronze = registros["Bronze"].get(chave, {})
        raw = registros["Raw"].get(chave, {})
        if (bronze.get("status") == "extraido" and bronze.get("versao") == versao
                and diretorio_existe(f"Bronze/{competencia}/{Path(nome).stem}")):
            etapa = "prata"
        elif raw.get("status") == "baixado" and raw.get("versao") == versao and (raiz / "Raw" / chave).is_file():
            etapa = "extracao"
        else:
            etapa = "download"
        arquivos.append({"competencia": competencia, "arquivo": nome, "entidade": entidade_arquivo(nome),
                         "etapa": etapa, "dentro_intervalo": competencia in solicitadas,
                         "status_raw": raw.get("status", "nao_iniciado"),
                         "status_bronze": bronze.get("status", "nao_iniciado"),
                         "status_prata": prata.get("status", "nao_iniciado")})

    ouro = []
    for competencia, itens in sorted(por_mes.items()):
        registro = registros["Ouro"].get(competencia, {})
        _, assinatura = assinatura_prata(registros["Prata"], itens, competencia)
        if any(a["competencia"] == competencia for a in arquivos):
            status = "aguardando_prata"
        elif {entidade_arquivo(i["nome"]) for i in itens} != {"empresas", "estabelecimentos", "socios"}:
            status = "aguardando_entidades"
        elif (registro.get("status") == "sucesso" and registro.get("versao") == assinatura
              and diretorio_existe(registro.get("caminho"))):
            continue
        else:
            status = "pendente"
        ouro.append({"competencia": competencia, "status": status, "dentro_intervalo": competencia in solicitadas})

    elegiveis = sum(a["dentro_intervalo"] for a in arquivos)
    return {"meses": meses, "competencias_solicitadas": solicitadas,
            "total_arquivos_pendentes": len(arquivos), "arquivos_elegiveis": elegiveis,
            "arquivos_fora_intervalo": len(arquivos) - elegiveis, "arquivos": arquivos, "ouro": ouro}
