"""Coordena uma competência por vez e mantém as duas rotinas periódicas."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
import shutil
from threading import Event, Thread
import time
from uuid import uuid4
from zipfile import BadZipFile
import zlib

from Scripts.Pipeline.BaixarArquivos import baixar_arquivo, catalogar, extrair_arquivo, versao_remota
from Scripts.Pipeline.Metadados import Metadados, agora, trava_arquivo
from Scripts.Pipeline.Pendencias import assinatura_prata


LOG = logging.getLogger("cnpj")

# Este arquivo organiza a sequência; as regras de leitura e transformação
# ficam nos módulos de Pipeline. "Competência" é o mês do lote, como 2026-11.

def raiz_dados(raiz=None) -> Path:
    # Nos containers, DATA_DIR=/app/DATA aponta para o diretório montado do host.
    return Path(raiz or os.getenv("DATA_DIR", Path(__file__).resolve().parents[1] / "DATA")).resolve()


def _evento(meta, camada, execucao_id, competencia, arquivo, etapa, status, inicio, **campos):
    # Um registro por tentativa/etapa alimenta a auditoria histórica e o Grafana.
    meta.evento(camada, {"execucao_id": execucao_id, "competencia": competencia, "arquivo": arquivo,
                         "etapa": etapa, "status": status, "inicio": inicio, "fim": agora(), **campos})
    LOG.info("Etapa %s: %s/%s, status=%s, duracao_s=%s", etapa, competencia, arquivo or "mes",
             status, campos.get("duracao_s", "-"))


def _iniciar_etapa(meta, etapa, competencia=None, arquivo=None):
    inicio = agora()
    meta.estado(etapa_atual=etapa, competencia_atual=competencia, arquivo_atual=arquivo, etapa_inicio=inicio)
    LOG.info("Iniciando %s: competencia=%s arquivo=%s", etapa, competencia, arquivo or "mes")
    return inicio


def _baixar(item, raiz, meta, execucao_id, forcar):
    # A chave mensal impede misturar Empresas0.zip de competências diferentes.
    chave = f"{item['competencia']}/{item['nome']}"
    versao = versao_remota(item)
    zip_path = raiz / "Raw" / item["competencia"] / item["nome"]
    bronze = raiz / "Bronze" / item["competencia"] / Path(item["nome"]).stem
    registro = meta.ler("Bronze")["arquivos"].get(chave, {})
    # Se a Bronze já foi extraída, a retomada pode ir direto para a Prata.
    # Nesse caso, retornar o caminho do ZIP não significa que ele ainda exista.
    if not forcar and registro.get("versao") == versao and registro.get("status") == "extraido" and bronze.is_dir():
        return zip_path
    raw = meta.ler("Raw")["arquivos"].get(chave, {})
    # Uma baixa válida interrompida antes da extração também pode ser reaproveitada.
    if not forcar and raw.get("versao") == versao and raw.get("status") == "baixado" and zip_path.is_file():
        return zip_path
    inicio = agora()
    meta.atualizar("Raw", chave, status="baixando", tentativa_em=inicio, erro=None)
    try:
        resultado = baixar_arquivo(item, zip_path)
        meta.atualizar("Raw", chave, status="baixado", versao=versao, baixado_em=agora(),
                       modificado_em=item.get("modificado_em"), etag=item.get("etag"),
                       caminho=str(zip_path.relative_to(raiz)), erro=None, **resultado)
        _evento(meta, "Raw", execucao_id, item["competencia"], item["nome"], "download", "sucesso", inicio, **resultado)
        return zip_path
    except Exception as erro:
        # A versão só é concluída no sucesso; a próxima tentativa verá este erro.
        meta.atualizar("Raw", chave, status="erro", erro=str(erro), etapa_erro="download")
        _evento(meta, "Raw", execucao_id, item["competencia"], item["nome"], "download", "erro", inicio, erro=str(erro))
        raise


def _descartar_intermediarios(item, raiz, meta):
    """Também reconcilia interrupções entre publicar a Prata e liberar disco."""
    chave = f"{item['competencia']}/{item['nome']}"
    (raiz / "Raw" / chave).unlink(missing_ok=True)
    bronze = raiz / "Bronze" / item["competencia"] / Path(item["nome"]).stem
    if bronze.exists():
        shutil.rmtree(bronze)
    for camada in ("Raw", "Bronze"):
        registro = meta.ler(camada)["arquivos"].get(chave)
        if registro and registro.get("status") != "descartado":
            meta.atualizar(camada, chave, status="descartado", descartado_em=agora(), erro=None)


def processar_competencia(competencia, itens, raiz, meta, execucao_id, usuario, forcar=False, spark_ref=None):
    from Scripts.Pipeline.Esquemas import entidade_arquivo
    from Scripts.Pipeline.PipelinePrata import criar_spark, processar_arquivo
    from Scripts.Pipeline.PipelineOuro import processar_ouro

    # 1. Exigir as três entidades antes de produzir indicadores daquele mês.
    esperados = [item for item in itens if entidade_arquivo(item["nome"])]
    entidades = {entidade_arquivo(item["nome"]) for item in esperados}
    if entidades != {"empresas", "estabelecimentos", "socios"}:
        raise ValueError(f"Competência {competencia} sem as três entidades obrigatórias")
    for item in itens:
        if not entidade_arquivo(item["nome"]):
            meta.atualizar("Raw", f"{competencia}/{item['nome']}", status="fora_do_escopo", bytes=item.get("bytes", 0))
    # 2. A Prata concluída é a referência para decidir se uma origem precisa
    # voltar ao pipeline. ZIP/CSV já descartados não provocam nova baixa.
    pendentes = []
    for item in esperados:
        chave = f"{competencia}/{item['nome']}"
        registro = meta.ler("Prata")["arquivos"].get(chave, {})
        caminho = registro.get("caminho")
        if forcar or registro.get("status") != "sucesso" or registro.get("versao") != versao_remota(item) or not caminho or not (raiz / caminho).is_dir():
            pendentes.append(item)
        else:
            _descartar_intermediarios(item, raiz, meta)
    # O tamanho do lote limita os ZIPs prontos aguardando extração; não enfileirar o mês inteiro.
    workers = max(1, int(os.getenv("DOWNLOAD_WORKERS", "3")))
    # A lista compartilha uma única sessão Spark entre arquivos e competências.
    # Começa vazia para uma execução sem novidades não iniciar a JVM à toa.
    spark_ref = spark_ref if spark_ref is not None else []
    for posicao in range(0, len(pendentes), workers):
        lote = pendentes[posicao:posicao + workers]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # 3. Somente os downloads são paralelos. Cada future representa uma
            # baixa em andamento; result() aguarda seu resultado e propaga falhas.
            futuros = [(item, pool.submit(_baixar, item, raiz, meta, execucao_id, forcar)) for item in lote]
            for item, futuro in futuros:
                _iniciar_etapa(meta, "aguardando_download", competencia, item["nome"])
                zip_path = futuro.result()
                chave = f"{competencia}/{item['nome']}"
                versao = versao_remota(item)
                destino = raiz / "Bronze" / competencia / Path(item["nome"]).stem
                registro = meta.ler("Bronze")["arquivos"].get(chave, {})
                # 4. Extração e transformação são sequenciais para limitar RAM/disco.
                if forcar or registro.get("versao") != versao or registro.get("status") != "extraido" or not destino.is_dir():
                    inicio = _iniciar_etapa(meta, "extracao", competencia, item["nome"])
                    meta.atualizar("Bronze", chave, status="extraindo", erro=None)
                    try:
                        extraido = extrair_arquivo(zip_path, destino)
                        meta.atualizar("Bronze", chave, status="extraido", versao=versao, extraido_em=agora(),
                                       arquivos=[str(p.relative_to(raiz)) for p in extraido["arquivos"]],
                                       bytes=extraido["bytes"], duracao_s=extraido["duracao_s"], erro=None)
                        _evento(meta, "Bronze", execucao_id, competencia, item["nome"], "extracao", "sucesso", inicio,
                                bytes=extraido["bytes"], duracao_s=extraido["duracao_s"])
                    except Exception as erro:
                        if isinstance(erro, (BadZipFile, EOFError, zlib.error)):
                            # Um ZIP reconhecível ainda pode ter CRC/conteúdo corrompido.
                            # Invalidá-lo impede reutilizar o mesmo arquivo na retomada.
                            meta.atualizar("Raw", chave, status="erro", erro=str(erro), etapa_erro="validacao_zip")
                        meta.atualizar("Bronze", chave, status="erro", erro=str(erro), etapa_erro="extracao")
                        _evento(meta, "Bronze", execucao_id, competencia, item["nome"], "extracao", "erro", inicio, erro=str(erro))
                        raise
                # Se houver falha na Prata, o ZIP já pode sair, mas os CSVs permanecem para retomar.
                zip_path.unlink(missing_ok=True)
                meta.atualizar("Raw", chave, status="descartado", descartado_em=agora(), erro=None)
                inicio, relogio = _iniciar_etapa(meta, "prata", competencia, item["nome"]), time.monotonic()
                meta.atualizar("Prata", chave, status="processando", erro=None)
                try:
                    if not spark_ref:
                        spark_ref.append(criar_spark())
                    arquivos = [p for p in destino.rglob("*") if p.is_file()]
                    resultado = processar_arquivo(spark_ref[0], arquivos, entidade_arquivo(item["nome"]), competencia,
                                                  item["nome"], raiz, execucao_id, usuario)
                    duracao = round(time.monotonic() - relogio, 3)
                    # Só marcar sucesso depois de o módulo Prata validar e publicar
                    # o Parquet. Até lá os CSVs continuam disponíveis para retomar.
                    meta.atualizar("Prata", chave, status="sucesso", versao=versao, publicado_em=agora(), erro=None, **resultado)
                    _evento(meta, "Prata", execucao_id, competencia, item["nome"], "transformacao", "sucesso", inicio,
                            duracao_s=duracao, **resultado)
                except Exception as erro:
                    meta.atualizar("Prata", chave, status="erro", erro=str(erro), etapa_erro="transformacao")
                    _evento(meta, "Prata", execucao_id, competencia, item["nome"], "transformacao", "erro", inicio, erro=str(erro))
                    raise
                _descartar_intermediarios(item, raiz, meta)
    # 5. Esta barreira só é alcançada quando toda a Prata necessária terminou.
    # A Ouro depende tanto da versão remota quanto da publicação local da Prata.
    dependencias, assinatura = assinatura_prata(meta.ler("Prata")["arquivos"], esperados, competencia)
    # Uma republicação local também invalida a Ouro, mesmo sem mudança no gov.
    ouro = meta.ler("Ouro")["arquivos"].get(competencia, {})
    if forcar or ouro.get("status") != "sucesso" or ouro.get("versao") != assinatura or not (raiz / ouro.get("caminho", "inexistente")).is_dir():
        inicio, relogio = _iniciar_etapa(meta, "ouro", competencia), time.monotonic()
        meta.atualizar("Ouro", competencia, status="processando", erro=None)
        try:
            if not spark_ref:
                spark_ref.append(criar_spark())
            resultado = processar_ouro(spark_ref[0], raiz, competencia, execucao_id, usuario)
            meta.atualizar("Ouro", competencia, status="sucesso", versao=assinatura,
                           dependencias=dependencias, publicado_em=agora(), erro=None, **resultado)
            _evento(meta, "Ouro", execucao_id, competencia, None, "agregacao", "sucesso", inicio,
                    duracao_s=round(time.monotonic() - relogio, 3), **resultado)
        except Exception as erro:
            meta.atualizar("Ouro", competencia, status="erro", erro=str(erro), etapa_erro="agregacao")
            _evento(meta, "Ouro", execucao_id, competencia, None, "agregacao", "erro", inicio, erro=str(erro))
            raise


def executar(meses=4, raiz=None, simular=False, forcar=False, usuario="agendador", _catalogo=None, _reservado=False) -> dict:
    from Scripts.Pipeline.BaixarArquivos import meses_calendario
    solicitadas = meses_calendario(meses)  # valida antes de escrever estado ou consultar a rede
    raiz = raiz_dados(raiz)
    if simular:
        # Simular consulta a fonte, mas não baixa ZIPs nem altera os manifestos.
        return {"status": "simulacao", **catalogar(meses)}
    # A API reserva esta mesma trava antes do HTTP 202 e a libera na thread.
    with (nullcontext() if _reservado else trava_arquivo(raiz / ".pipeline.lock", bloquear=False)):
        meta = Metadados(raiz)
        meta.estado(meses=meses, executando=True, manutencao_ativa=True,
                    catalogo_pendente=_catalogo is None)
        execucao_id, spark_ref = uuid4().hex, []
        _iniciar_etapa(meta, "catalogo")
        status_final = "interrompido"
        # Um identificador acompanha todos os arquivos/etapas da mesma execução.
        concluidas = []
        try:
            plano = _catalogo if _catalogo is not None else catalogar(meses)
            estado_anterior = meta.estado()
            # Guardar o catálogo conhecido para auditoria não autoriza executar
            # suas competências antigas. A janela atual também vale na manutenção.
            conhecidos = dict((estado_anterior.get("catalogo") or {}).get("arquivos", {}))
            conhecidos.update(plano["arquivos"])
            elegiveis_antigas = {m for m in estado_anterior.get("pendentes", [])
                                if m in solicitadas and m in conhecidos}
            competencias = sorted({m for m in plano["competencias"] if m in solicitadas} | elegiveis_antigas)
            pendentes = set(estado_anterior.get("pendentes", [])) | set(competencias)
            fora_intervalo = sorted(m for m in pendentes if m not in solicitadas)
            catalogo = {**plano, "arquivos": conhecidos, "competencias": competencias}
            plano = catalogo
            meta.estado(catalogo=catalogo, catalogo_pendente=False,
                        competencias_solicitadas=solicitadas, pendentes=sorted(pendentes),
                        pendentes_fora_intervalo=fora_intervalo)
            for competencia in plano["competencias"]:
                # O for é propositalmente sequencial: um mês inteiro antes do próximo.
                LOG.info("Processando competência %s", competencia)
                processar_competencia(competencia, plano["arquivos"][competencia], raiz, meta, execucao_id, usuario, forcar, spark_ref)
                concluidas.append(competencia)
                pendentes.discard(competencia)
                meta.estado(pendentes=sorted(pendentes))
            resultado = {"status": "sucesso", "execucao_id": execucao_id, "competencias": concluidas,
                         "indisponiveis": plano["indisponiveis"], "pendentes_fora_intervalo": fora_intervalo}
            meta.evento("Raw", {"execucao_id": execucao_id, "etapa": "pipeline", "status": "sucesso", "fim": agora()})
            # Pendências fora da janela ficam registradas, sem manter um job
            # tentando baixá-las. Voltam a ser elegíveis quando forem solicitadas.
            meta.estado(manutencao_ativa=False, ultimo_resultado=resultado, pendentes=sorted(pendentes))
            status_final = "concluido"
            return resultado
        except Exception as erro:
            status_final = "erro"
            # Retornar erro mantém a manutenção ativa. A lista persistida de
            # pendências e os estados por arquivo permitem continuar depois.
            resultado = {"status": "erro", "execucao_id": execucao_id, "competencias": concluidas, "erro": str(erro)}
            meta.estado(manutencao_ativa=True, ultimo_resultado=resultado)
            meta.evento("Raw", {"execucao_id": execucao_id, "etapa": "pipeline", "status": "erro", "fim": agora(), "erro": str(erro)})
            LOG.error("Execução %s interrompida (%s); a próxima execução retomará as pendências", execucao_id, type(erro).__name__)
            return resultado
        finally:
            # Libera a JVM mesmo quando uma etapa falha.
            if spark_ref:
                spark_ref[0].stop()
            meta.estado(executando=False, etapa_atual=status_final, competencia_atual=None,
                        arquivo_atual=None, etapa_inicio=None)


def reativar(raiz=None) -> dict:
    # Altera apenas a flag persistida; não inicia uma importação imediatamente.
    # O próximo ciclo do agendador, ou o comando manutencao, executará as pendências.
    return Metadados(raiz_dados(raiz)).estado(manutencao_ativa=True)


def manutencao(raiz=None) -> dict:
    raiz = raiz_dados(raiz)
    meta = Metadados(raiz)
    if not meta.estado().get("manutencao_ativa", False):
        return {"status": "inativo"}
    # Reutilizar versões conhecidas para retomar falhas locais. Se a própria
    # consulta falhou, repeti-la em 30 minutos, sem esperar os próximos 15 dias.
    estado = meta.estado()
    catalogo = None if estado.get("catalogo_pendente") else estado.get("catalogo")
    return executar(estado.get("meses", 4), raiz, _catalogo=catalogo)


def iniciar(raiz=None):
    raiz = raiz_dados(raiz)
    parar = Event()
    def rotina():
        # monotonic mede intervalos sem sofrer ajuste do relógio do sistema.
        # O prazo de 15 dias usa data UTC persistida para sobreviver a reinícios.
        proxima_manutencao = time.monotonic() + 1800
        while not parar.is_set():
            meta = Metadados(raiz)
            estado = meta.estado()
            proxima = estado.get("proxima_verificacao")
            agora_dt = datetime.now(timezone.utc)
            try:
                if not proxima or agora_dt >= datetime.fromisoformat(proxima):
                    executar(int(os.getenv("MESES", estado.get("meses", 4))), raiz)
                    meta.estado(proxima_verificacao=(datetime.now(timezone.utc) + timedelta(days=15)).isoformat())
                elif time.monotonic() >= proxima_manutencao:
                    manutencao(raiz)
                    proxima_manutencao = time.monotonic() + 1800
            except Exception:
                LOG.exception("Falha na rotina; estado preservado para nova tentativa")
            parar.wait(30)
            # Event.wait permite encerrar o agendador quando a API é desligada.
    thread = Thread(target=rotina, name="agendador-cnpj", daemon=True)
    thread.start()
    return parar, thread
