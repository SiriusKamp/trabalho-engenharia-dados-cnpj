"""API de arquivos e operação: leituras usam apenas disco e manifestos, sem Spark."""

from contextlib import asynccontextmanager
import importlib
import logging
import math
import os
from pathlib import Path
import re
import stat
import threading
from time import monotonic
from urllib.parse import quote

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from Scripts.Seguranca import (
    CAMADAS, Identidade, abrir_download, autorizar_camada, caminho_download,
    criar_autenticacao, diretorio_camada, exigir_perfil, nome_publico,
)


_FALHAS = {"erro", "falha", "error", "failed"}
# O Grafana recebe métricas operacionais, não cadastros de empresas/sócios.
# As rotas de arquivos têm autenticação e autorização por camada separadas.
_PENDENTES = _FALHAS | {"pendente", "processando", "em_andamento", "baixando", "iniciado"}
_ERRO_PUBLICO = "Falha registrada; consulte os registros operacionais restritos."
_LOG = logging.getLogger(__name__)


def _numero(valor):
    # Higieniza somente métricas operacionais para não enviar NaN/inf ao Grafana.
    if isinstance(valor, (int, float)) and not isinstance(valor, bool) and math.isfinite(valor):
        return max(0, valor)
    return 0


def _status(registro):
    return str(registro.get("status") or registro.get("estado") or "").lower()


def _estado_publico(estado):
    ultimo = estado.get("ultimo_resultado")
    # Resultados do executor podem conter mensagens de erro: expõe apenas o estado operacional.
    if isinstance(ultimo, dict):
        ultimo = {chave: ultimo[chave] for chave in ("status", "meses", "competencias", "pendencias")
                  if chave in ultimo and isinstance(ultimo[chave], (str, int, float, bool))}
    elif ultimo not in (None, "sucesso", "erro", "falha", "ocupado", "ok", "pendente"):
        ultimo = "registrado"
    return {"meses": estado.get("meses", 4),
            "manutencao_ativa": bool(estado.get("manutencao_ativa", False)),
            "proxima_verificacao": estado.get("proxima_verificacao"), "ultimo_resultado": ultimo,
            **{campo: estado.get(campo) if isinstance(estado.get(campo), str) else None
               for campo in ("etapa_atual", "competencia_atual", "arquivo_atual", "etapa_inicio")}}


def _ler_manifesto(raiz, camada):
    # A consulta depende dos JSONs de controle; não abre uma sessão Spark.
    from Scripts.Pipeline.Metadados import Metadados

    pasta = diretorio_camada(raiz, camada)
    manifesto = pasta / "Metadados.json"
    if manifesto.is_symlink() or (hasattr(manifesto, "is_junction") and manifesto.is_junction()):
        raise HTTPException(403, "Manifesto fora do diretório permitido")
    try:
        return Metadados(raiz).ler(camada)
    except (OSError, ValueError):
        raise HTTPException(503, "Metadados temporariamente indisponíveis") from None


def _arquivos_fisicos(raiz, camada):
    # Medir bytes dos arquivos presentes dá a ocupação atual. Raw/Bronze já
    # descartadas continuam no histórico, mas deixam de ocupar esses bytes.
    pasta = diretorio_camada(raiz, camada)
    if not pasta.exists():
        # Uma instalação nova pode consultar o Grafana antes da primeira carga.
        return
    if hasattr(os, "fwalk"):
        # Linux/Docker: o descritor prende cada diretório durante a medição.
        # stat relativo evita resolver e validar o caminho inteiro para cada
        # Parquet. Links não são seguidos, nem mesmo para medir seus bytes.
        for diretorio, subpastas, nomes, descritor in os.fwalk(pasta, follow_symlinks=False):
            subpastas[:] = [nome for nome in subpastas if nome_publico(nome)]
            for nome in nomes:
                if not nome_publico(nome):
                    continue
                try:
                    atributos = os.stat(nome, dir_fd=descritor, follow_symlinks=False)
                    if stat.S_ISREG(atributos.st_mode):
                        relativo = (Path(diretorio) / nome).relative_to(pasta).as_posix()
                        yield relativo, atributos.st_size
                except OSError:
                    # O pipeline pode publicar ou remover um arquivo neste instante.
                    continue
        return
    # Windows nativo: mantém a validação de junctions e caminhos da API.
    for diretorio, subpastas, nomes in os.walk(pasta, followlinks=False):
        subpastas[:] = [nome for nome in subpastas if nome_publico(nome)
                       and not (Path(diretorio) / nome).is_symlink()
                       and not (hasattr(Path, "is_junction") and (Path(diretorio) / nome).is_junction())]
        for nome in nomes:
            if not nome_publico(nome):
                continue
            relativo = (Path(diretorio) / nome).relative_to(pasta).as_posix()
            try:
                caminho = caminho_download(raiz, camada, relativo)
                if caminho.is_file():
                    yield relativo, caminho.stat().st_size
            except (HTTPException, OSError):
                # Publicações/descarte podem ocorrer durante a leitura; links nunca são seguidos.
                continue


def _da_competencia(chave, registro, competencia):
    if competencia is None:
        return True
    partes = chave.replace("\\", "/").split("/")
    return (registro.get("competencia") == competencia or competencia in partes
            or "competencia=" + competencia in partes)


def _eventos(manifestos):
    # A lista explícita de campos evita expor ao monitor mensagens livres que
    # eventualmente contenham dados de cadastro ou detalhes internos da falha.
    campos = ("execucao_id", "competencia", "arquivo", "etapa", "status", "inicio", "fim",
              "duracao_s", "bytes", "linhas", "duplicidades", "erros_qualidade")
    for camada, manifesto in manifestos.items():
        for evento in manifesto.get("execucoes", []):
            if not isinstance(evento, dict):
                continue
            publico = {campo: evento.get(campo) for campo in campos
                       if not isinstance(evento.get(campo), (dict, list))}
            if isinstance(evento.get("linhas"), dict):
                publico["linhas"] = sum(_numero(valor) for valor in evento["linhas"].values())
            publico["camada"] = camada
            publico["erro"] = _ERRO_PUBLICO if evento.get("erro") else None
            yield publico


def _resumo(manifestos, eventos, ocupacao):
    # Há duas visões: estado atual (pendências/falhas ainda abertas) e histórico
    # acumulado (tentativas, bytes e durações, inclusive de falhas já resolvidas).
    pendencias, falhas_atuais = set(), set()
    arquivos = {}
    for camada, manifesto in manifestos.items():
        for chave, registro in manifesto.get("arquivos", {}).items():
            arquivos[(camada, chave)] = registro
            if _status(registro) in _PENDENTES:
                pendencias.add((camada, chave))
            if _status(registro) in _FALHAS:
                falhas_atuais.add((camada, chave))
    ultimos = {}
    # Sobrescrever pela mesma chave mantém o último evento de cada etapa/origem.
    for evento in eventos:
        chave = (evento["camada"], evento.get("competencia"), evento.get("arquivo"), evento.get("etapa"))
        ultimos[chave] = evento
    for (camada, competencia, arquivo, _), evento in ultimos.items():
        chave = f"{competencia}/{arquivo}" if arquivo else f"{competencia}/execucao"
        # O estado atual do manifesto prevalece sobre uma falha histórica já resolvida.
        if (camada, chave) not in arquivos and _status(evento) in _PENDENTES:
            pendencias.add((camada, chave))
            if _status(evento) in _FALHAS:
                falhas_atuais.add((camada, chave))
    duracoes = [_numero(e["duracao_s"]) for e in eventos if e.get("duracao_s") is not None]
    etapas = sorted({str(e["etapa"]) for e in eventos if e.get("etapa")})
    por_etapa = []
    for etapa in etapas:
        lote = [e for e in eventos if e.get("etapa") == etapa]
        tempos = [_numero(e["duracao_s"]) for e in lote if e.get("duracao_s") is not None]
        por_etapa.append({"etapa": etapa, "eventos": len(lote),
                          "falhas": sum(_status(e) in _FALHAS for e in lote),
                          "duracao_media_s": sum(tempos) / len(tempos) if tempos else 0})
    extracoes = [e["fim"] for e in eventos if e.get("fim") and
                e.get("etapa") in ("extracao", "extrair", "extração") and _status(e) not in _FALHAS]
    importacoes = {}
    # execucao_id agrupa os arquivos de uma mesma chamada de importar. Assim
    # o dashboard mostra quantas baixas aquela importação efetivamente realizou.
    for evento in eventos:
        execucao_id = evento.get("execucao_id")
        if not execucao_id:
            continue
        carga = importacoes.setdefault(execucao_id, {"execucao_id": execucao_id, "downloads": 0,
                                                      "bytes_transferidos": 0, "falhas": 0, "duracao_s": 0})
        carga["falhas"] += int(_status(evento) in _FALHAS)
        carga["duracao_s"] += _numero(evento.get("duracao_s"))
        if evento.get("etapa") == "download" and _status(evento) not in _FALHAS:
            carga["downloads"] += 1
            carga["bytes_transferidos"] += _numero(evento.get("bytes"))
    return {
        "status": "falha" if falhas_atuais else "pendente" if pendencias else "ok",
        "falhas": sum(_status(e) in _FALHAS for e in eventos),
        "falhas_atuais": len(falhas_atuais), "pendencias": len(pendencias),
        "bytes_camadas": ocupacao, "bytes_total": sum(ocupacao.values()),
        "bytes_historicos": sum(_numero(e.get("bytes")) for e in eventos),
        "bytes_transferidos": sum(_numero(e.get("bytes")) for e in eventos if e.get("etapa") == "download"),
        "bytes_historicos_camadas": {c: sum(_numero(e.get("bytes")) for e in eventos if e["camada"] == c)
                                    for c in CAMADAS},
        "execucoes_total": len({e["execucao_id"] for e in eventos if e.get("execucao_id")}),
        "eventos_total": len(eventos), "duracao_media_s": sum(duracoes) / len(duracoes) if duracoes else 0,
        "duracao_total_s": sum(duracoes), "etapas": por_etapa,
        "downloads_total": sum(e.get("etapa") == "download" and _status(e) not in _FALHAS for e in eventos),
        "linhas_total": sum(_numero(e.get("linhas")) for e in eventos),
        "linhas_camadas": {c: sum(_numero(e.get("linhas")) for e in eventos if e["camada"] == c) for c in CAMADAS},
        "duplicidades": sum(_numero(e.get("duplicidades")) for e in eventos),
        "erros_qualidade": sum(_numero(e.get("erros_qualidade")) for e in eventos),
        "ultima_extracao": max(extracoes, default=None),
        "importacoes": list(importacoes.values()),
        "estado": _estado_publico(manifestos["Raw"].get("estado", {})),
    }


def criar_app(raiz=None, somente_metricas=False):
    """raiz aponta diretamente para DATA; nenhum acesso HTTP cria sessão Spark."""
    raiz = Path(raiz) if raiz is not None else Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "DATA"))
    raiz = raiz.absolute()
    autenticar = criar_autenticacao()
    agendamento_ativo = not somente_metricas and os.environ.get("AGENDAMENTO_ATIVO", "0") == "1"
    ocupacao_cache, ocupacao_ate = None, 0
    ocupacao_lock = threading.Lock()

    @asynccontextmanager
    async def lifespan(app):
        # lifespan acompanha a vida da API: inicia a rotina opcional ao subir
        # e solicita seu encerramento quando o servidor termina.
        parada = thread = None
        if agendamento_ativo:
            agendador = importlib.import_module("Scripts.Agendador")
            parada, thread = agendador.iniciar(raiz)
        try:
            yield
        finally:
            if parada is not None:
                parada.set()
                thread.join(timeout=5)

    app = FastAPI(title="CNPJ — operação e arquivos", lifespan=lifespan)
    app.state.raiz = raiz
    app.state.operacao = {"status": "inativo"}

    def monitor(identidade: Identidade = Depends(autenticar)):
        # Depends primeiro autentica; em seguida esta função exige a role da rota.
        exigir_perfil(identidade, "monitoramento")
        return identidade

    def engenharia(identidade: Identidade = Depends(autenticar)):
        exigir_perfil(identidade, "engenharia")
        return identidade

    @app.get("/saude")
    def saude():
        # Verificação pública e leve usada pelo healthcheck do Docker Compose.
        return {"status": "ok"}

    @app.get("/metricas/resumo", dependencies=[Depends(monitor)])
    def resumo():
        nonlocal ocupacao_cache, ocupacao_ate
        manifestos = {c: _ler_manifesto(raiz, c) for c in CAMADAS}
        # Somente a medição de espaço tem cache. Falhas, eventos e etapa atual
        # continuam sendo lidos dos JSONs em cada consulta. O lock faz os 15
        # painéis compartilharem uma varredura, inclusive na primeira chamada.
        with ocupacao_lock:
            if ocupacao_cache is None or monotonic() >= ocupacao_ate:
                ocupacao_cache = {c: sum(n for _, n in _arquivos_fisicos(raiz, c)) for c in CAMADAS}
                ocupacao_ate = monotonic() + 30
            ocupacao = dict(ocupacao_cache)
        return _resumo(manifestos, list(_eventos(manifestos)), ocupacao)

    @app.get("/metricas/execucoes", dependencies=[Depends(monitor)])
    def execucoes():
        return list(_eventos({c: _ler_manifesto(raiz, c) for c in CAMADAS}))

    if somente_metricas:
        # O serviço do Grafana não tem endpoints de download ou operação e pode
        # ser recriado separadamente, com DATA montado apenas para leitura.
        return app

    @app.get("/camadas/{camada}")
    def listar(camada: str, competencia: str | None = None, identidade: Identidade = Depends(autenticar)):
        autorizar_camada(identidade, camada)
        if competencia is not None and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", competencia):
            raise HTTPException(422, "Competência deve usar AAAA-MM")
        manifesto = _ler_manifesto(raiz, camada)
        fisicos = dict(_arquivos_fisicos(raiz, camada))
        arquivos = {}
        for chave, registro in manifesto.get("arquivos", {}).items():
            if _da_competencia(chave, registro, competencia):
                caminho = registro.get("caminho", chave)
                # Metadados de origens descartadas permanecem visíveis; não prometem um download.
                disponivel = chave in fisicos
                if isinstance(caminho, str):
                    relativo = caminho.replace("\\", "/").removeprefix(camada + "/")
                    disponivel = disponivel or relativo in fisicos or any(
                        nome.startswith(relativo.rstrip("/") + "/") for nome in fisicos)
                arquivos[chave] = {**registro, "disponivel": disponivel}
        for chave, tamanho in fisicos.items():
            if chave not in arquivos and _da_competencia(chave, {}, competencia):
                arquivos[chave] = {"bytes": tamanho, "disponivel": True}
        return {"camada": camada, "competencia": competencia, "arquivos": arquivos,
                "estado": _estado_publico(manifesto.get("estado", {})) if camada == "Raw" else {}}

    @app.get("/arquivos/{camada}/{arquivo:path}")
    def download(camada: str, arquivo: str, identidade: Identidade = Depends(autenticar)):
        autorizar_camada(identidade, camada)
        stream, tamanho = abrir_download(raiz, camada, arquivo)
        def blocos():
            # Streaming envia o arquivo aos poucos em vez de carregá-lo inteiro na RAM.
            try:
                while bloco := stream.read(1024 * 1024):
                    yield bloco
            finally:
                stream.close()
        return StreamingResponse(blocos(), media_type="application/octet-stream", headers={
            "Content-Length": str(tamanho), "X-Content-Type-Options": "nosniff",
            "Content-Disposition": "attachment; filename*=UTF-8''" + quote(arquivo.split("/")[-1], safe=""),
        })

    @app.post("/operacao/reativar", dependencies=[Depends(engenharia)])
    def reativar():
        return importlib.import_module("Scripts.Agendador").reativar(raiz)

    @app.get("/operacao/pendentes", dependencies=[Depends(engenharia)])
    def pendentes(meses: int | None = Query(default=None, gt=0)):
        # Inclui origens ainda não iniciadas, usando o catálogo já conhecido.
        # A consulta apenas informa elegibilidade; não inicia downloads/retomada.
        from Scripts.Pipeline.Pendencias import listar_pendencias
        manifestos = {camada: _ler_manifesto(raiz, camada) for camada in CAMADAS}
        return listar_pendencias(raiz, manifestos, meses)

    @app.post("/operacao/importar", status_code=202)
    def importar(meses: int = Query(default=4, gt=0), identidade: Identidade = Depends(engenharia)):
        from Scripts.Pipeline.Metadados import trava_arquivo
        agendador = importlib.import_module("Scripts.Agendador")
        reserva = trava_arquivo(raiz / ".pipeline.lock", bloquear=False)
        # Reservar antes do 202 garante que uma chamada aceita não perderá
        # a disputa para a CLI. __enter__ abre a seção protegida explicitamente.
        try:
            reserva.__enter__()
        except RuntimeError:
            raise HTTPException(409, "Pipeline ocupado por outra execução") from None
        try:
            executar = agendador.executar
            app.state.operacao = {"status": "em_andamento", "meses": meses}
            def trabalho():
                # A carga pode levar horas. A thread permite devolver o HTTP 202
                # rapidamente; a auditoria da carga fica persistida nos manifestos.
                try:
                    resultado = executar(meses, raiz, usuario=identidade.usuario, _reservado=True)
                    app.state.operacao = {"status": resultado.get("status", "concluido") if isinstance(resultado, dict) else "concluido"}
                except Exception:
                    app.state.operacao = {"status": "erro"}
                    _LOG.error("Falha na importação em segundo plano; consulte os metadados restritos.")
                finally:
                    # A thread passa a ser responsável por liberar a reserva.
                    reserva.__exit__(None, None, None)
            threading.Thread(target=trabalho, name="cnpj-importacao", daemon=True).start()
        except BaseException:
            reserva.__exit__(None, None, None)
            raise
        return {"status": "aceito", "meses": meses, "usuario": identidade.usuario}

    return app
