"""Dimensões e três fatos mensais, publicados juntos após validação."""

from contextlib import contextmanager
import logging
import shutil
import tempfile
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from Scripts.Pipeline.Esquemas import IDADES_FAIXAS
from Scripts.Pipeline.Metadados import agora, publicar_diretorio
from Scripts.Pipeline.PipelinePrata import (
    bytes_diretorio, deduplicar, ler_tabela, preservar_insercao,
    reconciliar_publicacao, validar_competencia,
)


LOG = logging.getLogger("cnpj")


@contextmanager
def _medir(etapa, competencia, produto):
    """Mede ações reais; registra só etapa, competência e nome técnico do produto."""
    inicio = time.monotonic()
    LOG.info("Ouro %s inicio: competencia=%s produto=%s", etapa, competencia, produto)
    status = "erro"
    try:
        yield
        status = "sucesso"
    finally:
        # Não registrar linhas, chaves, usuário ou texto da exceção: podem conter PII.
        LOG.info("Ouro %s fim: competencia=%s produto=%s status=%s duracao_s=%.3f",
                 etapa, competencia, produto, status, time.monotonic() - inicio)


def _validar_estagio(tabela, chaves, nome):
    """Confere o Parquet gravado e devolve sua contagem em uma única ação Spark."""
    chave_nula = F.lit(False)
    for campo in chaves:
        chave_nula = chave_nula | F.col(campo).isNull()
    # O primeiro agrupamento já verifica unicidade. Sobre seus grupos calculamos
    # contagem e indicadores juntos, sem três leituras/ações independentes.
    grupos = tabela.groupBy(*chaves).agg(F.count("*").alias("_ocorrencias"))
    estatisticas = grupos.agg(
        F.coalesce(F.sum("_ocorrencias"), F.lit(0)).alias("linhas"),
        F.coalesce(F.max(chave_nula.cast("int")), F.lit(0)).alias("chave_nula"),
        F.coalesce(F.max((F.col("_ocorrencias") > 1).cast("int")), F.lit(0)).alias("chave_duplicada"),
    ).first()
    # Um estágio vazio é válido; nulos continuam tendo precedência sobre duplicatas.
    if estatisticas.chave_nula:
        raise ValueError(f"Chave nula no produto {nome}")
    if estatisticas.chave_duplicada:
        raise ValueError(f"Chave duplicada no produto {nome}")
    return estatisticas.linhas


def _produtos(empresas, estabelecimentos, socios, competencia, persistidos):
    # Fatos guardam medidas e chaves; dimensões guardam nomes/descrições.
    # Incluir competência nos joins mantém o vínculo com o retrato mensal correto.
    chaves = ["cnpj_basico", "competencia"]
    referencia = F.last_day(F.to_date(F.lit(competencia + "-01")))
    # Uma carga histórica usa o fim daquele mês como referência, não o dia da execução.
    matriz_valida = (F.col("identificador_matriz_filial").isin("1", "01")
                     & (F.col("data_inicio_atividade") <= referencia))
    # Agregar cada relação 1:N ANTES dos joins impede o produto filiais x sócios.
    estabelecimentos_empresa = estabelecimentos.groupBy(*chaves).agg(
        F.count("*").alias("quantidade_estabelecimentos"),
        F.sum(F.when(F.col("situacao_cadastral").isin("02", "2"), 1).otherwise(0)).alias("estabelecimentos_ativos"),
        F.min(F.when(matriz_valida, F.col("data_inicio_atividade"))).alias("_abertura_matriz"),
    )
    idades = F.create_map(*[F.lit(valor) for codigo, idade in IDADES_FAIXAS.items() for valor in (codigo, idade)])
    # Apenas sócios PF recebem idade estimada. avg ignora null, portanto idade
    # desconhecida e pessoa jurídica não entram como zero na média.
    idade_pf = F.when(F.col("identificador_socio") == "2", F.element_at(idades, F.col("faixa_etaria")))
    socios_empresa = socios.groupBy(*chaves).agg(
        F.count("*").alias("quantidade_socios"),
        F.avg(idade_pf).alias("idade_media_estimada_socios_pf"),
    )
    base = (empresas.select(*chaves, "capital_social")
            # left mantém empresas mesmo quando não há observação de uma relação.
            .join(estabelecimentos_empresa, chaves, "left")
            .join(socios_empresa, chaves, "left"))
    # Os writes de resumo e perfil são ações separadas. Persistir esta base
    # estreita evita repetir seus agrupamentos/joins; o chamador libera no finally.
    base = base.persist(StorageLevel.DISK_ONLY)
    persistidos.append(base)
    resumo = base.select(*chaves, "quantidade_estabelecimentos", "estabelecimentos_ativos",
                         "quantidade_socios", "capital_social")
    perfil = base.select(
        *chaves,
        F.floor(F.months_between(referencia, F.col("_abertura_matriz"), False))
        .cast("int").alias("meses_desde_abertura_matriz"),
        "idade_media_estimada_socios_pf", "capital_social",
    )
    multiplos = (socios.groupBy("socio_id", "competencia")
                # ler_tabela já garante um vínculo por empresa/sócio/competência.
                # Assim count(*) equivale a countDistinct(cnpj_basico), sem outro
                # agrupamento distinto. O contrato entre origens é testado.
                .agg(F.count("*").alias("quantidade_empresas"))
                .where(F.col("quantidade_empresas") >= 2))
    dim_empresa = empresas.select(
        *chaves, "razao_social", "natureza_juridica", "qualificacao_responsavel",
        "porte_empresa", "ente_federativo_responsavel",
    )
    # A mesma pessoa pode ter atributos em vários vínculos; uma linha por dimensão.
    dim_socio = deduplicar(socios, "socios", ("socio_id", "competencia")).select(
        "socio_id", "competencia", "identificador_socio", "documento_mascarado",
        "nome_socio", "nome_normalizado", "pais", "faixa_etaria", "identificacao_aproximada",
    )
    return {
        "dim_empresa": (dim_empresa, chaves),
        "dim_socio": (dim_socio, ["socio_id", "competencia"]),
        "fato_resumo_empresarial": (resumo, chaves),
        "fato_socios_multiplas_empresas": (multiplos, ["socio_id", "competencia"]),
        "fato_perfil_empresarial": (perfil, chaves),
    }


def processar_ouro(spark, raiz: Path, competencia: str, execucao_id: str, usuario: str) -> dict:
    """Substitui somente a competência pedida, com cinco produtos inseparáveis.

    Capital tem uma observação por empresa/mês; médias por conjunto são derivadas
    pelo consumo sobre os fatos, sem ponderação por filiais ou vínculos societários.
    Métricas sem observação permanecem nulas; zero é preservado quando conhecido.
    """
    validar_competencia(competencia)
    raiz = Path(raiz)
    destino = raiz / "Ouro" / f"competencia={competencia}"
    destino.parent.mkdir(parents=True, exist_ok=True)
    reconciliar_publicacao(destino)
    staging = Path(tempfile.mkdtemp(prefix=f".{destino.name}.staging-", dir=destino.parent))
    # Os cinco produtos ficam juntos num staging do mês. Uma falha em qualquer
    # produto impede substituir o conjunto válido que o usuário já consultava.
    tabelas = []
    persistidos = []
    try:
        # Ausência de uma entidade é erro; um snapshot existente sem linhas é válido.
        for entidade in ("empresas", "estabelecimentos", "socios"):
            tabela = ler_tabela(spark, raiz, entidade, competencia)
            # Projetar DEPOIS da deduplicação global: campos descartados ainda
            # precisam participar da qualidade e do desempate entre origens.
            if entidade == "estabelecimentos":
                tabela = tabela.select("cnpj_basico", "competencia", "identificador_matriz_filial",
                                       "situacao_cadastral", "data_inicio_atividade")
            elif entidade == "empresas":
                tabela = tabela.select("cnpj_basico", "competencia", "capital_social", "razao_social",
                                       "natureza_juridica", "qualificacao_responsavel", "porte_empresa",
                                       "ente_federativo_responsavel")
            else:
                # dim_socio ainda usa todos os campos de negócio, origem e inserção
                # para escolher atributos entre vínculos. Só auditoria refeita sai.
                tabela = tabela.drop("data_atualizacao", "atualizado_por", "execucao_id")
            tabela = tabela.persist(StorageLevel.DISK_ONLY)
            tabelas.append(tabela)
            persistidos.append(tabela)
        instante = F.to_timestamp(F.lit(agora()))
        contagens = {}
        for nome, (tabela, chaves) in _produtos(*tabelas, competencia, persistidos=persistidos).items():
            with _medir("produto", competencia, nome):
                # Auditoria também na Ouro permite rastrear quem e qual execução
                # produziram os indicadores, além da competência dos dados.
                tabela = (tabela.withColumn("arquivo_origem", F.lit(f"Prata/competencia={competencia}"))
                          .withColumn("data_insercao", instante)
                          .withColumn("data_atualizacao", instante)
                          .withColumn("atualizado_por", F.lit(usuario))
                          .withColumn("execucao_id", F.lit(execucao_id)))
                anterior = destino / nome
                if anterior.exists():
                    tabela = preservar_insercao(tabela, spark.read.parquet(str(anterior)), chaves)
                pasta = staging / nome
                tabela.write.mode("overwrite").parquet(str(pasta))
                with _medir("validacao", competencia, nome):
                    contagens[nome] = _validar_estagio(spark.read.parquet(str(pasta)), chaves, nome)
        tamanho = bytes_diretorio(staging)
        # Só uma troca de diretório, depois de todos os produtos escritos e lidos.
        publicar_diretorio(staging, destino)
        return {"caminho": destino.relative_to(raiz).as_posix(), "linhas": contagens, "bytes": tamanho}
    finally:
        for tabela in reversed(persistidos):
            tabela.unpersist()
        if staging.exists():
            shutil.rmtree(staging)
