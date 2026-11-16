"""CSV oficial para snapshots Parquet por competência e origem lógica do ZIP."""

import os
import re
import shutil
import tempfile
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from Scripts.Pipeline.Esquemas import CAMPOS_NOME, CHAVES, COLUNAS, esquema_csv, esquema_prata
from Scripts.Pipeline.Metadados import agora, publicar_diretorio

# DataFrames Spark descrevem operações que só são executadas quando uma ação
# como count() ou write é chamada. F.col/F.when geram expressões para o Spark;
# não percorremos cada registro com um for de Python.

def criar_spark() -> SparkSession:
    """Sessão local; temporários devem estar em volume criptografado no servidor."""
    temporarios = Path(os.environ.get("SPARK_LOCAL_DIRS", str(Path(tempfile.gettempdir()) / "cnpj-spark")).split(",")[0])
    temporarios.mkdir(parents=True, exist_ok=True)
    # local[2] usa duas threads de execução neste computador, sem cluster externo.
    # Memória da JVM e limite total do container são configurações diferentes.
    return (
        SparkSession.builder.appName("CNPJ")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
        # Criptografia de comunicação e arquivos temporários do Spark. O Parquet
        # final depende do volume criptografado do host, conforme a documentação.
        .config("spark.authenticate", "true")
        .config("spark.network.crypto.enabled", "true")
        .config("spark.io.encryption.enabled", "true")
        .config("spark.local.dir", str(temporarios))
        .config("spark.sql.warehouse.dir", (temporarios / "warehouse").resolve().as_uri())
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        # Partições de shuffle dividem trabalho de joins/agrupamentos; muitas
        # partições em um notebook pequeno aumentam o custo de agendamento.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def validar_competencia(competencia: str) -> None:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", competencia) or competencia.startswith("0000-"):
        raise ValueError("Competência deve estar no formato YYYY-MM")


def reconciliar_publicacao(destino: Path) -> None:
    """Usa a mesma convenção .anterior do publicador antes de ler a versão válida.

    A execução tem um único escritor. Se a troca parou entre os dois renames,
    restaurar primeiro evita perder datas de inserção ou publicar Ouro parcial.
    """
    backup = destino.with_name(destino.name + ".anterior")
    if backup.exists():
        if not destino.exists():
            backup.rename(destino)
        else:
            shutil.rmtree(backup)


def bytes_diretorio(diretorio: Path) -> int:
    return sum(arquivo.stat().st_size for arquivo in diretorio.rglob("*") if arquivo.is_file())


def _diretorio_competencia(raiz: Path, entidade: str, competencia: str) -> Path:
    if entidade not in COLUNAS:
        raise ValueError(f"Entidade fora do escopo: {entidade}")
    validar_competencia(competencia)
    return Path(raiz) / "Prata" / entidade / f"competencia={competencia}"


def _fragmentos(diretorio: Path) -> list[Path]:
    # Cada fragmento corresponde a um ZIP de origem. Pastas temporárias e backups
    # ficam fora da leitura lógica para não misturar dados incompletos/antigos.
    if not diretorio.exists():
        return []
    for backup in diretorio.glob("*.anterior"):
        if backup.is_dir() and not backup.name.startswith("."):
            reconciliar_publicacao(backup.with_name(backup.name.removesuffix(".anterior")))
    return sorted(pasta for pasta in diretorio.iterdir()
                  if pasta.is_dir() and not pasta.name.startswith((".", "_"))
                  and not pasta.name.endswith(".anterior"))


def deduplicar(tabela: DataFrame, entidade: str, chaves=None) -> DataFrame:
    """Qualidade precede desempate estável; capital maior não indica qualidade."""
    chaves = list(chaves or CHAVES[entidade])
    negocios = [nome for nome in COLUNAS[entidade] if nome in tabela.columns]
    nulos = sum(F.col(nome).isNull().cast("int") for nome in negocios)
    # True vale 1 após cast: somar estes indicadores conta campos de negócio vazios.
    ordem = [F.col(CAMPOS_NOME[entidade]).isNotNull().desc()]
    if entidade == "empresas":
        ordem.append(F.col("capital_social").isNotNull().desc())
    ordem.extend([
        nulos.asc(),
        F.sha2(F.to_json(F.struct(*[F.col(nome) for nome in negocios]), {"ignoreNullFields": "false"}), 256).asc(),
        F.col("arquivo_origem").asc_nulls_last(),
    ])
    janela = Window.partitionBy(*chaves)
    # A janela reúne candidatos da mesma chave. row_number escolhe o primeiro
    # na ordem de qualidade; o hash dá um desempate estável entre execuções.
    return (
        tabela.withColumn("data_insercao", F.min("data_insercao").over(janela))
        .withColumn("_posicao", F.row_number().over(janela.orderBy(*ordem)))
        .where(F.col("_posicao") == 1).drop("_posicao")
    )


def ler_tabela(spark, raiz: Path, entidade: str, competencia: str) -> DataFrame:
    """Consolida candidatos entre origens sem apagá-los dos snapshots físicos."""
    diretorio = _diretorio_competencia(raiz, entidade, competencia)
    fragmentos = _fragmentos(diretorio)
    if not fragmentos:
        raise FileNotFoundError(f"Prata ausente: {entidade}/{competencia}")
    # Ler todos os fragmentos antes de deduplicar resolve duplicatas entre ZIPs,
    # preservando fisicamente candidatos necessários para correções futuras.
    tabela = (spark.read.schema(esquema_prata(entidade))
              .option("recursiveFileLookup", "true")
              .parquet(*[str(pasta) for pasta in fragmentos]))
    return deduplicar(tabela.where(F.col("competencia") == competencia), entidade)


def _normalizar(bronze: DataFrame, entidade: str) -> DataFrame:
    # Espaços são removidos das pontas; texto vazio vira null para as regras
    # de qualidade distinguirem ausência de um valor efetivamente informado.
    tabela = bronze.select(*[
        F.when(F.length(F.trim(F.col(nome))) > 0, F.trim(F.col(nome))).alias(nome)
        for nome in COLUNAS[entidade]
    ])
    for nome in COLUNAS[entidade]:
        if nome.startswith("data_"):
            # Datas inválidas ou 00000000 viram null; try_to_timestamp tolera o erro.
            valor = F.when(F.col(nome).rlike(r"^[0-9]{8}$") & (F.col(nome) != "00000000"), F.col(nome))
            tabela = tabela.withColumn(nome, F.to_date(F.try_to_timestamp(valor, F.lit("yyyyMMdd"))))
    if entidade == "empresas":
        # O arquivo oficial usa vírgula decimal; também tolera separador de milhar.
        tabela = tabela.withColumn("capital_social", F.expr(
            "try_cast(replace(case when instr(capital_social, ',') > 0 "
            "then replace(capital_social, '.', '') else capital_social end, ',', '.') as decimal(20,2))"
        ))
    if entidade == "estabelecimentos":
        # As três partes permanecem texto: concatená-las mantém os 14 dígitos e zeros.
        tabela = tabela.withColumn("cnpj_completo", F.concat("cnpj_basico", "cnpj_ordem", "cnpj_dv"))
    if entidade == "socios":
        # A fonte normalmente já mascara CPF; proteja também um CPF íntegro inesperado.
        # CNPJ de pessoa jurídica permanece como publicado, sem tentar inferir CPF.
        for campo in ("documento_mascarado", "representante_legal"):
            digitos = F.regexp_replace(F.col(campo), r"[.\-\s]", "")
            cpf_integro = digitos.rlike(r"^[0-9]{11}$")
            if campo == "documento_mascarado":
                cpf_integro = cpf_integro & (F.col("identificador_socio") == "2")
            tabela = tabela.withColumn(campo, F.when(cpf_integro,
                F.concat(F.lit("***"), F.substring(digitos, 4, 6), F.lit("**"))).otherwise(F.col(campo)))
        tabela = tabela.withColumn("nome_normalizado", F.upper(F.regexp_replace("nome_socio", r"\s+", " ")))
        # Nunca remover '*' ou reconstruir CPF. Sem documento, distinguir país.
        identificacao = F.struct(
            F.col("identificador_socio").alias("tipo"),
            F.col("documento_mascarado").alias("documento"),
            F.col("nome_normalizado").alias("nome"),
            F.when(F.col("documento_mascarado").isNull(), F.col("pais")).alias("pais_sem_documento"),
        )
        tabela = tabela.withColumn("socio_id", F.sha2(F.to_json(identificacao, {"ignoreNullFields": "false"}), 256))
        # O hash é uma chave textual reproduzível. Ele não torna a identificação
        # exata: CPF mascarado + nome ainda pode aproximar pessoas diferentes.
        # Toda identificação societária é aproximada, mesmo com documento mascarado.
        tabela = tabela.withColumn("identificacao_aproximada", F.lit(True))
    return tabela


def preservar_insercao(tabela: DataFrame, anterior: DataFrame, chaves) -> DataFrame:
    """Preserva a primeira observação da chave na competência, entre origens."""
    datas = anterior.groupBy(*chaves).agg(F.min("data_insercao").alias("_insercao_anterior"))
    # coalesce escolhe a data antiga quando a chave já existia; caso contrário,
    # mantém a inserção atual. Isso vale dentro da mesma competência.
    return (tabela.join(datas, list(chaves), "left")
            .withColumn("data_insercao", F.coalesce("_insercao_anterior", "data_insercao"))
            .drop("_insercao_anterior"))


def processar_arquivo(spark, arquivos: list[Path], entidade: str, competencia: str,
                      origem: str, raiz: Path, execucao_id: str, usuario: str) -> dict:
    # Fluxo de um ZIP: CSV textual -> normalização -> auditoria -> deduplicação
    # -> Parquet temporário validado -> substituição do fragmento daquela origem.
    diretorio = _diretorio_competencia(raiz, entidade, competencia)
    if (not origem.lower().endswith(".zip") or any(c in origem for c in '/\\:')
            or origem.startswith((".", "_")) or not origem[:-4].strip()):
        raise ValueError("Origem deve ser o nome do ZIP, sem diretórios")
    if not arquivos:
        raise ValueError("Informar pelo menos um arquivo Bronze; arquivo vazio representa remoção total")
    for arquivo in arquivos:
        if not Path(arquivo).is_file():
            raise FileNotFoundError(arquivo)
    destino = diretorio / origem[:-4]
    diretorio.mkdir(parents=True, exist_ok=True)
    reconciliar_publicacao(destino)
    staging = Path(tempfile.mkdtemp(prefix=f".{destino.name}.staging-", dir=diretorio))
    bronze = tabela = None
    try:
        # DISK_ONLY reaproveita resultados entre ações sem tentar manter o lote
        # inteiro na RAM. ISO-8859-1 e ';' seguem o formato publicado pela Receita.
        bronze = (spark.read.schema(esquema_csv(entidade))
                  .options(sep=";", encoding="ISO-8859-1", header="false", quote='"', escape='"', mode="FAILFAST")
                  .csv([str(Path(arquivo)) for arquivo in arquivos])
                  .persist(StorageLevel.DISK_ONLY))
        lidas = bronze.count()
        # Uma única data literal por lote evita horários diferentes entre ações Spark.
        instante = F.to_timestamp(F.lit(agora()))
        tabela = (_normalizar(bronze, entidade)
                  .withColumn("competencia", F.lit(competencia))
                  .withColumn("arquivo_origem", F.lit(origem))
                  .withColumn("data_insercao", instante)
                  .withColumn("data_atualizacao", instante)
                  .withColumn("atualizado_por", F.lit(usuario))
                  .withColumn("execucao_id", F.lit(execucao_id)))
        # Datas antigas devem ser lidas ANTES de substituir o snapshot da origem.
        if _fragmentos(diretorio):
            tabela = preservar_insercao(tabela, ler_tabela(spark, raiz, entidade, competencia), CHAVES[entidade])
        tabela = deduplicar(tabela, entidade).select(*esquema_prata(entidade).fieldNames()).persist(StorageLevel.DISK_ONLY)
        obrigatorios = ["cnpj_basico"]
        if entidade == "estabelecimentos":
            obrigatorios.extend(["cnpj_ordem", "cnpj_dv", "cnpj_completo"])
        if tabela.where(F.lit(False) | sum(F.col(nome).isNull().cast("int") for nome in obrigatorios).cast("boolean")).limit(1).count():
            raise ValueError("Chave de negócio ausente no lote Bronze")
        linhas = tabela.count()
        # overwrite atua somente no staging desta origem/competência, nunca em
        # toda a Prata. A troca definitiva fica a cargo de publicar_diretorio.
        tabela.write.mode("overwrite").parquet(str(staging))
        # Força a leitura completa do estágio antes de tocar a última versão válida.
        if spark.read.schema(esquema_prata(entidade)).parquet(str(staging)).count() != linhas:
            raise ValueError("Contagem do Parquet publicado difere do lote validado")
        tamanho = bytes_diretorio(staging)
        publicar_diretorio(staging, destino)
        return {"caminho": destino.relative_to(Path(raiz)).as_posix(), "linhas": linhas,
                "duplicidades": lidas - linhas, "bytes": tamanho}
    finally:
        # persist() precisa de unpersist(): libera o cache em disco ao terminar.
        if tabela is not None:
            tabela.unpersist()
        if bronze is not None:
            bronze.unpersist()
        if staging.exists():
            shutil.rmtree(staging)
