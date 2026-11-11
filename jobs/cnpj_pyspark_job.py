"""Caminho de escala: contrato PySpark para os arquivos oficiais do CNPJ.

Execute somente depois de instalar o extra `spark` e apontar `INPUT_ROOT` para
os CSVs oficiais. O demo local não depende deste módulo.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


EMPRESAS_COLS = [
    "cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel",
    "capital_social", "porte", "ente_federativo_responsavel",
]
ESTAB_COLS = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
    "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
    "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
    "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
    "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep",
    "uf", "municipio", "ddd1", "telefone1", "ddd2", "telefone2", "ddd_fax",
    "fax", "correio_eletronico", "situacao_especial", "data_situacao_especial",
]
SOCIOS_COLS = [
    "cnpj_basico", "identificador_socio", "nome_socio", "cnpj_cpf_socio",
    "qualificacao_socio", "data_entrada_sociedade", "pais", "representante_legal",
    "nome_representante", "qualificacao_representante", "faixa_etaria",
]


def read_cnpj_csv(spark: SparkSession, path: str, columns: list[str]) -> DataFrame:
    return (
        spark.read.option("delimiter", ";")
        .option("header", "false")
        .option("encoding", "ISO-8859-1")
        .csv(path)
        .toDF(*columns)
    )


def mask_document(column):
    digits = F.regexp_replace(column, r"\D", "")
    return F.when(F.length(digits) > 4, F.concat(F.repeat(F.lit("*"), F.length(digits) - 4), F.substring(digits, -4, 4))).otherwise(F.lit(""))


def run(input_root: str, output_root: str) -> None:
    spark = (
        SparkSession.builder.appName("cnpj_lakehouse")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )
    try:
        empresas_bronze = read_cnpj_csv(spark, f"{input_root}/empresas/*.csv", EMPRESAS_COLS)
        estabelecimentos_bronze = read_cnpj_csv(spark, f"{input_root}/estabelecimentos/*.csv", ESTAB_COLS)
        socios_bronze = read_cnpj_csv(spark, f"{input_root}/socios/*.csv", SOCIOS_COLS)

        empresas_silver = (
            empresas_bronze
            .withColumn("razao_social", F.upper(F.trim("razao_social")))
            .withColumn("capital_social", F.col("capital_social").cast("double"))
        )
        estabelecimentos_silver = (
            estabelecimentos_bronze
            .withColumn("uf", F.upper(F.trim("uf")))
            .withColumn("situacao_cadastral", F.col("situacao_cadastral").cast("int"))
        )
        socios_silver = socios_bronze.withColumn("cnpj_cpf_socio", mask_document(F.col("cnpj_cpf_socio")))

        empresas_silver.write.mode("overwrite").parquet(f"{output_root}/silver/empresas")
        estabelecimentos_silver.write.mode("overwrite").parquet(f"{output_root}/silver/estabelecimentos")
        socios_silver.write.mode("overwrite").parquet(f"{output_root}/silver/socios")

        top_ufs = (
            estabelecimentos_silver.filter(F.col("situacao_cadastral") == 2)
            .groupBy("uf").count().withColumnRenamed("count", "total_estabelecimentos")
        )
        top_ufs.write.mode("overwrite").parquet(f"{output_root}/gold/top_ufs")
    finally:
        spark.stop()


if __name__ == "__main__":
    run("datalake/cnpj", "datalake/cnpj")
