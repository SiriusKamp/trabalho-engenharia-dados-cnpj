"""Caminho opcional de escala para os CSVs oficiais do CNPJ."""

from pyspark.sql import SparkSession, functions as F


def run(input_root: str, output_root: str) -> None:
    """Lê CSVs, aplica o contrato Silver e grava Parquet."""

    spark = (
        SparkSession.builder.appName("cnpj_lakehouse")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )
    try:
        empresas = spark.read.option("delimiter", ";").option("header", "false").csv(f"{input_root}/empresas/*.csv")
        estabelecimentos = spark.read.option("delimiter", ";").option("header", "false").csv(f"{input_root}/estabelecimentos/*.csv")
        socios = spark.read.option("delimiter", ";").option("header", "false").csv(f"{input_root}/socios/*.csv")

        # O layout real deve ser aplicado com toDF(...) depois de validar
        # a referência do lote baixado. Aqui mostramos o padrão da Silver.
        empresas = empresas.withColumn("razao_social", F.upper(F.trim("_c1")))
        estabelecimentos = estabelecimentos.withColumn("uf", F.upper(F.trim("_c19")))
        socios = socios.withColumn("cnpj_cpf_socio_mascarado", F.concat(F.lit("*******"), F.substring("_c3", -4, 4)))

        empresas.write.mode("overwrite").parquet(f"{output_root}/silver/empresas")
        estabelecimentos.write.mode("overwrite").parquet(f"{output_root}/silver/estabelecimentos")
        socios.write.mode("overwrite").parquet(f"{output_root}/silver/socios")
    finally:
        spark.stop()


if __name__ == "__main__":
    run("data/raw/cnpj", "data/processed/cnpj")
