"""Layout oficial do CNPJ: ordem dos campos do CSV e tipos da camada Prata.

Referência: https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf
Códigos permanecem texto, inclusive documentos já mascarados pela Receita.
"""

import re


# Os CSVs oficiais não têm cabeçalho: a posição de cada campo é parte do layout.
# Por isso essa ordem precisa ser mantida mesmo quando só algumas colunas são usadas na Ouro.
COLUNAS = {
    "empresas": (
        "cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel",
        "capital_social", "porte_empresa", "ente_federativo_responsavel",
    ),
    "estabelecimentos": (
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep",
        "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax",
        "fax", "correio_eletronico", "situacao_especial", "data_situacao_especial",
    ),
    "socios": (
        "cnpj_basico", "identificador_socio", "nome_socio", "documento_mascarado",
        "qualificacao_socio", "data_entrada_sociedade", "pais", "representante_legal",
        "nome_representante", "qualificacao_representante_legal", "faixa_etaria",
    ),
}
# A competência faz parte de todas as chaves: a mesma empresa em dois meses
# representa duas observações válidas do histórico.
CHAVES = {
    "empresas": ("cnpj_basico", "competencia"),
    "estabelecimentos": ("cnpj_completo", "competencia"),
    "socios": ("cnpj_basico", "socio_id", "competencia"),
}
CAMPOS_NOME = {
    "empresas": "razao_social", "estabelecimentos": "nome_fantasia", "socios": "nome_socio",
}
# Estas colunas são acrescentadas pela aplicação, não existem nos CSVs originais.
METADADOS = (
    "competencia", "arquivo_origem", "data_insercao", "data_atualizacao",
    "atualizado_por", "execucao_id",
)
# Códigos oficiais: 1=0–12, 2=13–20, 3=21–30, ..., 8=71–80, 9=acima de 80.
# Usamos o ponto médio; no último grupo usamos 87 por decisão do projeto.
# O código 0 não entra no mapa e resulta em idade desconhecida (null).
IDADES_FAIXAS = {"1": 6.0, "2": 16.5, "3": 25.5, "4": 35.5, "5": 45.5,
                 "6": 55.5, "7": 65.5, "8": 75.5, "9": 87.0}


def entidade_arquivo(nome: str) -> str | None:
    """Reconhece ZIPs e os nomes internos oficiais, sem exigir extensão .csv."""
    nome = nome.replace("\\", "/").rsplit("/", 1)[-1].lower()
    zip_ou_csv = re.fullmatch(r"(empresas|estabelecimentos|socios)\d*\.(zip|csv)", nome)
    if zip_ou_csv:
        return zip_ou_csv.group(1)
    for sufixo, entidade in ((".emprecsv", "empresas"), (".estabele", "estabelecimentos"), (".sociocsv", "socios")):
        if nome.endswith(sufixo):
            return entidade
    return None


def esquema_csv(entidade: str):
    """A leitura inicial é textual; conversões explícitas vêm depois do CSV."""
    from pyspark.sql.types import StringType, StructField, StructType

    # Ler como texto primeiro preserva zeros à esquerda de CNPJ, CEP e códigos.
    return StructType([StructField(nome, StringType(), True) for nome in COLUNAS[entidade]])


def esquema_prata(entidade: str):
    from pyspark.sql.types import BooleanType, DateType, DecimalType, StringType, StructField, StructType, TimestampType

    campos = []
    for nome in COLUNAS[entidade]:
        tipo = DateType() if nome.startswith("data_") else StringType()
        if nome == "capital_social":
            # Decimal evita erros de arredondamento típicos de valores financeiros em float.
            tipo = DecimalType(20, 2)
        campos.append(StructField(nome, tipo, True))
    if entidade == "estabelecimentos":
        campos.append(StructField("cnpj_completo", StringType(), True))
    if entidade == "socios":
        campos.extend([
            StructField("nome_normalizado", StringType(), True),
            StructField("socio_id", StringType(), True),
            StructField("identificacao_aproximada", BooleanType(), True),
        ])
    for nome in METADADOS:
        # Datas de negócio usam DateType; datas da auditoria têm horário (TimestampType).
        tipo = TimestampType() if nome in ("data_insercao", "data_atualizacao") else StringType()
        campos.append(StructField(nome, tipo, True))
    return StructType(campos)
