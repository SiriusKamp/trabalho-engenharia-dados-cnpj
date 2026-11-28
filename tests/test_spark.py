"""Contratos e integração com CSVs sintéticos completos, sem baixar dados reais.

Executar: python -B -m unittest discover -s tests -p test_spark.py -v
Os testes de integração exigem PySpark 4 e Java; ausência de PySpark é explícita.
"""

import csv
import importlib.util
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


EMPRESAS = (
    "cnpj_basico razao_social natureza_juridica qualificacao_responsavel "
    "capital_social porte_empresa ente_federativo_responsavel"
).split()
ESTABELECIMENTOS = (
    "cnpj_basico cnpj_ordem cnpj_dv identificador_matriz_filial nome_fantasia "
    "situacao_cadastral data_situacao_cadastral motivo_situacao_cadastral "
    "nome_cidade_exterior pais data_inicio_atividade cnae_fiscal_principal "
    "cnae_fiscal_secundaria tipo_logradouro logradouro numero complemento "
    "bairro cep uf municipio ddd_1 telefone_1 ddd_2 telefone_2 ddd_fax fax "
    "correio_eletronico situacao_especial data_situacao_especial"
).split()
SOCIOS = (
    "cnpj_basico identificador_socio nome_socio documento_mascarado "
    "qualificacao_socio data_entrada_sociedade pais representante_legal "
    "nome_representante qualificacao_representante_legal faixa_etaria"
).split()
CAMPOS = {"empresas": EMPRESAS, "estabelecimentos": ESTABELECIMENTOS, "socios": SOCIOS}
AUDITORIA = {
    "competencia", "arquivo_origem", "data_insercao", "data_atualizacao",
    "atualizado_por", "execucao_id",
}
PRODUTOS = {
    "dim_empresa", "dim_socio", "fato_resumo_empresarial",
    "fato_socios_multiplas_empresas", "fato_perfil_empresarial",
}


def empresa(cnpj="00000001", **alteracoes):
    linha = dict(zip(EMPRESAS, [cnpj, "São José; Comércio", "2062", "49", "1000,00", "01", ""]))
    return linha | alteracoes


def estabelecimento(cnpj="00000001", ordem="0001", **alteracoes):
    valores = [
        cnpj, ordem, "00", "1", "São José", "02", "20240131", "00", "", "105",
        "20240131", "0111301", "0111302,0111303", "RUA", "São João", "001", "",
        "Centro", "01001000", "SP", "7107", "011", "00000000", "", "", "", "",
        "exemplo@example.test", "", "00000000",
    ]
    return dict(zip(ESTABELECIMENTOS, valores)) | alteracoes


def socio(cnpj="00000001", **alteracoes):
    valores = [cnpj, "2", "  Ana   Silva  ", "***123456**", "49", "20240131", "105", "***654321**", "João", "05", "9"]
    return dict(zip(SOCIOS, valores)) | alteracoes


class TestClassificacao(unittest.TestCase):
    def test_identifica_zip_e_arquivos_oficiais_sem_extensao_csv(self):
        from Scripts.Pipeline.Esquemas import entidade_arquivo

        for nome, esperado in [
            ("Empresas0.zip", "empresas"), ("Estabelecimentos9.ZIP", "estabelecimentos"),
            ("Socios3.zip", "socios"), ("K3241.K03200Y0.D60214.EMPRECSV", "empresas"),
            ("K3241.K03200Y0.D60214.ESTABELE", "estabelecimentos"),
            ("K3241.K03200Y0.D60214.SOCIOCSV", "socios"),
            ("Simples.zip", None), ("Qualificacoes.zip", None), ("empresas_backup.txt", None),
        ]:
            with self.subTest(nome=nome):
                self.assertEqual(entidade_arquivo(nome), esperado)


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "PySpark não instalado: integração não executada")
class TestPipelineSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from Scripts.Pipeline.PipelinePrata import criar_spark

        cls.spark = criar_spark()
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temporario = tempfile.TemporaryDirectory(prefix="cnpj-teste-")
        self.addCleanup(self.temporario.cleanup)
        self.raiz = Path(self.temporario.name) / "DATA"
        self.raiz.mkdir()
        self.sequencia = 0

    def carregar(self, entidade, linhas, competencia="2024-02", origem=None, instante="2024-03-01T12:00:00+00:00"):
        from Scripts.Pipeline.PipelinePrata import processar_arquivo

        self.sequencia += 1
        arquivo = self.raiz.parent / f"bronze-{self.sequencia}.DADOS"
        with arquivo.open("w", encoding="iso8859-1", newline="") as destino:
            escritor = csv.writer(destino, delimiter=";", quoting=csv.QUOTE_ALL)
            escritor.writerows([[linha.get(campo, "") for campo in CAMPOS[entidade]] for linha in linhas])
        with patch("Scripts.Pipeline.PipelinePrata.agora", return_value=instante):
            return processar_arquivo(
                self.spark, [arquivo], entidade, competencia,
                origem or f"{entidade}0.zip", self.raiz, f"execucao-{self.sequencia}", "teste",
            )

    def tabela(self, entidade, competencia="2024-02"):
        from Scripts.Pipeline.PipelinePrata import ler_tabela

        return ler_tabela(self.spark, self.raiz, entidade, competencia)

    def ouro(self, competencia="2024-02"):
        from Scripts.Pipeline.PipelineOuro import processar_ouro

        return processar_ouro(self.spark, self.raiz, competencia, "ouro-teste", "teste")

    def produto(self, nome, competencia="2024-02"):
        return self.spark.read.parquet(str(self.raiz / "Ouro" / f"competencia={competencia}" / nome))

    def preparar_mes(self, competencia="2024-02"):
        self.carregar("empresas", [empresa(), empresa("00000002", capital_social="3000,00")], competencia)
        self.carregar("estabelecimentos", [
            estabelecimento(), estabelecimento(),
            estabelecimento(ordem="0002", identificador_matriz_filial="2", situacao_cadastral="08"),
            estabelecimento("00000002", identificador_matriz_filial="2"),
        ], competencia)
        self.carregar("socios", [
            socio(), socio(nome_socio="ANA SILVA"), socio("00000002"),
            socio(nome_socio="Bia", documento_mascarado="***222222**", faixa_etaria="2"),
            socio(nome_socio="Pessoa Jurídica", identificador_socio="1", documento_mascarado="00000003000100", faixa_etaria="1"),
            socio(nome_socio="Idade desconhecida", documento_mascarado="***333333**", faixa_etaria="0"),
        ], competencia)

    def test_configura_spark_local_com_shuffle_criptografado_e_ui_desligada(self):
        configuracao = self.spark.sparkContext.getConf()
        self.assertEqual(configuracao.get("spark.io.encryption.enabled"), "true")
        self.assertEqual(configuracao.get("spark.ui.enabled"), "false")
        self.assertEqual(configuracao.get("spark.driver.bindAddress"), "127.0.0.1")

    def test_normaliza_layouts_completos_sem_perder_zeros_mascara_ou_competencia(self):
        resultado = self.carregar("empresas", [empresa(capital_social="1234,56"), empresa(capital_social="999999,00", razao_social="")])
        self.assertEqual(resultado["linhas"], 1)
        self.assertEqual(resultado["duplicidades"], 1)
        self.assertGreater(resultado["bytes"], 0)
        self.assertEqual(resultado["caminho"], "Prata/empresas/competencia=2024-02/empresas0")
        empresas = self.tabela("empresas")
        self.assertEqual(empresas.schema["capital_social"].dataType.simpleString(), "decimal(20,2)")
        linha = empresas.first()
        self.assertEqual(linha.cnpj_basico, "00000001")
        self.assertEqual(linha.razao_social, "São José; Comércio")
        self.assertEqual(linha.capital_social, Decimal("1234.56"))
        self.carregar("estabelecimentos", [estabelecimento(data_situacao_cadastral="20240230")])
        est = self.tabela("estabelecimentos").first()
        self.assertEqual(est.cnpj_completo, "00000001000100")
        self.assertEqual(est.cep, "01001000")
        self.assertEqual(est.data_inicio_atividade, date(2024, 1, 31))
        self.assertIsNone(est.data_situacao_cadastral)
        self.assertIsNone(est.data_situacao_especial)
        self.carregar("socios", [socio(), socio(nome_socio="ANA SILVA")])
        pessoa = self.tabela("socios").first()
        self.assertEqual(pessoa.documento_mascarado, "***123456**")
        self.assertEqual(pessoa.nome_normalizado, "ANA SILVA")
        self.assertEqual(self.tabela("socios").count(), 1)
        for entidade, campos in CAMPOS.items():
            tabela = self.tabela(entidade)
            self.assertTrue((set(campos) | AUDITORIA).issubset(tabela.columns))
            for campo in campos:
                if campo.startswith("data_") or campo == "capital_social":
                    continue
                self.assertEqual(tabela.schema[campo].dataType.simpleString(), "string", campo)
        self.carregar("empresas", [empresa(capital_social="0,00")], "2024-01")
        self.assertEqual(self.tabela("empresas", "2024-01").first().capital_social, Decimal("0.00"))
        self.assertEqual(self.tabela("empresas").first().capital_social, Decimal("1234.56"))
        self.assertFalse(list(self.raiz.rglob("*.crc")))

    def test_le_csv_com_quebra_de_linha_entre_aspas_sem_deslocar_colunas(self):
        # Um campo entre aspas pode ocupar duas linhas físicas no CSV oficial.
        # O registro seguinte também deve continuar independente, com 30 colunas.
        complemento = 'ANDAR 16\nTORRE "2"; SALA 2'
        resultado = self.carregar("estabelecimentos", [
            estabelecimento(complemento=complemento),
            estabelecimento("00000002", complemento="SALA 3"),
        ])
        self.assertEqual(resultado["linhas"], 2)
        self.assertEqual(resultado["duplicidades"], 0)
        linhas = {linha.cnpj_basico: linha for linha in self.tabela("estabelecimentos").collect()}
        self.assertEqual(linhas["00000001"].complemento, complemento)
        self.assertEqual(linhas["00000001"].bairro, "Centro")
        self.assertEqual(linhas["00000001"].cep, "01001000")
        self.assertEqual(linhas["00000001"].correio_eletronico, "exemplo@example.test")
        self.assertEqual(linhas["00000002"].complemento, "SALA 3")

    def test_substitui_origem_com_remocoes_preservando_candidato_e_insercao(self):
        self.carregar("empresas", [empresa(), empresa("00000002"), empresa("00000003")], origem="Empresas0.zip")
        insercao = self.tabela("empresas").where("cnpj_basico = '00000001'").first().data_insercao
        self.carregar("empresas", [empresa(razao_social="", capital_social="999999,00")], origem="Empresas1.zip", instante="2024-03-02T12:00:00+00:00")
        self.assertEqual(self.tabela("empresas").where("cnpj_basico = '00000001'").first().arquivo_origem, "Empresas0.zip")
        self.carregar("empresas", [empresa()], "2024-01", origem="Empresas0.zip")
        self.carregar("empresas", [empresa("00000002", capital_social="5,00")], origem="Empresas0.zip", instante="2024-03-03T12:00:00+00:00")
        linhas = {linha.cnpj_basico: linha for linha in self.tabela("empresas").collect()}
        self.assertEqual(set(linhas), {"00000001", "00000002"})
        self.assertEqual(linhas["00000001"].arquivo_origem, "Empresas1.zip")
        self.assertEqual(linhas["00000001"].data_insercao, insercao)
        self.assertEqual(linhas["00000002"].capital_social, Decimal("5.00"))
        self.assertEqual(self.tabela("empresas", "2024-01").count(), 1)
        self.carregar("empresas", [], origem="Empresas0.zip")
        self.assertEqual(self.tabela("empresas").count(), 1)
        self.carregar("empresas", [], origem="Empresas1.zip")
        self.assertEqual(self.tabela("empresas").count(), 0)
        self.assertEqual(self.tabela("empresas").schema["capital_social"].dataType.simpleString(), "decimal(20,2)")

    def test_qualidade_e_desempate_independem_da_ordem_e_do_maior_capital(self):
        completa = empresa(capital_social="10,00")
        incompleta = empresa(capital_social="999999,00", natureza_juridica="")
        self.carregar("empresas", [incompleta, completa])
        self.assertEqual(self.tabela("empresas").first().capital_social, Decimal("10.00"))
        self.carregar("empresas", [empresa(capital_social="20,00"), completa])
        escolhido = self.tabela("empresas").first().capital_social
        self.carregar("empresas", [completa, empresa(capital_social="20,00")])
        self.assertEqual(self.tabela("empresas").first().capital_social, escolhido)
        self.carregar("socios", [socio(documento_mascarado="", identificador_socio="3"), socio(documento_mascarado="", identificador_socio="3", pais="249")])
        self.assertEqual(self.tabela("socios").count(), 2)
        self.assertTrue(all(linha.identificacao_aproximada for linha in self.tabela("socios").collect()))

    def test_ouro_preagrega_dimensoes_e_indicadores_na_competencia(self):
        for competencia in ("2024-01", "2024-02"):
            self.preparar_mes(competencia)
            resultado = self.ouro(competencia)
            self.assertEqual(set(resultado["linhas"]), PRODUTOS)
            self.assertEqual(resultado["linhas"]["dim_empresa"], 2)
            self.assertEqual(resultado["linhas"]["dim_socio"], 4)
            self.assertEqual(resultado["linhas"]["fato_socios_multiplas_empresas"], 1)
            self.assertEqual(resultado["caminho"], f"Ouro/competencia={competencia}")
            self.assertGreater(resultado["bytes"], 0)
        resumo = self.produto("fato_resumo_empresarial")
        linhas = {linha.cnpj_basico: linha for linha in resumo.collect()}
        self.assertEqual(len(linhas), 2)
        self.assertEqual(linhas["00000001"].quantidade_estabelecimentos, 2)
        self.assertEqual(linhas["00000001"].estabelecimentos_ativos, 1)
        self.assertEqual(linhas["00000001"].quantidade_socios, 4)
        self.assertEqual(resumo.selectExpr("avg(capital_social) as media").first().media, Decimal("2000.000000"))
        self.assertNotIn("razao_social", resumo.columns)
        perfil = {linha.cnpj_basico: linha for linha in self.produto("fato_perfil_empresarial").collect()}
        self.assertAlmostEqual(perfil["00000001"].idade_media_estimada_socios_pf, 51.75)
        self.assertEqual(perfil["00000001"].meses_desde_abertura_matriz, 1)
        self.assertIsNone(perfil["00000002"].meses_desde_abertura_matriz)
        self.assertEqual(self.produto("fato_perfil_empresarial", "2024-01").where("cnpj_basico = '00000001'").first().meses_desde_abertura_matriz, 0)
        multiplo = self.produto("fato_socios_multiplas_empresas").first()
        self.assertEqual(multiplo.quantidade_empresas, 2)
        self.assertEqual(self.produto("dim_socio").where("socio_id = '" + multiplo.socio_id + "'").first().documento_mascarado, "***123456**")
        self.assertEqual(self.ouro()["linhas"]["fato_resumo_empresarial"], 2)

    def test_todas_faixas_pf_e_metricas_desconhecidas(self):
        self.carregar("empresas", [empresa(capital_social=""), empresa("00000002", capital_social="0,00")])
        self.carregar("estabelecimentos", [])
        pessoas = [socio(nome_socio=f"Pessoa {faixa}", documento_mascarado=f"***00000{faixa}**", faixa_etaria=str(faixa)) for faixa in range(10)]
        pessoas.append(socio(identificador_socio="1", nome_socio="PJ", faixa_etaria="9"))
        self.carregar("socios", pessoas)
        self.ouro()
        perfil = {linha.cnpj_basico: linha for linha in self.produto("fato_perfil_empresarial").collect()}
        self.assertAlmostEqual(perfil["00000001"].idade_media_estimada_socios_pf, 412.5 / 9)
        self.assertIsNone(perfil["00000001"].capital_social)
        self.assertIsNone(perfil["00000002"].idade_media_estimada_socios_pf)
        self.assertEqual(perfil["00000002"].capital_social, Decimal("0.00"))

    def test_mascara_cpf_integro_inesperado_antes_da_chave_e_publicacao(self):
        self.carregar("socios", [socio(documento_mascarado="12345678901", representante_legal="10987654321")])
        pessoa = self.tabela("socios").first()
        self.assertEqual(pessoa.documento_mascarado, "***456789**")
        self.assertEqual(pessoa.representante_legal, "***876543**")

    def test_otimizacao_valida_estagio_em_uma_acao_inclusive_vazio_e_chaves_invalidas(self):
        from Scripts.Pipeline import PipelineOuro

        validar = getattr(PipelineOuro, "_validar_estagio", None)
        self.assertTrue(callable(validar), "Falta o validador do staging em uma única ação")
        casos = [
            ([("a", "2024-02"), ("b", "2024-02")], 2, None),
            ([], 0, None),
            ([(None, "2024-02")], None, "Chave nula"),
            ([("a", None)], None, "Chave nula"),
            ([("a", "2024-02"), ("a", "2024-02")], None, "Chave duplicada"),
            ([(None, "2024-02"), (None, "2024-02")], None, "Chave nula"),
        ]
        for indice, (linhas, esperado, erro) in enumerate(casos):
            with self.subTest(caso=indice):
                pasta = self.raiz / f"estagio-{indice}"
                self.spark.createDataFrame(linhas, "chave string, competencia string").write.parquet(str(pasta))
                estagio = self.spark.read.parquet(str(pasta))
                classe = type(estagio)
                coletar_original = classe.collect
                # collect é a fronteira da ação; os dois agrupamentos são planos lazy.
                with patch.object(classe, "collect", autospec=True, side_effect=coletar_original) as coletar:
                    if erro:
                        with self.assertRaisesRegex(ValueError, erro):
                            validar(estagio, ["chave", "competencia"], "produto_teste")
                    else:
                        self.assertEqual(validar(estagio, ["chave", "competencia"], "produto_teste"), esperado)
                self.assertEqual(coletar.call_count, 1)

    def test_otimizacao_cache_estreito_base_compartilhada_logs_e_datas(self):
        from pyspark import StorageLevel
        from Scripts.Pipeline import PipelineOuro

        self.preparar_mes()
        classe = type(self.tabela("empresas"))
        persistir_original = classe.persist
        caches = []

        def persistir(tabela, nivel):
            caches.append(tabela)
            self.assertEqual(nivel, StorageLevel.DISK_ONLY)
            return persistir_original(tabela, nivel)

        with patch.object(classe, "persist", autospec=True, side_effect=persistir):
            with self.assertLogs("cnpj", level="INFO") as logs:
                with patch.object(PipelineOuro, "agora", return_value="2024-03-01T12:00:00+00:00"):
                    PipelineOuro.processar_ouro(self.spark, self.raiz, "2024-02", "execucao-privada", "usuario-privado")
        self.assertEqual(len(caches), 4, "Três fontes e uma base compartilhada devem ser persistidas")
        self.assertEqual(set(caches[1].columns), {
            "cnpj_basico", "competencia", "identificador_matriz_filial", "situacao_cadastral", "data_inicio_atividade",
        })
        self.assertIn("_abertura_matriz", caches[3].columns)
        self.assertIn("idade_media_estimada_socios_pf", caches[3].columns)
        self.assertTrue(all(tabela.storageLevel == StorageLevel.NONE for tabela in caches))
        texto = "\n".join(logs.output)
        for nome in PRODUTOS:
            for etapa in ("produto", "validacao"):
                self.assertIn(f"Ouro {etapa} inicio: competencia=2024-02 produto={nome}", texto)
                self.assertIn(f"Ouro {etapa} fim: competencia=2024-02 produto={nome} status=sucesso duracao_s=", texto)
        self.assertNotIn("usuario-privado", texto)
        self.assertNotIn("execucao-privada", texto)
        primeiras = {nome: self.produto(nome).collect() for nome in PRODUTOS}
        with patch.object(PipelineOuro, "agora", return_value="2024-03-02T12:00:00+00:00"):
            PipelineOuro.processar_ouro(self.spark, self.raiz, "2024-02", "nova-execucao", "novo-usuario")
        for nome in PRODUTOS:
            atuais = self.produto(nome).collect()
            self.assertEqual(len(atuais), len(primeiras[nome]))
            chave = lambda linha: (linha.competencia, getattr(linha, "cnpj_basico", getattr(linha, "socio_id", None)))
            for antiga, atual in zip(sorted(primeiras[nome], key=chave), sorted(atuais, key=chave)):
                self.assertEqual(atual.data_insercao, antiga.data_insercao)
                self.assertGreater(atual.data_atualizacao, antiga.data_atualizacao)
                self.assertEqual(atual.execucao_id, "nova-execucao")
                self.assertEqual(atual.atualizado_por, "novo-usuario")
                campos = set(atual.asDict()) - {"data_atualizacao", "execucao_id", "atualizado_por"}
                self.assertEqual({c: atual[c] for c in campos}, {c: antiga[c] for c in campos})

    def test_otimizacao_preserva_qualidade_antes_da_projecao_e_unicidade_entre_origens(self):
        self.carregar("empresas", [empresa(), empresa("00000002")])
        # logradouro não vai para o cache estreito, mas decide o vencedor completo.
        self.carregar("estabelecimentos", [estabelecimento(logradouro="", situacao_cadastral="08")], origem="Estabelecimentos0.zip")
        self.carregar("estabelecimentos", [estabelecimento()], origem="Estabelecimentos1.zip")
        self.carregar("socios", [socio(), socio()], origem="Socios0.zip")
        self.carregar("socios", [socio(), socio("00000002")], origem="Socios1.zip")
        vinculos = self.tabela("socios")
        self.assertEqual(vinculos.count(), 2)
        self.assertEqual(vinculos.select("cnpj_basico", "socio_id", "competencia").distinct().count(), 2)
        self.ouro()
        self.assertEqual(self.produto("fato_socios_multiplas_empresas").first().quantidade_empresas, 2)
        resumo = self.produto("fato_resumo_empresarial").where("cnpj_basico = '00000001'").first()
        self.assertEqual(resumo.quantidade_estabelecimentos, 1)
        self.assertEqual(resumo.estabelecimentos_ativos, 1)

    def test_otimizacao_falha_validacao_libera_caches_e_preserva_publicacao(self):
        from pyspark import StorageLevel
        from Scripts.Pipeline import PipelineOuro

        validar = getattr(PipelineOuro, "_validar_estagio", None)
        self.assertTrue(callable(validar), "Falta o validador do staging")
        self.preparar_mes()
        self.ouro()
        anteriores = {nome: self.produto(nome).collect() for nome in PRODUTOS}
        classe = type(self.tabela("empresas"))
        persistir_original = classe.persist
        caches = []

        def persistir(tabela, nivel):
            caches.append(tabela)
            return persistir_original(tabela, nivel)

        def validar_com_falha(tabela, chaves, nome):
            if nome == "fato_resumo_empresarial":
                raise ValueError("falha sintética na validação")
            return validar(tabela, chaves, nome)

        with patch.object(classe, "persist", autospec=True, side_effect=persistir):
            with patch.object(PipelineOuro, "_validar_estagio", side_effect=validar_com_falha):
                with self.assertLogs("cnpj", level="INFO") as logs:
                    with self.assertRaisesRegex(ValueError, "falha sintética"):
                        self.ouro()
        self.assertEqual(len(caches), 4)
        self.assertTrue(all(tabela.storageLevel == StorageLevel.NONE for tabela in caches))
        for nome in PRODUTOS:
            self.assertCountEqual(self.produto(nome).collect(), anteriores[nome])
        self.assertFalse([p for p in self.raiz.rglob("*") if ".staging-" in p.name])
        texto = "\n".join(logs.output)
        for etapa in ("produto", "validacao"):
            self.assertIn(f"Ouro {etapa} fim: competencia=2024-02 produto=fato_resumo_empresarial status=erro duracao_s=", texto)

    def test_falhas_preservam_prata_e_conjunto_ouro_validos(self):
        self.preparar_mes()
        anterior = self.ouro()["linhas"]
        with patch("Scripts.Pipeline.PipelinePrata.publicar_diretorio", side_effect=OSError("falha de publicação")):
            with self.assertRaises(OSError):
                self.carregar("empresas", [empresa(capital_social="1,00")])
        self.assertEqual(self.tabela("empresas").where("cnpj_basico = '00000001'").first().capital_social, Decimal("1000.00"))
        with self.assertRaises(ValueError):
            self.carregar("empresas", [empresa(cnpj="")])
        from Scripts.Pipeline.PipelineOuro import processar_ouro
        with patch("Scripts.Pipeline.PipelineOuro.publicar_diretorio", side_effect=OSError("falha de publicação")):
            with self.assertRaises(OSError):
                processar_ouro(self.spark, self.raiz, "2024-02", "falha", "teste")
        self.assertEqual({nome: self.produto(nome).count() for nome in PRODUTOS}, anterior)
        self.assertFalse([p for p in self.raiz.rglob("*") if ".staging-" in p.name])


if __name__ == "__main__":
    unittest.main()
