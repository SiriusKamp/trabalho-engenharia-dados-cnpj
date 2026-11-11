# Trilha de execução do trabalho avaliativo

O trabalho deve ser executado nesta ordem. A regra prática é não avançar sem guardar a evidência da etapa anterior.

## 0. Definir o problema e o contrato

- Formar o grupo de até quatro pessoas.
- Escolher o Cenário A (CNPJ) e escrever três perguntas de negócio mensuráveis.
- Definir a data de referência dos arquivos e a chave de cada tabela.
- Registrar o que ficará fora do escopo e quais trade-offs serão defendidos.

**Evidência:** `docs/decisoes-arquitetura.md` e uma tabela de perguntas, fontes, métricas e consumidores.

## 1. Desenhar a arquitetura - 20%

Desenhar fonte → ingestão → Bronze → Silver → Gold → consumo. Para cada bloco, informar tecnologia, formato, particionamento, segurança e motivo da escolha.

**Obrigatório:** justificar por que usar processamento distribuído, por que separar camadas e como reprocessar sem duplicar.

**Evidência:** diagrama exportado, legenda dos componentes e tabela de trade-offs.

## 2. Baixar e catalogar os dados

- Baixar somente o lote oficial escolhido.
- Guardar checksum, data de download, referência e tamanho de cada arquivo.
- Preservar os ZIPs e CSVs brutos; não sobrescrever a Bronze com dados tratados.
- Validar delimitador, encoding, quantidade de colunas e presença da chave.

**Evidência:** manifesto de ingestão e log de validação.

## 3. Construir a Bronze

- Copiar os dados para uma estrutura previsível por tabela e referência.
- Adicionar metadados de ingestão: `run_id`, `ingested_at`, `source_file`.
- Tornar a etapa idempotente: a mesma referência não pode gerar duplicação.

**Evidência:** árvore da Bronze, contagem de linhas e log de uma segunda execução.

## 4. Construir a Silver - início dos 30% do pipeline

- Aplicar o layout oficial.
- Tipar capital, situação cadastral, datas e códigos.
- Normalizar texto, UF e campos vazios.
- Deduplicar pela chave natural.
- Mascarar CPF/CNPJ de sócios antes de publicar a camada.
- Validar regras de qualidade e bloquear a Gold se houver falha crítica.

**Evidência:** schema, regras de qualidade, antes/depois de uma amostra e resultado dos testes.

## 5. Construir a Gold - conclusão dos 30% do pipeline

Produzir tabelas de resposta para pelo menos três perguntas:

1. UFs com mais estabelecimentos ativos.
2. Distribuição por porte e capital social médio.
3. Perfil de faixa etária/qualificação dos sócios.

Cada resultado deve conter métrica, período, filtros, contagem de registros usados e consulta reproduzível.

**Evidência:** arquivos Gold, consultas, tabelas e interpretação de cada achado.

## 6. Implementar segurança - 20%

- Separar acesso de operador, engenheiro e consumidor.
- Criptografar em trânsito e em repouso no ambiente escolhido.
- Mascarar PII e provar que a Gold não expõe a identificação original.
- Registrar auditoria de execução sem gravar dados sensíveis no log.
- Documentar retenção e descarte.

**Evidência:** matriz IAM, regra de mascaramento, teste de não exposição e amostra de auditoria.

## 7. Implementar observabilidade - 15%

Medir pelo menos:

- duração por etapa;
- linhas lidas, escritas e rejeitadas;
- taxa de nulos e duplicidades;
- status de qualidade;
- timestamp, `run_id` e origem.

Criar alertas para falha de schema, queda brusca de volume e qualidade abaixo do limite.

**Evidência:** dashboard ou export de métricas, alertas disparados em teste e log correlacionado.

## 8. Preparar a apresentação - 15%

Em 20 minutos:

1. problema e perguntas de negócio;
2. arquitetura e trade-offs;
3. execução Bronze → Silver → Gold;
4. segurança e qualidade;
5. observabilidade;
6. resultados e próximos passos.

Reserve a arguição para explicar decisões, limites, custo e como a solução evolui.

**Evidência:** roteiro, slides, demo ensaiada e respostas para as perguntas difíceis.

## Ordem mínima no repositório

```text
01_decisoes -> 02_manifesto -> 03_bronze -> 04_silver -> 05_gold
    -> 06_seguranca -> 07_observabilidade -> 08_apresentacao
```

Não comece pelo dashboard. Sem contrato, Bronze e Silver, o dashboard só esconde problemas de qualidade.
