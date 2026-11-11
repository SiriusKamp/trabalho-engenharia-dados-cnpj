# Projeto de Avaliação - Plataforma de Dados CNPJ

## Intenção

Criar uma base local-first, reproduzível e didática para o trabalho avaliativo de Engenharia de Dados em Larga Escala, usando o Cenário A da aula: dados abertos do CNPJ, pipeline Bronze → Silver → Gold, perguntas de negócio, segurança, qualidade e observabilidade.

O objetivo desta primeira versão não é baixar ou processar os mais de 60 milhões de registros. É deixar um caminho executável com dados sintéticos pequenos, contratos claros e pontos de extensão para os dados reais.

## Metas

- Registrar uma trilha de execução em ordem, alinhada aos pesos da rubrica.
- Demonstrar o fluxo de dados com uma execução local pequena.
- Preservar o bruto na Bronze e produzir dados tipados, deduplicados e mascarados na Silver.
- Produzir resultados Gold para três perguntas de negócio.
- Registrar qualidade, métricas operacionais e auditoria em artefatos simples.
- Preparar o projeto para a substituição do dataset sintético pelo dataset oficial e para a evolução para PySpark/Delta.

## Fora de escopo desta versão

- Download automático dos arquivos oficiais do CNPJ.
- Deploy em cloud ou criação de recursos pagos.
- Dashboard web completo.
- Cluster Spark distribuído obrigatório.

## Critérios de sucesso

1. `python scripts/run_demo.py` executa do início ao fim sem dados externos.
2. As camadas `artifacts/bronze`, `artifacts/silver` e `artifacts/gold` são geradas.
3. O CPF/CNPJ de sócio aparece mascarado na Silver e não é copiado para a Gold.
4. A execução é idempotente: repetir o comando não duplica registros.
5. Testes automatizados cobrem normalização, mascaramento, qualidade e agregações.
6. A documentação liga cada evidência a um item da rubrica: arquitetura 20%, pipeline 30%, segurança 20%, observabilidade 15% e apresentação 15%.

## Abordagens consideradas

### 1. Notebook mínimo

Seria rápido para demonstrar consultas, mas deixaria segurança, reprocessamento, testes e observabilidade dependentes de explicação oral. Não atende bem ao objetivo de maximizar evidências da rubrica.

### 2. Repositório local-first de referência - escolhido

Usa Python e arquivos locais para uma primeira execução pequena, com interfaces que podem ser trocadas por PySpark, Parquet/Delta, Airflow e Prometheus. O trade-off é que a primeira versão não representa a escala total, mas permite validar o desenho e registrar evidências antes de subir o custo operacional.

### 3. Arquitetura cloud/produção desde o primeiro commit

Seria mais próxima de uma solução real, mas desviaria o esforço para credenciais, rede, custos e provisionamento. Fica como evolução após o pipeline local e os critérios de aceite estarem comprovados.

## Decisões de implementação

- O cenário padrão é CNPJ, porque é o caso detalhado nos slides 40–45 e permite começar com dados públicos.
- O demo usa CSV e JSONL por portabilidade; o job de produção previsto em `jobs/` usa PySpark e mantém o mesmo contrato de camadas.
- A função de mascaramento fica isolada para ser demonstrada e testada.
- Os artefatos de execução são descartáveis e ficam ignorados pelo Git; apenas exemplos e evidências documentais entram no repositório.
- Cada etapa deve gerar uma evidência verificável antes da próxima ser iniciada.

## O que ainda caberá ao aluno

- Escolher três perguntas finais e justificar o valor para o negócio.
- Trocar os dados sintéticos pelo lote oficial e validar o layout da referência baixada.
- Medir tempo, volume, partições e custo da execução real.
- Completar os componentes de produção escolhidos para a apresentação.
