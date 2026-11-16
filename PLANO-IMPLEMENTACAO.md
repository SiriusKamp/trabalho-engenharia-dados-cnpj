# Plano de implementação — reconstrução do pipeline CNPJ

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** substituir o starter por pipeline Python/PySpark incremental, com competências parametrizadas, segurança por perfil, Grafana e operação Docker.

**Architecture:** cada competência percorre Raw/Bronze/Prata/Ouro antes da seguinte. Quatro manifestos JSON controlam versões, auditoria e retomada; Parquet preserva todas as competências concluídas. A API serve métricas/arquivos e o Grafana usa Infinity.

**Tech Stack:** Python 3.12 no container, PySpark 4, Java 17, requests, FastAPI, Uvicorn, Grafana e unittest.

**Spec:** ARQUITETURA.md

## Global Constraints

- `--meses N` conta o mês atual e N-1 anteriores; padrão 4; inteiros positivos sem teto arbitrário.
- Preservar o histórico Prata/Ouro; nenhuma política de exclusão de meses antigos.
- Um mês por vez; até 3 downloads, uma extração e um processamento Spark por vez.
- Gravar sucesso somente após validação/publicação; descarte de Raw/Bronze não força download repetido.
- Apenas empresas, estabelecimentos e socios são ingeridos nesta versão; catalogar auxiliares sem afirmar que foram processados.
- Credenciais de demonstração documentadas, configuração privada por ambiente.
- Não executar carga real completa ou publicar serviços remotamente nesta implementação.

## Review Focus

- Mês atual ainda não publicado: informar ausência, processar somente meses disponíveis no intervalo solicitado.
- ZIP e CSV removidos após sucesso: repetir carga não deve baixar novamente sem alteração remota.
- Correção de origem: remover contribuição antiga sem perder competências anteriores ou candidatos de outras origens.
- Falha após publicação e antes do manifesto: reexecução deve reconciliar sem duplicação.
- Perfil de análise e credencial Grafana não podem acessar Raw/Bronze/Prata nem disparar carga.

## Interfaces compartilhadas

- `Metadados(raiz: Path)`: `ler(camada) -> dict`, `atualizar(camada, chave, **campos)`, `evento(camada, evento: dict)`, `estado(**campos) -> dict`; camadas `Raw`, `Bronze`, `Prata`, `Ouro`. Dados por camada: `arquivos`, `execucoes`; Raw inclui `estado`.
- `publicar_diretorio(staging: Path, destino: Path)`: publicação recuperável de um fragmento; `agora() -> str` UTC ISO.
- Prata: `processar_arquivo(spark, arquivos: list[Path], entidade, competencia, origem, raiz: Path, execucao_id, usuario) -> dict`; salva em `Prata/<entidade>/competencia=<mes>/<origem-sem-zip>/`; retorna `caminho`, `linhas`, `duplicidades`, `bytes`.
- `ler_tabela(spark, raiz: Path, entidade, competencia)` retorna DataFrame consolidado/deduplicado; `criar_spark()` e `entidade_arquivo(nome) -> str | None`.
- Ouro: `processar_ouro(spark, raiz: Path, competencia, execucao_id, usuario) -> dict`; publica todos os produtos em `Ouro/competencia=<mes>/` e retorna contagens/bytes.
- `executar(meses=4, raiz=None, simular=False, forcar=False, usuario='agendador') -> dict` em `Scripts/Agendador.py`; `reativar(raiz)` e `manutencao(raiz)`. API agenda trabalho sem bloquear HTTP; um lock de execução protege CLI/servidor.
- API: `criar_app(raiz=None)` lê manifestos para observabilidade e recebe agendador; serviço de métricas não depende de Spark para ler metadados.

### Task 1: Controle, fonte e seleção de competências

**Files:** `Scripts/Pipeline/Metadados.py`, `Scripts/Pipeline/BaixarArquivos.py`, `tests/test_controle.py`, `tests/test_download.py`.

- [x] Escrever testes para seleção atravessando ano, N=0, WebDAV com propstat 404 parcial, download/extrair com servidor local e ZIP inválido.
- [x] Reproduzir regressões antes das correções, incluindo listagem inválida e ZIP corrompido.
- [x] Implementar JSON atômico, reconciliação de publicação, listagem WebDAV, checksums, comparação remota e extração segura.
- [x] Executar testes e confirmar GREEN, incluindo metadados de falha.

### Task 2: Prata e Ouro PySpark

**Files:** `Scripts/Pipeline/Esquemas.py`, `Scripts/Pipeline/PipelinePrata.py`, `Scripts/Pipeline/PipelineOuro.py`, `tests/test_spark.py`.

- [x] Escrever teste de amostra com decimal brasileiro, códigos com zeros, idade 87, dois meses, duplicata e substituição de origem.
- [x] Implementar interfaces compartilhadas e validar com PySpark real, sem carregar tudo em pandas.
- [x] Validar esquema completo, deduplicação determinística e contribuição por arquivo.
- [x] Implementar dimensões, três fatos, indicadores sem multiplicação em joins e datas da competência.
- [x] Executar teste Spark real em ambiente Linux/Docker, ou registrar explicitamente impedimento e executar a parte disponível.

### Task 3: Operação mensal e CLI

**Files:** `Scripts/Agendador.py`, `main.py`, `tests/test_agendador.py`.

- [x] Escrever testes para ordem mensal, parada em falha, manutenção e reativação persistida.
- [x] Confirmar RED; implementar fila limitada, pipeline por arquivo e barreira da competência antes da Ouro.
- [x] Implementar CLI `importar --meses N`, `--simular`, `--forcar`, `manutencao`, `reativar`, `servir`.
- [x] Confirmar GREEN com adaptadores locais controlados; não baixar base real no teste.

### Task 4: API, roles e Grafana

**Files:** `Scripts/API.py`, `Scripts/Seguranca.py`, `Grafana/*`, `tests/test_api.py`.

- [x] Escrever testes de autenticação, negação por role, path traversal e métricas históricas.
- [x] Confirmar RED; implementar HTTP Basic, credenciais de ambiente, listagem/download autenticados e sumários JSON.
- [x] Provisionar datasource Infinity backend, dashboard e alerta de falha.
- [x] Confirmar GREEN; nenhum endpoint inicia Spark para consultar arquivos/metadados.

### Task 5: Limpeza, Docker e documentação

**Files:** `README.md`, `CREDENCIAIS.md`, `PLANO-DESENVOLVIMENTO.md`, `.env.example`, `.gitignore`, `.dockerignore`, `Dockerfile`, `compose.yaml`, `requirements.txt`, quatro `DATA/*/Metadados.json`.

- [x] Remover arquivos versionados do starter que foram substituídos e diretórios vazios; preservar `.git` e documentos aprovados.
- [x] Documentar comandos, credenciais de demonstração, responsabilidades de volume criptografado/TLS e resultados reais de validação.
- [x] Executar suíte, CLI sem rede, simulação WebDAV e validação Docker Compose.
- [x] Solicitar revisão independente de código; corrigir falhas importantes e repetir verificações pertinentes.

## Registro de execução

- 2026-11-16: desenho aprovado com `--meses` e documentação de credenciais. Implementação autorizada; execução local com partes independentes delegadas. Nenhum novo ciclo de aprovação de método é necessário.
- O plano fica na raiz por preferência do usuário por poucas pastas; o próprio checklist registra o progresso.
- 2026-11-16: suíte final na imagem atualizada: **45 testes, 188,005 segundos, OK**, sem testes ignorados. Inclui servidor HTTP/WebDAV local, ZIP/CRC real, PySpark e publicação/retomada Parquet.
- Revisão independente encontrou seis problemas de recuperação/concorrência. Corrigidos e cobertos por regressões: persistência de pendências fora da janela, invalidação da Ouro por republicação da Prata, nova baixa após CRC inválido, repetição de descoberta em 30 minutos, descarte interrompido da Bronze e reserva da trava antes do HTTP 202.
- Docker Compose validado; aplicação saudável e Grafana em execução. As consultas dos 16 painéis responderam sem erro; a regra de alerta apresentou saúde `ok` e estado inativo com manifestos vazios.
- Consulta real ao catálogo oficial confirmou agosto–novembro de 2026, com 37 arquivos catalogados em cada competência. Nenhum ZIP nacional foi baixado; os quatro manifestos do repositório permanecem vazios.
- A execução emite avisos do PySpark/JDK, descarte de temporários e `ResourceWarning` de sockets nos testes, além de depreciação Starlette/AnyIO. A suíte terminou com código zero; isso não representa um benchmark da base nacional.
- Carga nacional, dimensionamento com dados reais, criptografia do volume e implantação HTTPS precisam ser validados no servidor. Não foram configurados automaticamente no computador.
