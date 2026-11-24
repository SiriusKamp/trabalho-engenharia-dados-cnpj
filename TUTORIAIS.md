# Tutoriais: executar, testar e configurar a aplicação

Este guia reúne os comandos de uso diário. Execute-os na raiz do repositório, onde estão `main.py`, `requirements.txt` e `compose.yaml`.

Consulte [Endpoints_Documentacao.MD](Endpoints_Documentacao.MD) para permissões e contratos HTTP, e [Parquets_Documentacao.MD](Parquets_Documentacao.MD) para tipos, relacionamentos e visões históricas de cinco meses.

Os blocos `powershell` são para o terminal do Windows; os blocos `bash` são para Linux/WSL2. Os exemplos usam as credenciais públicas de demonstração de [CREDENCIAIS.md](CREDENCIAIS.md).

## 1. Escolher o modo de execução

| Modo | O que precisa estar instalado | Observação |
|---|---|---|
| Aplicação e Grafana no Docker | Docker Desktop/Engine com containers Linux | Ambiente usado na validação completa do projeto |
| Aplicação direta no Windows | Python, dependências e Java para Spark | API e catálogo podem ser estudados sem iniciar Spark; o pipeline completo exige o ambiente nativo Hadoop/Spark funcionando |
| Aplicação direta no Linux/WSL2 | Python, dependências e Java para Spark | Alternativa para executar Spark localmente sem containers |

A configuração inicial do Docker limita o serviço `app` a 6 GB, `metricas` a 256 MB e o Grafana a 512 MB. A JVM do Spark recebe 4 GB e usa `local[2]`. São valores iniciais para o notebook de 16 GB; o consumo da carga nacional ainda precisa ser medido.

Subir a aplicação inicia a API. Com `AGENDAMENTO_ATIVO=0`, você executa a importação manualmente. A API e as consultas de métricas funcionam antes da primeira importação.

## 2. Preparar o arquivo .env

Na primeira configuração:

```powershell
Copy-Item .env.example .env
```

Mantenha `.env.example` como modelo e edite `.env`. Se a cópia já existir, edite o arquivo existente. Ele está ignorado pelo Git.

Configurações principais:

| Variável | Padrão | Efeito |
|---|---|---|
| `MESES` | `4` | Mês atual e N-1 anteriores usados pelo agendador |
| `DOWNLOAD_WORKERS` | `3` | Quantidade de downloads simultâneos |
| `AGENDAMENTO_ATIVO` | `0` | `1` habilita as rotinas junto com a API |
| `SPARK_MASTER` | `local[2]` | Duas threads locais de execução Spark |
| `SPARK_DRIVER_MEMORY` | `4g` | Memória da JVM do Spark |
| `SPARK_SHUFFLE_PARTITIONS` | `16` no Compose | Partições de joins e agrupamentos |
| `DIRETORIO_DATA` | `./DATA` | Pasta do host montada em `/app/DATA` pelo Compose |
| `DIRETORIO_SPARK_TMP` | `spark-temporarios` | Volume Linux para cache/shuffle, ou caminho de host definido pelo operador |

As portas publicadas são configuradas em `compose.yaml`, na seção `ports`; o `.env.example` atual não possui uma variável de porta. Em uma cópia antiga do `.env`, troque `DIRETORIO_SPARK_TMP=./.spark-tmp` por `spark-temporarios` para usar o novo padrão Linux de temporários, mantendo `DIRETORIO_DATA` para reaproveitar seus dados existentes.

**No Docker:** Compose lê `.env` e preenche as variáveis declaradas em `compose.yaml`. Os padrões permitem iniciar o projeto mesmo sem `.env`.

**Com Python direto:** `main.py` consulta as variáveis do processo; ele não carrega `.env` automaticamente. Use `$env:NOME=...` no PowerShell ou `export NOME=...` no Linux, conforme a seção 5. `DIRETORIO_DATA` é uma configuração do Compose; a aplicação direta utiliza `DATA_DIR`.

## 3. Executar e testar com Docker

### 3.1. Construir e iniciar

```powershell
docker compose build app
docker compose up -d
docker compose ps
```

O serviço `app` deve ficar `running` e saudável. O Grafana inicia depois que o serviço `metricas` responde ao healthcheck. Na primeira criação, o Grafana também instala o plugin Infinity e provisiona dashboard e alerta.

Confira os três serviços:

```powershell
curl.exe -sS http://localhost:8000/saude
curl.exe -sS http://localhost:3000/api/health
```

Resultados esperados: API com `{"status":"ok"}` e Grafana com `"database":"ok"`.

### 3.2. Consultar o catálogo antes da carga

```powershell
docker compose exec app python main.py importar --meses 4 --simular
```

Esse comando usa a internet para listar competências e arquivos na Receita, sem baixar ZIPs nem alterar os manifestos. Confira `competencias`, `indisponiveis` e os arquivos de cada mês.

`--meses 4` inclui o mês atual e os três anteriores. Aceita também 6, 12, 36 ou outro inteiro positivo. Um mês indisponível é informado; o programa não o substitui por um mês mais antigo.

### 3.3. Executar a carga real

O comando abaixo começa a baixar os arquivos nacionais e a processar os meses selecionados:

```powershell
docker compose exec app python main.py importar --meses 4
```

O fluxo é:

```text
Mês mais antigo: Raw -> Bronze -> Prata -> Ouro -> concluir
Mês seguinte:   Raw -> Bronze -> Prata -> Ouro -> concluir
```

A saída da carga manual aparece no terminal do `exec`. Deixe o comando terminar para obter o resultado final.

Para conferir o resultado:

1. Verifique `status: sucesso` e a lista de competências concluídas na saída.
2. Abra os quatro `DATA/<camada>/Metadados.json` e confira estados, datas e eventos.
3. Confira Parquet em `DATA/Prata` e os cinco produtos mensais em `DATA/Ouro`.
4. Raw e Bronze podem estar sem ZIP/CSV: eles são descartados depois da etapa seguinte validada; os metadados permanecem.
5. Repita a importação e confira que origens concluídas e inalteradas não geram novos downloads.

Para reprocessar deliberadamente o intervalo, inclusive com nova baixa:

```powershell
docker compose exec app python main.py importar --meses 4 --forcar
```

### 3.4. Retomar pendências e acompanhar os serviços

```powershell
# Reativar a flag de manutenção e executar uma tentativa agora.
docker compose exec app python main.py reativar
docker compose exec app python main.py manutencao

# Consultar pendências sem baixar, mostrando também os meses fora da janela.
curl.exe -u "engenharia:Engenharia123!" "http://localhost:8000/operacao/pendentes?meses=1"

# Logs da API e, quando habilitado, do agendador.
docker compose logs -f app
docker compose logs -f grafana
```

`reativar` apenas altera a flag. `manutencao` executa a retomada; quando houver agendamento ativo, o próximo ciclo também poderá fazê-la. A verificação remota ocorre a cada 15 dias, e a manutenção fica ativa a cada 30 minutos enquanto houver pendências elegíveis. Falha na própria descoberta remota também é repetida pela manutenção. Meses fora da janela ficam registrados e não são baixados/processados até voltarem ao intervalo de uma solicitação.

`exec` usa o container existente. Como alternativa, `docker compose run --rm app python main.py importar --meses 4` cria um container temporário para executar o mesmo comando. Ambos usam o DATA montado e a trava compartilhada; somente um pipeline pode executar por vez.

### 3.5. Aplicar alterações e encerrar

```powershell
# Depois de alterar .env ou mapeamento de portas.
docker compose up -d --force-recreate

# Depois de editar código Python, que é copiado para a imagem.
docker compose up -d --build app

# Parar os serviços conservando seus containers.
docker compose stop

# Remover os containers/rede do projeto, conservando DATA e o volume do Grafana.
docker compose down
```

Um `restart` reaproveita a configuração existente. Recriar o container aplica variáveis e portas novas. Os comandos de encerramento acima conservam os dados persistidos.

## 4. Executar testes automatizados

### 4.1. Suíte completa no Docker

```powershell
docker compose run --rm -T -v "${PWD}/tests:/app/tests" -v "${PWD}/Grafana:/app/Grafana:ro" app python -B -m unittest discover -s tests -v
```

No Linux/WSL2, o equivalente é:

```bash
docker compose run --rm -T -v "$PWD/tests:/app/tests" -v "$PWD/Grafana:/app/Grafana:ro" app python -B -m unittest discover -s tests -v
```

As amostras são sintéticas. A integração sobe uma fonte HTTP/WebDAV local e percorre ZIP -> CSV -> Spark -> Parquet, sem baixar a base nacional. Os testes usam diretórios próprios e não precisam limpar seu histórico em DATA.

Na validação registrada em 16/11/2026, os **45 testes passaram no Docker**. A execução pode emitir avisos do Spark/JDK e de sockets nos testes; confira o resumo `OK`, o número de testes executados e eventuais testes ignorados.

O teste de contrato JavaScript do Infinity usa Node, ausente na imagem Python; nesse ambiente ele é ignorado. No host com Python e Node, valide os targets reais do dashboard com `python -B -m unittest discover -s tests -p test_grafana.py -v`.

Na revisão de 23/11/2026, a suíte encontrou 52 testes: 51 passaram no container em aproximadamente 100 segundos, incluindo geração/retomada da Ouro em amostras; um foi ignorado por exigir Node. Esse teste JavaScript passou separadamente no host. Essa validação não é uma medição do tempo de processamento da base nacional.

Na correção de métricas e desempenho de 23/11/2026, a suíte integrada encontrou **64 testes**: **63 passaram no Docker e 1 foi ignorado por exigir Node**. O teste do Grafana passou separadamente no Windows com Node. A execução integrada levou aproximadamente 370 segundos com heap de 1 GB, `local[1]` e duas partições, enquanto a carga real continuava em outro container. Incluiu o serviço de métricas somente leitura, cache concorrente/expiração, início sem DATA, auditoria da Prata, cinco produtos Ouro e recuperação de publicações. Avisos de sockets do PySpark permaneceram sem falhas. Esses tempos não medem o ganho da otimização sobre a base nacional.

### 4.2. Executar um grupo

```powershell
# Roles, autenticação, endpoints e concorrência da API.
docker compose run --rm -T -v "${PWD}/tests:/app/tests" app python -B -m unittest discover -s tests -p test_api.py -v

# Transformações e indicadores com Spark real.
docker compose run --rm -T -v "${PWD}/tests:/app/tests" app python -B -m unittest discover -s tests -p test_spark.py -v

# Pipeline completo com fonte HTTP local e retomada após falha.
docker compose run --rm -T -v "${PWD}/tests:/app/tests" app python -B -m unittest discover -s tests -p test_integracao.py -v
```

## 5. Executar diretamente com Python

### 5.1. Preparar o ambiente no Windows

Para acompanhar o ambiente da imagem, use Python 3.12 e Java 17. Spark 4.0.1 também documenta suporte a Java 21. Java deve estar no `PATH` ou indicado em `JAVA_HOME`; essa variável aponta para a instalação do JDK, não para sua pasta `bin`.

```powershell
py -3.12 --version
java -version

# Criar o ambiente virtual e instalar as versões do projeto.
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# Variáveis válidas para esta janela do PowerShell.
$env:AGENDAMENTO_ATIVO = "0"
$env:MESES = "4"
$env:DATA_DIR = Join-Path $PWD.Path "DATA"
$env:SPARK_MASTER = "local[2]"
$env:SPARK_DRIVER_MEMORY = "4g"
$env:SPARK_SHUFFLE_PARTITIONS = "16"
$env:SPARK_LOCAL_IP = "127.0.0.1"
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
```

Se `py -3.12` não estiver disponível, instale essa versão ou use o executável da instalação escolhida para criar o ambiente. Os comandos usam o Python da `.venv` explicitamente, dispensando sua ativação.

As credenciais de demonstração são os padrões da aplicação. Para substituí-las, declare no mesmo terminal, por exemplo, `$env:ANALISE_SENHA = "sua-senha"`, antes de iniciar a API. Repita as variáveis nos terminais usados para outros comandos.

### 5.2. Iniciar a API e executar comandos

Se o `app` do Docker estiver usando 8000, pare-o antes de usar a mesma porta:

```powershell
docker compose stop app

# A API fica em http://localhost:8000/docs. Ctrl+C encerra o servidor.
.\.venv\Scripts\python.exe main.py servir --host 127.0.0.1 --porta 8000
```

Em outro terminal na raiz, com as variáveis locais configuradas, execute:

```powershell
.\.venv\Scripts\python.exe main.py importar --meses 4 --simular

# Teste da API sem iniciar Spark nem baixar dados nacionais.
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -p test_api.py -v

# Carga real; exige Spark/Hadoop nativos funcionando neste Windows.
.\.venv\Scripts\python.exe main.py importar --meses 4

# Suíte completa, incluindo Spark real.
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
```

A API e a simulação não iniciam a JVM. As etapas Prata/Ouro e os testes Spark precisam de Java e das dependências nativas exigidas pelo Hadoop no Windows. Erros relacionados a `HADOOP_HOME`, `winutils` ou bibliotecas nativas indicam configuração desse ambiente. O pipeline completo foi validado em Linux/Docker; a execução completa nativa no Windows ainda não foi validada. Use Docker ou Linux/WSL2 para reproduzir o ambiente Linux dos testes.

### 5.3. Alternativa local Linux/WSL2, sem Docker

No terminal Linux, entre na raiz do projeto e tenha Python 3.12, suporte a `venv` e Java 17 ou 21 instalados:

```bash
python3.12 --version
java -version
python3.12 -m venv .venv-linux
.venv-linux/bin/python -m pip install -r requirements.txt

export AGENDAMENTO_ATIVO=0
export DATA_DIR="$PWD/DATA"
export SPARK_MASTER='local[2]'
export SPARK_DRIVER_MEMORY=4g
export SPARK_SHUFFLE_PARTITIONS=16
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv-linux/bin/python"

.venv-linux/bin/python main.py importar --meses 4 --simular
.venv-linux/bin/python -B -m unittest discover -s tests -v
.venv-linux/bin/python main.py servir --host 127.0.0.1 --porta 8000
```

Para a carga real, use `.venv-linux/bin/python main.py importar --meses 4` em outro terminal com as mesmas variáveis. A `.venv-linux` deve ser criada pelo Python do Linux; ela é diferente da `.venv` criada no Windows. Ambos os diretórios estão ignorados pelo Git. A receita local acima não foi executada nesta documentação; a validação integral registrada usou o container Linux.

## 6. Endereços da API e testes de permissão

Base padrão no notebook: [http://localhost:8000](http://localhost:8000).

| Método | Endpoint | Credencial/perfil | Resultado esperado |
|---|---|---|---|
| GET | `/saude` | Público | `200`, estado da API |
| GET | `/docs` | Público | Interface para experimentar os endpoints |
| GET | `/openapi.json` | Público | Contrato dos endpoints |
| GET | `/metricas/resumo` | `monitor` | `200`, métricas consolidadas |
| GET | `/metricas/execucoes` | `monitor` | `200`, eventos históricos |
| GET | `/camadas/Raw`, `/camadas/Bronze`, `/camadas/Prata` | `engenharia` | `200`, arquivos e estados da camada |
| GET | `/camadas/Ouro` | `analise` | `200`, arquivos e estados da Ouro |
| GET | `/arquivos/{camada}/{caminho}` | Perfil autorizado à camada | Arquivo físico existente, conforme o caminho listado |
| POST | `/operacao/reativar` | `engenharia` | Reativa a manutenção |
| GET | `/operacao/pendentes?meses=1` | `engenharia` | Lista o que falta e separa origens dentro/fora do intervalo |
| POST | `/operacao/importar?meses=4` | `engenharia` | `202`, inicia carga real em segundo plano; `409` se ocupado |

Exemplos no PowerShell:

```powershell
# Permissões válidas: HTTP 200.
curl.exe -i -u monitor:Monitor123! http://localhost:8000/metricas/resumo
curl.exe -i -u analise:Analise123! http://localhost:8000/camadas/Ouro
curl.exe -i -u engenharia:Engenharia123! "http://localhost:8000/camadas/Prata?competencia=2026-11"

# Credencial ausente: HTTP 401.
curl.exe -i http://localhost:8000/camadas/Ouro

# Usuário autenticado, mas sem acesso à camada: HTTP 403.
curl.exe -i -u analise:Analise123! http://localhost:8000/camadas/Prata

# Esta chamada só reativa a flag; não começa uma carga imediatamente.
curl.exe -i -X POST -u engenharia:Engenharia123! http://localhost:8000/operacao/reativar
```

No Linux use `curl` no lugar de `curl.exe` e coloque `usuario:senha` entre aspas simples. Para experimentar a carga via API, use o POST de importação na tela `/docs` com o perfil de engenharia. HTTP 202 significa que a execução foi aceita; acompanhe a conclusão pelos metadados e métricas.

## 7. Grafana: acessos, endpoints e métricas

| Acesso | Endereço padrão |
|---|---|
| Interface/login | [http://localhost:3000](http://localhost:3000) |
| Dashboard CNPJ | [http://localhost:3000/d/cnpj-operacional](http://localhost:3000/d/cnpj-operacional) |
| Saúde do Grafana | [http://localhost:3000/api/health](http://localhost:3000/api/health) |
| JSON do dashboard provisionado | [http://localhost:3000/api/dashboards/uid/cnpj-operacional](http://localhost:3000/api/dashboards/uid/cnpj-operacional) |

Login inicial do Grafana: `admin` / `admin`. O JSON do dashboard exige autenticação; a interface também exige login. A senha inicial pode já ter sido alterada na instalação. As credenciais da API são separadas das credenciais administrativas do Grafana.

```powershell
curl.exe -sS http://localhost:3000/api/health
curl.exe -sS -u admin:admin http://localhost:3000/api/dashboards/uid/cnpj-operacional
```

O Grafana consulta o serviço `metricas` com o usuário `monitor`, nos endpoints `/metricas/resumo` e `/metricas/execucoes`, pelo endereço interno `http://metricas:8000`. Ele compartilha o código da API, monta DATA somente para leitura e não inicia Spark, downloads ou agendador. Não publica porta no notebook. A API principal continua acessível em `http://localhost:8000`, com as mesmas rotas de métricas protegidas. O volume físico pode ter até 30 segundos de defasagem pelo cache; os estados e eventos são relidos a cada consulta.

O dashboard mostra downloads, falhas, pendências, duração, volumes, linhas e estado do agendamento. Antes da primeira carga, contadores zerados e tabelas sem eventos são esperados. São métricas operacionais do pipeline; os indicadores de negócio estão nos Parquets da Ouro.

### 7.1. Pipeline em Python local e monitoramento no Docker

Se a aplicação Python usar a mesma pasta DATA montada pelo Compose, o serviço de métricas pode acompanhá-la diretamente pelos JSONs e arquivos. Não é necessário mudar o datasource para o endereço do Windows.

1. Defina `DATA_DIR` no Python local e `DIRETORIO_DATA` no `.env` apontando para a mesma pasta.
2. Ao mudar do Docker para Python local, conclua a importação e pare o serviço `app` antes de usar a mesma porta ou iniciar outro pipeline.
3. Inicie a API Python conforme a seção 5.
4. Execute `docker compose up -d --no-deps metricas grafana` para manter o monitoramento no Docker, sem iniciar `app`.
5. Abra o dashboard. O datasource continua usando `http://metricas:8000` na rede interna.

Para voltar ao pipeline no Docker, encerre o Python local e execute `docker compose up -d app`. Nunca mantenha duas importações escrevendo no mesmo DATA.

### 7.2. Atualizar o monitoramento sem interromper a importação

Na raiz do repositório, para aplicar mudanças somente na coleta de métricas:

```powershell
docker compose build metricas
docker compose up -d --no-deps metricas
```

Se os arquivos provisionados do Grafana também mudaram, recarregue-os sem reiniciar os serviços. Estas são as credenciais públicas de demonstração; use as configuradas no seu ambiente:

```powershell
curl.exe -f -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/datasources/reload
curl.exe -f -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload
curl.exe -f -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload
```

Recarregue a página do dashboard no navegador. O serviço `app` e a JVM da importação continuam rodando. Evite usar `up --build app` ou recriar todos os serviços durante uma carga: isso encerra a execução em andamento.

O serviço de métricas não publica uma porta no host. Para testar diretamente dentro dele:

```powershell
docker compose exec metricas python -c "import requests; r=requests.get('http://localhost:8000/metricas/resumo',auth=('monitor','Monitor123!'),timeout=10); print(r.status_code); print(r.json())"
docker compose logs --tail 50 metricas grafana
```

Em 23/11/2026, com 480 Parquets publicados e a Ouro nacional em execução, a primeira consulta corrigida levou 3,0 segundos; 15 consultas simultâneas ao serviço concluíram em 2,4 segundos no total. Um lote contendo as consultas dos 16 painéis, enviado à API do Grafana, retornou HTTP 200 em 19,0 segundos na primeira validação e em 3,0 segundos na repetição após a atualização final, sem erros. São medições desse ambiente durante carga, não garantias de latência.

## 8. Mudar as portas

### 8.1. Aplicação no Docker

A sintaxe atual é `IP_DO_HOST:PORTA_NO_HOST:PORTA_NO_CONTAINER`:

```text
127.0.0.1:8001:8000
           │    └── porta da API dentro do container
           └─────── porta que você usa no notebook
```

Para publicar API em 8001 e Grafana em 3001, altere apenas as linhas `ports` dos serviços existentes em `compose.yaml`:

```yaml
# Trechos dos serviços existentes; conserve as demais configurações do arquivo.
services:
  app:
    ports:
      - 127.0.0.1:8001:8000
  grafana:
    ports:
      - 127.0.0.1:3001:3000
```

Depois aplique e confira:

```powershell
docker compose up -d --force-recreate
docker compose ps
curl.exe -sS http://localhost:8001/saude
curl.exe -sS http://localhost:3001/api/health
```

Os novos acessos serão `http://localhost:8001/docs` e `http://localhost:3001`. O Grafana continua consultando `http://metricas:8000`, pois a porta interna da API permaneceu 8000. O healthcheck também continua na porta interna 8000. Mantenha o endereço interno nas configurações do Grafana quando mudar somente a porta publicada no notebook.

### 8.2. Aplicação executada diretamente

Encerre a API anterior com Ctrl+C e inicie outra porta:

```powershell
.\.venv\Scripts\python.exe main.py servir --host 127.0.0.1 --porta 8001
```

No Linux/WSL2, use `.venv-linux/bin/python main.py servir --host 127.0.0.1 --porta 8001`. O acesso passa a ser `http://localhost:8001/docs`. Se houver Grafana conectado a essa API local, atualize as URLs das consultas conforme a seção 7.1.

## 9. Problemas comuns

### Identificar onde a carga está demorando

O painel de estado do Grafana passou a mostrar última etapa registrada, competência, arquivo e início da etapa, também disponíveis em `/metricas/resumo` com a credencial `monitor`. Os logs registram o começo e o fim das etapas. Se mostrar `prata`, a demora é de transformação/publicação; se mostrar `extracao`, ainda está descompactando/validando o ZIP. Os campos persistidos podem permanecer com o último valor depois de uma interrupção abrupta.

Na execução analisada de 18/11/2026, os dez arquivos de empresas tinham aproximadamente 75 minutos acumulados de transformação na Prata. A Ouro ainda não havia sido chamada, e o manifesto Bronze indicava extração interrompida de `Estabelecimentos0.zip`. Os tempos de downloads paralelos registrados por arquivo não equivalem ao tempo corrido total da execução.

Foram corrigidas as releituras de todas as colunas/deduplicações globais feitas só para preservar datas de inserção. Essa leitura agora projeta apenas chaves e `data_insercao`. Cache/shuffle também passaram ao volume Linux para evitar o custo de I/O no bind mount Windows/OneDrive. Isso reduz trabalho redundante; uma nova medição com a base completa ainda é necessária para estimar a duração final. A regra de concluir um mês antes do próximo continua vigente.

### Desempenho da Prata e da Ouro

Baixar/extrair ZIPs mede rede e descompressão. A Prata acrescenta leitura de todas as linhas, conversão de tipos, escolha entre duplicatas, preservação de datas, escrita Parquet e validação. A Ouro consolida candidatos entre origens, agrega relações e publica dimensões/fatos. O tempo de uma carga completa não é comparável diretamente ao de um script que apenas baixa e extrai.

A revisão de 23/11/2026 remove o cache intermediário do CSV textual na Prata: `Observation` conta as linhas enquanto a transformação é executada. Apenas o resultado normalizado permanece em cache em disco. O retorno/metadado passa a incluir `linhas_lidas`, além de `linhas` e `duplicidades`. A validação de chaves e a contagem da saída compartilham uma ação completa. A leitura de campos multilinha, os esquemas, as regras de qualidade e a preservação das datas permanecem nos testes.

Na Ouro, a escolha global do melhor candidato continua considerando todos os campos. Depois dela, o cache guarda somente as colunas consumidas pelos produtos. A base agregada comum dos fatos de resumo/perfil é reaproveitada. A validação dos Parquets gravados reúne contagem, chaves nulas e duplicadas em uma ação por produto, e o fato de múltiplas empresas aproveita a unicidade dos vínculos já consolidada. Os logs informam início/fim e duração por produto e validação; o mês continua sendo publicado inteiro depois das verificações.

As mudanças de Prata/Ouro só entram em vigor quando um processo novo carregar o código. Atualizar somente `metricas` não altera a JVM ou o Python de uma importação já iniciada. Conclua a carga atual antes de recriar `app`; os comandos da seção 7.2 atualizam apenas o monitoramento.

Para comparar alternativas de SQL:

| Alternativa | Consequência para este projeto |
|---|---|
| Spark SQL | Usa o mesmo motor dos DataFrames PySpark; mudar a sintaxe não elimina leituras, shuffles ou joins repetidos. |
| SQL sobre Parquet com DuckDB | Pode consultar os Parquets diretamente, sem carregar três tabelas em um servidor; requer comparar resultados, regras e desempenho antes de uma migração. |
| Prata em SQL Server/PostgreSQL | Pode facilitar consultas concorrentes e conexão do BI; acrescenta carga das tabelas, índices, logs e armazenamento no banco. Não há ganho de velocidade garantido apenas por usar SQL. |

O projeto mantém PySpark conforme a escolha inicial. Uma troca de motor precisa preservar candidatos por origem/competência, datas, histórico e publicação consistente; carregar apenas os vencedores sem essa lógica muda o comportamento de uma correção de origem. Antes de decidir, compare a mesma competência e os cinco produtos, incluindo o custo de carregar o banco. Não extrapole tempos de amostras para a base nacional.

Referências: [motor comum do Spark SQL/DataFrames](https://spark.apache.org/docs/4.0.1/sql-programming-guide.html), [métricas com Observation](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.Observation.html), [SQL sobre Parquet no DuckDB](https://duckdb.org/docs/2.0/data/parquet/overview).

#### Análise de melhoria de desempenho em 24/11/2026

Esta revisão usa o código atual, os manifestos e o diagnóstico da execução de 23/11. É uma análise: não modifica os algoritmos em produção nem reinicia a importação. O usuário encerrou os três serviços às 16h54 de 23/11 e pediu que permanecessem parados. O Docker Engine também ficou indisponível posteriormente. O campo persistido `executando=true` não comprova que há um processo vivo.

Os manifestos de outubro registram estas saídas da Prata:

| Entidade | Registros publicados nos fragmentos | Bytes Parquet | Duplicidades removidas dentro dos lotes | Transformação acumulada |
|---|---:|---:|---:|---:|
| Empresas | 70.517.437 | 2.214.961.719 | 1 | 32,59 min |
| Estabelecimentos | 73.809.379 | 6.204.386.482 | 0 | 116,92 min |
| Sócios | 28.495.639 | 3.302.773.006 | 322 | 37,58 min |

Esses totais somam os fragmentos e não provam unicidade entre origens. Remover a deduplicação global apenas porque há poucas duplicidades locais pode mudar os indicadores. Os tempos acima são eventos concluídos da Prata; não incluem download/extração ou a Ouro incompleta, e não são previsão para outra competência.

Na execução da Ouro, as duas tarefas inspecionadas estavam em `UnsafeExternalSorter.insertRecord` e `WindowGroupLimit`: a ordenação associada à consolidação estava consumindo CPU. Isso localiza trabalho caro, mas não prova, sozinho, skew ou falta de RAM. Na amostra anterior, aproximadamente 25% do tempo de CPU foi de coleta de memória; em outro instante o heap usava aproximadamente 1,37 GiB de 3,67 GiB comprometidos. A análise não demonstrou OOM. Na inspeção posterior à parada havia `dim_empresa` com `_SUCCESS` no staging, mas nenhuma competência Ouro publicada. O marcador foi gravado às 16h48min42s de 23/11, aproximadamente 55 minutos depois do início da etapa otimizada, às 15h53min32s. `_SUCCESS` confirma a escrita, não a validação nem a publicação do conjunto.

**Comparação controlada da deduplicação.** Um contêiner temporário sem montagem de DATA comparou o código atual com duas alternativas. Recursos: 1 CPU, limite de 2 GiB para o contêiner, heap de 1 GiB, Spark 4.0.1, `local[1]`, shuffle configurado em 16, duas partições de entrada e cache de entrada em disco. Os serviços do projeto estavam parados durante as medições.

O conjunto sintético tinha 1.000.000 de registros e 999.000 chaves de empresa: baixa proporção de duplicatas, textos, capital decimal, datas de auditoria e alguns nomes nulos. Cada alternativa escreveu a saída completa em Parquet temporário. A segunda rodada inverteu a ordem dos algoritmos. Os tempos não incluem a geração/materialização inicial da entrada nem a verificação posterior da saída.

| Estratégia experimental | Rodada 1 | Rodada 2 | Ordenações no plano inicial | Janelas no plano inicial |
|---|---:|---:|---:|---:|
| Código atual | 11,189 s | 5,278 s | 2 | 2 |
| Mesma ordenação nas duas chamadas de janela | 9,336 s | 8,384 s | 2 | 2 |
| `groupBy` com `min_by` e mínimo da inserção | 23,108 s | 7,938 s | 2 | 0 |

Houve equivalência linha a linha em uma amostra adicional de 64 registros de cada entidade, com chaves compostas, origens, competências, nulos e datas distintas. Na amostra de 1 milhão, todas as execuções produziram 999.000 linhas e a mesma soma de hashes de todas as colunas. Isso é evidência de equivalência nessas amostras; não substitui os contratos completos de correção de origem e publicação.

O `min_by` retirou as janelas, mas não as duas ordenações do plano inicial e não demonstrou ganho neste ensaio. A variante de duas chamadas de janela com a mesma ordenação também não se tornou uma única janela física. Uma segunda experiência, usando uma única projeção, não teve resultado final recuperável após a mudança de sessão/indisponibilidade do Docker; não foi usada para decidir. Não recomendar trocar o algoritmo apenas pela forma do código.

As duas rodadas mostram efeito de aquecimento e ordem. Não há repetição suficiente para significância estatística, nem foram reproduzidos os 70 milhões de empresas, o bind Windows/OneDrive, todas as distribuições reais ou os limites de spill da execução nacional. Os operadores registrados são do plano físico inicial; a quantidade efetiva de partições após AQE precisa ser medida separadamente.

**Melhorias recomendadas, em ordem de investigação:**

1. **Instrumentar produto, fase e jobs Spark antes de outra execução completa.** Hoje o log de início de `dim_empresa` inclui consolidação e preenchimento do cache antes da gravação. Usar grupos de jobs e registrar duração por tarefa, partições efetivas, entrada, shuffle, spill e GC; distinguir preparo, escrita, validação e publicação. UI/event log estão desligados em `criar_spark`. Permitir diagnóstico local com acesso restrito, retenção e revisão dos dados expostos. No Grafana, expor produto/fase e atualização recente, mantendo separado o estado persistido da vivacidade real. Não acrescentar `count()` apenas para produzir uma barra de progresso.

2. **Comparar o dimensionamento das partições na consolidação.** Começar com a configuração efetiva de 16 e comparar 64 e, se necessário, 128 com a mesma entrada e recursos. Mais partições dividem lotes; não criam mais núcleos. Conferir se o AQE volta a agrupá-las em poucas partições. Se isso ocorrer, medir um ensaio separado sem a consolidação automática, preservando o restante do AQE. Comparar duração máxima/mediana, spill, GC e bytes por tarefa. `repartition` depois da deduplicação não resolve a ordenação anterior; antes dela pode adicionar um shuffle redundante. Não tratar 64/128 como valores já aprovados por este benchmark.

3. **Evitar cache com um único consumidor e liberar os demais depois do último uso.** Em `processar_ouro`, estabelecimentos alimentam somente a agregação da base compartilhada; comparar sua execução sem o cache intermediário de 73,8 milhões de registros. Empresas e sócios têm múltiplos consumidores e precisam de avaliação diferente. A base comum já está persistida. Hoje os caches ficam retidos até o `finally`; antecipar liberações somente depois de a base e os consumidores necessários estarem materializados, sem provocar recomputação. Medir pico de disco e I/O, além do tempo.

4. **Comparar staging como única materialização da Prata.** Hoje há cache do resultado normalizado, validação e escrita em staging. Avaliar escrever o lote diretamente no staging, obter a observação da entrada e validar o Parquet antes da publicação. Isso pode retirar uma cópia em disco, mas grava também lotes que serão rejeitados por chave inválida. Preservar contagem de entrada, nulos, vazio válido, descarte após falha e a última publicação íntegra.

5. **Rever a preservação das datas entre arquivos.** `ler_insercoes` projeta poucas colunas, mas cada novo ZIP lê os fragmentos anteriores da entidade/competência. Dez origens implicam 45 participações acumuladas de fragmentos na construção inicial. Um índice estreito ou snapshot pode reduzir releituras, mas regravá-lo inteiro a cada ZIP mantém crescimento acumulativo. Ele também precisa acompanhar remoções, origem vazia, ausência total/reaparecimento e falhas entre publicação e manifesto. Não substituir os candidatos por somente os vencedores, nem usar um mapa Python nacional de chaves na RAM. Esta mudança tem maior complexidade e deve vir depois das medições da Ouro.

6. **Adicionar retomada por produto validado sem publicação parcial.** Associar checkpoints à assinatura da Prata e à versão do processamento. Produtos privados já validados podem ser reaproveitados somente com entradas idênticas; o conjunto público continua sendo trocado uma vez após os cinco produtos. Isso reduz perda de trabalho em interrupções, mas não acelera a primeira geração e exige invalidação/recuperação testadas. Um timeout futuro deve cancelar o grupo Spark e aguardar as tarefas terminarem; encerrar apenas a espera de um `Future` Python não cancela a JVM.

7. **Comparar outro motor somente com regras equivalentes.** Spark SQL usa o mesmo motor dos DataFrames e não elimina o trabalho por mudança de sintaxe. DuckDB sobre Parquet é um candidato para um ensaio local sem carregar três tabelas em outro servidor. SQL Server/PostgreSQL podem facilitar consultas concorrentes e BI, mas acrescentam carga, índices, logs e armazenamento. Comparar também competência, datas, capital decimal, nulos, identidade aproximada e seleção dos candidatos. Não recomendar migração de motor sem esse comparativo.

As referências de código para essa investigação são `PipelinePrata.deduplicar`, `ler_tabela`, `ler_insercoes`, `preservar_insercao`, `processar_arquivo` e `PipelineOuro.processar_ouro`. As projeções após deduplicar, a base compartilhada, a validação em uma ação e a contagem simples de vínculos já estavam implementadas; não são ganhos novos desta análise.

Referências técnicas: [partições e AQE no Spark 4.0.1](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html), [semântica de min_by](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.functions.min_by.html), [grupos de jobs Spark](https://spark.apache.org/docs/4.0.1/api/python/reference/api/pyspark.SparkContext.setJobGroup.html). O desempate de `min_by` precisa ser definido: a função não garante seleção estável em empates completos do critério.

| Sintoma | O que conferir |
|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop/Engine ligado e configurado para containers Linux |
| Porta já ocupada | `docker compose ps`; pare a API anterior ou altere a porta publicada/local |
| HTTP 401 | Usuário/senha corretos para aquele serviço; a senha do Grafana é diferente das roles da API |
| HTTP 403 | Perfil autorizado ao endpoint/camada solicitado |
| HTTP 409 na importação | Outra execução detém a trava; aguarde a conclusão |
| HTTP 422 no parâmetro `meses` | Use inteiro positivo, como 4 |
| API acessível, Grafana sem métricas | Teste `/metricas/resumo` com `monitor` e confira URL, credenciais e logs do Grafana |
| Valores zero antes de importar | Estado esperado quando os manifestos ainda não têm eventos de carga |
| ZIP/CSV não existe depois do sucesso | Descarte previsto de Raw/Bronze; consulte os metadados e os Parquets Prata/Ouro |
| Spark não inicia localmente | Versão/caminho do Java, Python da venv e dependências nativas; compare com o ambiente Docker validado |
| Mudança no código não aparece no container | Reconstrua com `docker compose up -d --build app` |
| Mudança no `.env` não aparece | Recrie o serviço; no Python direto declare as variáveis antes de iniciar o processo |

Para implantação remota, confira as instruções de HTTPS e volume criptografado em [CREDENCIAIS.md](CREDENCIAIS.md). A carga nacional completa e o dimensionamento por competência permanecem como validações de implantação.

## Referências dos comandos

- [Docker Compose: execução em container existente](https://docs.docker.com/reference/cli/docker/compose/exec/).
- [Docker Compose: leitura do .env](https://docs.docker.com/compose/how-tos/environment-variables/envvars/).
- [Docker Compose: serviços e mapeamento de portas](https://docs.docker.com/reference/compose-file/services/).
- [Spark 4.0.1: plataformas e versões de Java/Python](https://spark.apache.org/docs/4.0.1/index.html).
- [PySpark 4.0.1: instalação](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html).
- [Docker Desktop: comunicação entre host e containers](https://docs.docker.com/desktop/features/networking/).
- [Grafana: API de saúde](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/api-legacy/other/).
