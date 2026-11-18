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

A configuração inicial do Docker limita o serviço `app` a 6 GB e o Grafana a 512 MB. A JVM do Spark recebe 4 GB e usa `local[2]`. São valores iniciais para o notebook de 16 GB; o consumo da carga nacional ainda precisa ser medido.

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
| `DIRETORIO_SPARK_TMP` | `./.spark-tmp` | Temporários Spark montados pelo Compose |

As portas publicadas são configuradas em `compose.yaml`, na seção `ports`; o `.env.example` atual não possui uma variável de porta.

**No Docker:** Compose lê `.env` e preenche as variáveis declaradas em `compose.yaml`. Os padrões permitem iniciar o projeto mesmo sem `.env`.

**Com Python direto:** `main.py` consulta as variáveis do processo; ele não carrega `.env` automaticamente. Use `$env:NOME=...` no PowerShell ou `export NOME=...` no Linux, conforme a seção 5. `DIRETORIO_DATA` é uma configuração do Compose; a aplicação direta utiliza `DATA_DIR`.

## 3. Executar e testar com Docker

### 3.1. Construir e iniciar

```powershell
docker compose build app
docker compose up -d
docker compose ps
```

O serviço `app` deve ficar `running` e saudável. O Grafana inicia depois que a API responde ao healthcheck. Na primeira criação, o Grafana também instala o plugin Infinity e provisiona dashboard e alerta.

Confira os dois serviços:

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

# Logs da API e, quando habilitado, do agendador.
docker compose logs -f app
docker compose logs -f grafana
```

`reativar` apenas altera a flag. `manutencao` executa a retomada; quando houver agendamento ativo, o próximo ciclo também poderá fazê-la. A verificação remota ocorre a cada 15 dias, e a manutenção fica ativa a cada 30 minutos enquanto houver pendências. Falha na própria descoberta remota também é repetida pela manutenção.

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
docker compose run --rm -T -v "${PWD}/tests:/app/tests" app python -B -m unittest discover -s tests -v
```

No Linux/WSL2, o equivalente é:

```bash
docker compose run --rm -T -v "$PWD/tests:/app/tests" app python -B -m unittest discover -s tests -v
```

As amostras são sintéticas. A integração sobe uma fonte HTTP/WebDAV local e percorre ZIP -> CSV -> Spark -> Parquet, sem baixar a base nacional. Os testes usam diretórios próprios e não precisam limpar seu histórico em DATA.

Na validação registrada em 16/11/2026, os **45 testes passaram no Docker**. A execução pode emitir avisos do Spark/JDK e de sockets nos testes; confira o resumo `OK`, o número de testes executados e eventuais testes ignorados.

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

O Grafana consulta a aplicação com o usuário `monitor`, nos endpoints `/metricas/resumo` e `/metricas/execucoes`. Na rede do Compose, o endereço é `http://app:8000`; no navegador do notebook é `http://localhost:8000`. `app` é o nome do serviço resolvido dentro da rede Docker.

O dashboard mostra downloads, falhas, pendências, duração, volumes, linhas e estado do agendamento. Antes da primeira carga, contadores zerados e tabelas sem eventos são esperados. São métricas operacionais do pipeline; os indicadores de negócio estão nos Parquets da Ouro.

### 7.1. Grafana no Docker consultando Python direto no Windows

Esta configuração opcional mantém somente o Grafana no Docker Desktop. Ela altera o endereço da fonte de métricas:

1. Pare o serviço `app` do Compose, conforme a seção 5.
2. Inicie a API local com `.\.venv\Scripts\python.exe main.py servir --host 0.0.0.0 --porta 8000`. O bind precisa aceitar conexões vindas do container.
3. Em `Grafana/datasource.yaml`, `Grafana/dashboard.json` e `Grafana/alertas.yaml`, substitua `http://app:8000` por `http://host.docker.internal:8000`. Isso inclui `allowedHosts` e todas as consultas. Se sua API local usar 8001, use essa porta no novo endereço.
4. Configure na API local o mesmo usuário/senha de monitoramento fornecido ao Grafana pelo Compose.
5. Execute `docker compose up -d --no-deps --force-recreate grafana`. `--no-deps` evita iniciar novamente o `app` do Docker.
6. Confira `/metricas/resumo` no host e abra o dashboard para validar a comunicação.

`host.docker.internal` é o endereço do host para containers no Docker Desktop. A configuração de rede de uma API dentro do WSL/Linux pode ser diferente e deve ser validada nesse ambiente. A ligação opcional acima não foi exercitada nesta documentação.

Ao voltar para a aplicação no Compose, restaure `http://app:8000` nos três arquivos, encerre a API local e recrie os serviços com `docker compose up -d --force-recreate`.

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

Os novos acessos serão `http://localhost:8001/docs` e `http://localhost:3001`. O Grafana continua consultando `http://app:8000`, pois a porta interna da API permaneceu 8000. O healthcheck também continua na porta interna 8000. Mantenha o endereço interno nas configurações do Grafana quando mudar somente a porta publicada no notebook.

### 8.2. Aplicação executada diretamente

Encerre a API anterior com Ctrl+C e inicie outra porta:

```powershell
.\.venv\Scripts\python.exe main.py servir --host 127.0.0.1 --porta 8001
```

No Linux/WSL2, use `.venv-linux/bin/python main.py servir --host 127.0.0.1 --porta 8001`. O acesso passa a ser `http://localhost:8001/docs`. Se houver Grafana conectado a essa API local, atualize as URLs das consultas conforme a seção 7.1.

## 9. Problemas comuns

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
