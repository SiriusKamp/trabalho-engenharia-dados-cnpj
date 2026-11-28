# Endpoints: funções, permissões e exemplos

Este documento descreve as rotas implementadas em [Scripts/API.py](Scripts/API.py) e as permissões de [Scripts/Seguranca.py](Scripts/Seguranca.py). Os comandos de instalação e execução estão em [TUTORIAIS.md](TUTORIAIS.md); o dicionário dos dados está em [Parquets_Documentacao.MD](Parquets_Documentacao.MD).

## 1. Endereço e autenticação

No Compose fornecido, a API é publicada em [http://localhost:8000](http://localhost:8000). Se alterar a porta publicada ou usar um servidor, substitua a URL base nos exemplos. O Grafana é outro serviço, publicado em [http://localhost:3000](http://localhost:3000).

```powershell
$api = "http://localhost:8000"
```

As rotas protegidas usam **HTTP Basic**, com usuário e senha em cada chamada. No servidor remoto, use o endereço HTTPS configurado para a aplicação. Os nomes abaixo são os usuários de demonstração; variáveis de ambiente podem substituí-los. Senhas e configuração estão em [CREDENCIAIS.md](CREDENCIAIS.md).

| Perfil | Usuário de demonstração | O que pode fazer |
|---|---|---|
| Engenharia | `engenharia` | Listar/baixar Raw, Bronze e Prata; disparar importação e reativar manutenção |
| Análise | `analise` | Listar/baixar somente Ouro |
| Monitoramento | `monitor` | Consultar somente métricas e histórico operacional |
| Administração do Grafana | `admin` | Administrar o Grafana; essa credencial não autentica na API CNPJ |

A identidade técnica do pipeline pode processar as quatro camadas. Isso é diferente das permissões concedidas aos usuários HTTP. As roles da API também não substituem as permissões de acesso direto aos arquivos no sistema operacional.

## 2. Matriz de acesso

“Público” significa que essas rotas também podem ser consultadas pelos usuários autenticados, sem uma role específica.

| Método | Rota | Quem pode acessar | O que faz |
|---|---|---|---|
| GET | `/saude` | Público | Responde se a API está disponível |
| GET | `/docs` | Público | Abre a documentação interativa Swagger |
| GET | `/redoc` | Público | Abre a documentação de referência ReDoc |
| GET | `/openapi.json` | Público | Entrega o contrato OpenAPI da aplicação |
| GET | `/docs/oauth2-redirect` | Público | Rota auxiliar gerada pelo Swagger; não é uma operação do pipeline |
| GET | `/metricas/resumo` | Monitoramento | Retorna contadores, volumes, durações e estado operacional resumido |
| GET | `/metricas/execucoes` | Monitoramento | Retorna eventos históricos das etapas |
| GET | `/camadas/Raw` | Engenharia | Lista arquivos/metadados da Raw |
| GET | `/camadas/Bronze` | Engenharia | Lista arquivos/metadados da Bronze |
| GET | `/camadas/Prata` | Engenharia | Lista arquivos/metadados da Prata |
| GET | `/camadas/Ouro` | Análise | Lista arquivos/metadados da Ouro |
| GET | `/arquivos/Raw/{arquivo}` | Engenharia | Baixa um arquivo físico autorizado da Raw |
| GET | `/arquivos/Bronze/{arquivo}` | Engenharia | Baixa um arquivo físico autorizado da Bronze |
| GET | `/arquivos/Prata/{arquivo}` | Engenharia | Baixa um arquivo físico autorizado da Prata |
| GET | `/arquivos/Ouro/{arquivo}` | Análise | Baixa um arquivo físico autorizado da Ouro |
| POST | `/operacao/importar` | Engenharia | Inicia uma importação real em segundo plano |
| POST | `/operacao/reativar` | Engenharia | Define a manutenção como ativa nos metadados |
| GET | `/operacao/pendentes` | Engenharia | Lista pendências conhecidas e separa o que está dentro/fora da janela solicitada |

As quatro rotas de camada são variações de `/camadas/{camada}`; as quatro rotas de download são variações de `/arquivos/{camada}/{arquivo:path}`. Os nomes de camada são `Raw`, `Bronze`, `Prata` e `Ouro`, com essa capitalização.

Engenharia não recebe acesso HTTP à Ouro; análise não recebe acesso à Prata; monitoramento não recebe acesso a arquivos nem às operações. A documentação pública mostra as rotas, mas a execução das rotas protegidas exige a identidade apropriada.

## 3. Listar uma camada

```http
GET /camadas/{camada}?competencia=AAAA-MM
```

| Parâmetro | Local | Obrigatório | Significado |
|---|---|---|---|
| `camada` | Caminho | Sim | Uma das quatro camadas permitidas |
| `competencia` | Consulta | Não | Restringe a listagem a um mês no formato `AAAA-MM` |

```powershell
curl.exe -u "engenharia:Engenharia123!" "$api/camadas/Prata?competencia=2026-11"
curl.exe -u "analise:Analise123!" "$api/camadas/Ouro?competencia=2026-11"
```

O resultado é **JSON de controle**, contendo `camada`, `competencia`, `arquivos` e `estado`. Em `arquivos` podem aparecer:

- Registros lógicos do manifesto: estado, versão, datas, quantidade de linhas, bytes e caminho publicado, conforme a camada.
- Arquivos físicos acessíveis: caminho relativo à camada, tamanho e `disponivel: true`.
- Origens descartadas: os metadados permanecem, com `disponivel: false` quando não há conteúdo físico.

Na Ouro, um registro lógico pode representar toda a competência; os arquivos individuais aparecem com caminhos como `competencia=2026-11/fato_resumo_empresarial/part-...parquet`. Para baixar, escolha uma chave de arquivo físico terminado em `.parquet`, e não apenas a chave mensal do manifesto.

O campo `estado` é preenchido somente na listagem Raw. Nas demais camadas ele é `{}`. O estado público expõe `meses`, `manutencao_ativa`, `proxima_verificacao` e uma versão resumida de `ultimo_resultado`.

Também expõe `etapa_atual`, `competencia_atual`, `arquivo_atual` e `etapa_inicio`. Esses campos mostram o último estágio registrado; após uma interrupção abrupta podem permanecer no manifesto até a próxima execução. Na conclusão ou erro tratado, o estágio é finalizado como `concluido` ou `erro`.

**Essas chamadas não executam a Prata ou a Ouro e não retornam as linhas dos Parquets.** Elas consultam arquivos e manifestos existentes, sem abrir uma sessão Spark.

## 4. Baixar um Parquet

```http
GET /arquivos/Ouro/{arquivo}
```

`arquivo` é o caminho relativo à camada, incluindo as subpastas. Exemplo ilustrativo:

```text
competencia=2026-11/fato_resumo_empresarial/part-00000-IDENTIFICADOR.parquet
```

O nome real é gerado pelo Spark e deve ser copiado da listagem. Ele não é fixo e pode mudar após uma republicação.

```powershell
# Primeiro obtenha a lista de arquivos reais.
curl.exe -u "analise:Analise123!" "$api/camadas/Ouro?competencia=2026-11"

# Cole a chave de um arquivo .parquet retornado nessa lista.
$caminho = Read-Host "Caminho relativo do arquivo na Ouro"
$nomeLocal = Split-Path $caminho 
curl.exe -f -u "analise:Analise123!" "$api/arquivos/Ouro/$caminho" -o $nomeLocal
```

A resposta transmite um arquivo por chamada, em blocos, com `Content-Type: application/octet-stream` e cabeçalho de download. Uma tabela Spark pode ter vários `part-*.parquet`; reúna todas as partes daquele produto/competência para consumir sua tabela completa.

A API atual não monta ZIP de uma pasta, não oferece SQL e não converte a tabela completa para JSON. Manifestos, locks, staging, backups e caminhos fora da camada não são disponibilizados pelo endpoint de arquivos. Raw/Bronze descartadas depois do processamento continuam no manifesto, mas seu download deixa de existir.

## 5. Iniciar uma importação

```http
POST /operacao/importar?meses=5
```

| Parâmetro | Tipo | Padrão | Regra |
|---|---|---|---|
| `meses` | Inteiro | `4` | Maior que zero; inclui o mês atual e N-1 anteriores |

O endpoint não exige corpo JSON. Ele usa a identidade autenticada como responsável pela auditoria e executa a sequência mensal Raw -> Bronze -> Prata -> Ouro.

**O comando abaixo inicia uma carga real:**

```powershell
curl.exe -i -X POST -u "engenharia:Engenharia123!" "$api/operacao/importar?meses=5"
```

Uma aceitação devolve HTTP **202**, por exemplo:

```json
{"status": "aceito", "meses": 5, "usuario": "engenharia"}
```

202 significa que a execução foi aceita, não que os cinco meses terminaram. Acompanhe a conclusão pelas métricas e pelos manifestos autorizados. Se outro processo já estiver com a trava do pipeline, a resposta é **409**.

Não existe parâmetro HTTP implementado para `simular` ou `forcar` nessa rota. Acrescentar `simular=true` à URL não transforma essa chamada em simulação. Para consultar somente o catálogo, use o comando CLI `python main.py importar --meses 5 --simular`, conforme os tutoriais.

Fechar o navegador, cancelar a espera do cliente ou encerrar a conexão HTTP não solicita o cancelamento da thread em segundo plano. Não há endpoint de cancelamento implementado.

## 6. Reativar manutenção e entender os jobs

### Consultar pendências sem executar

```powershell
curl.exe -u "engenharia:Engenharia123!" "$api/operacao/pendentes?meses=1"
```

`meses` é opcional e deve ser positivo. Sem ele, a consulta usa a última quantidade persistida, ou quatro meses como padrão. A consulta usa apenas catálogo/manifestos já conhecidos e existência dos caminhos; não consulta o gov, baixa arquivos ou inicia Spark.

| Campo | Significado |
|---|---|
| `competencias_solicitadas` | Janela calculada para a consulta |
| `total_arquivos_pendentes` | Origens ainda não concluídas, inclusive meses fora da janela |
| `arquivos_elegiveis` | Arquivos pendentes dentro do intervalo |
| `arquivos_fora_intervalo` | Arquivos preservados na auditoria, mas fora do intervalo |
| `arquivos` | Competência, ZIP, entidade, próxima etapa e estados Raw/Bronze/Prata de cada origem |
| `ouro` | Competências aguardando Prata, entidades ou publicação Ouro, com indicador de elegibilidade |

O catálogo permite mostrar também arquivos que ainda não começaram. Essa é uma listagem de controle do pipeline, não acesso da engenharia às linhas ou downloads da Ouro.

O inventário conhecido de cada mês define suas origens ativas. Uma origem retirada desse inventário continua nos registros de auditoria da camada, mas não cria uma nova pendência nem muda a assinatura esperada da Ouro na consulta. Quando uma competência pendente continua dentro da janela e não aparece na nova descoberta, a retomada pode usar seu catálogo persistido; falhas dessa tentativa ficam registradas normalmente.

Na execução e na manutenção, arquivos de meses fora da janela não são baixados nem transformados. Eles voltam a ser elegíveis quando uma solicitação incluir sua competência novamente; registros e arquivos já existentes continuam preservados. Consultar `pendentes?meses=1` só muda o recorte exibido: para executar esse recorte, use `POST /operacao/importar?meses=1`.

### Reativar a manutenção

```http
POST /operacao/reativar
```

```powershell
curl.exe -X POST -u "engenharia:Engenharia123!" "$api/operacao/reativar"
```

Essa operação persiste `manutencao_ativa: true` e retorna o estado gravado. Ela não inicia imediatamente uma carga, não força download de arquivos e não liga o agendador geral.

| Controle | Onde é definido | Efeito |
|---|---|---|
| `AGENDAMENTO_ATIVO` | Ambiente na inicialização da API | `1` inicia a thread das rotinas; `0` mantém execução automática desligada |
| `manutencao_ativa` | Estado Raw/Metadados.json | Habilita a tentativa de retomada no ciclo de manutenção |
| Trava do pipeline | Arquivo compartilhado entre processos | Impede importações simultâneas |

Com o agendador iniciado, a verificação remota é feita a cada 15 dias, e a manutenção tenta pendências a cada 30 minutos enquanto estiver ativa. Ao concluir tudo sem erro, a manutenção fica inativa. Falhas a mantêm ativa; a própria descoberta remota também pode ser repetida na retomada.

Uma importação manual por HTTP funciona mesmo com `AGENDAMENTO_ATIVO=0`. Já chamar `reativar` com o agendador desligado apenas muda a flag; é necessário executar a manutenção por CLI ou iniciar o serviço com o agendamento habilitado.

**Limites atuais:** não há rotas HTTP para simulação, manutenção imediata, desativação do agendador, cancelamento da execução ou consulta detalhada da execução atual. Após uma interrupção abrupta, o campo interno `executando` pode permanecer desatualizado no manifesto; ele não é um endpoint de estado confiável nem prova de que a trava continua ocupada.

## 7. Métricas e histórico

```powershell
curl.exe -u "monitor:Monitor123!" "$api/metricas/resumo"
curl.exe -u "monitor:Monitor123!" "$api/metricas/execucoes"
```

| Informação | Campos principais de `/metricas/resumo` | Interpretação |
|---|---|---|
| Situação atual | `status`, `pendencias`, `falhas_atuais` | Pendências/falhas ainda abertas |
| Auditoria acumulada | `falhas`, `eventos_total`, `execucoes_total` | Histórico, incluindo tentativas já recuperadas |
| Downloads | `downloads_total`, `bytes_transferidos`, `importacoes` | Baixas registradas e agrupamento por execução |
| Ocupação atual | `bytes_camadas`, `bytes_total` | Arquivos de dados presentes no disco |
| Movimento histórico | `bytes_historicos`, `bytes_historicos_camadas` | Bytes que passaram pelas etapas, inclusive arquivos já descartados |
| Duração | `duracao_media_s`, `duracao_total_s`, `etapas` | Tempos dos eventos; somas de tarefas paralelas não equivalem ao tempo corrido da carga |
| Registros | `linhas_total`, `linhas_camadas`, `duplicidades`, `erros_qualidade` | Contadores que foram registrados nas etapas |
| Atualização e rotina | `ultima_extracao`, `estado` | Última extração concluída e estado público resumido |

`/metricas/execucoes` devolve a lista dos eventos com camada, execução, competência, etapa, estado e medidas disponíveis. Mensagens livres de erro são substituídas por uma indicação genérica; os detalhes ficam nos registros restritos.

São métricas de operação. Os indicadores empresariais e as visões históricas estão nos Parquets da Ouro, não nesses endpoints.

A ocupação (`bytes_camadas`/`bytes_total`) é medida uma vez a cada 30 segundos e compartilhada entre as consultas concorrentes. Falhas, pendências, etapas e eventos continuam sendo lidos dos manifestos em cada chamada. A medição não conta staging, backups, metadados nem links.

No Compose, o Grafana usa `http://metricas:8000`, um serviço interno somente de leitura que executa a mesma implementação dos endpoints acima. Ele oferece saúde, documentação e as duas rotas de métricas; rotas de arquivos ou operação retornam 404 e o agendador permanece desabilitado. As rotas da API pública em `localhost:8000` continuam disponíveis com a role `monitor`.

## 8. Grafana

| Acesso | Endereço padrão | Autenticação |
|---|---|---|
| Interface | [http://localhost:3000](http://localhost:3000) | Conta do Grafana, inicialmente `admin` / `admin` |
| Dashboard CNPJ | [http://localhost:3000/d/cnpj-operacional](http://localhost:3000/d/cnpj-operacional) | Login do Grafana |
| Saúde | [http://localhost:3000/api/health](http://localhost:3000/api/health) | Público na configuração local |
| JSON do dashboard | [http://localhost:3000/api/dashboards/uid/cnpj-operacional](http://localhost:3000/api/dashboards/uid/cnpj-operacional) | Conta do Grafana com permissão de leitura |

O datasource usa a conta `monitor` da API e consulta `http://metricas:8000/metricas/resumo` e `/metricas/execucoes` pela rede interna Docker. Alterar somente a porta publicada no notebook não muda esse endereço interno. Para rodar a API fora do Docker, consulte a configuração específica dos tutoriais.

## 9. Códigos HTTP para diagnóstico

| Código | Significado neste projeto |
|---|---|
| 200 | Consulta, download ou reativação concluída |
| 202 | Importação aceita em segundo plano |
| 401 | Credencial ausente ou inválida |
| 403 | Perfil/caminho sem autorização |
| 404 | Camada inexistente ou arquivo indisponível |
| 409 | Pipeline ocupado por outra execução |
| 422 | Parâmetro inválido, como `meses=0` ou competência fora do formato |
| 503 | Manifesto temporariamente indisponível ou inválido na consulta |
