1. Encontrar o motivo do dash do grafana "Downloads por Importação" estar dando o erro "Cant acess property method" 
2. Quero saber se há uma forma de verificar os pendentes e caso sim, também gostaria de alterar o codigo para que não baixe os pendentes a menos que eles se encontrem dentro da faixa de meses solicitada novamente
3. Na documentação aparentemente as camadas silver e ouro estão gravando competencia como string em vez de data, no caso de socios faixa etaria esta como string em vez de inteiro teria motivo ?
4. Deixei horas e não passou do 2026-08, não passou pra ouro, mesmo que eu tenha colocado 2 no meses ele tentou baixar 2026-08 

# Pipeline CNPJ - Python, PySpark e Grafana

Pipeline mensal com histórico Parquet e controle incremental por arquivo. Processa uma competência por vez, da mais antiga para a mais nova.

Veja [TUTORIAIS.md](TUTORIAIS.md) para executar e testar com Docker ou Python direto, acessar os endpoints da API/Grafana e alterar portas.

```text
Receita Federal -> Raw (ZIP) -> Bronze (CSV) -> Prata (Parquet) -> Ouro (dimensões e fatos)
                         Metadados.json de cada camada -> API -> Grafana
```

## Começar com Docker

Requer Docker Desktop/Engine com containers Linux. A imagem inclui Python 3.12, Java 17 e PySpark 4.0.1; não precisa configurar Hadoop/winutils no Windows.

Na raiz do repositório:

```powershell
Copy-Item .env.example .env
docker compose build app
docker compose up -d
```

Abra o Grafana em `http://localhost:3000` com `admin` / `admin`. A API está em `http://localhost:8000/docs`. Todas as credenciais de demonstração e permissões estão em [CREDENCIAIS.md](CREDENCIAIS.md).

O `.env.example` é o modelo versionado. Faça uma cópia chamada `.env` e edite essa cópia; se ela já existir, edite a configuração existente. O Compose lê `.env` automaticamente para preencher as variáveis declaradas em `compose.yaml`. Neste projeto ele também funciona sem `.env`, usando os padrões do Compose. O Python chamado diretamente fora do Docker consulta as variáveis do processo e não carrega esse arquivo automaticamente.

O agendamento começa desabilitado para permitir conferir a configuração antes de baixar a base. No servidor, defina `AGENDAMENTO_ATIVO=1` no `.env` e recrie a aplicação com `docker compose up -d --force-recreate app`. A primeira verificação será imediata; as seguintes ocorrerão a cada 15 dias.

## Executar com o container já ligado

Subir o serviço `app` inicia a API. Com `AGENDAMENTO_ATIVO=0`, a importação começa quando você chama o comando manualmente. Na raiz do repositório:

```powershell
# Conferir serviços e portas publicados.
docker compose ps

# Executar dentro do app existente; consulta o catálogo sem iniciar a carga.
docker compose exec app python main.py importar --meses 4 --simular

# Iniciar Raw -> Bronze -> Prata -> Ouro para o intervalo escolhido.
docker compose exec app python main.py importar --meses 4

# Acompanhar os logs da API e, quando habilitado, do agendador.
docker compose logs -f app
```

A saída da carga iniciada com `exec` aparece no próprio terminal desse comando. `exec` reaproveita o container ligado; `run --rm`, usado nos exemplos abaixo, cria um container temporário para executar o mesmo comando com a configuração do serviço. Ambos usam o mesmo diretório DATA e a mesma trava de pipeline.

## Configurar portas e aplicar alterações

As portas ficam em `compose.yaml`, em `ports` de cada serviço. A sintaxe usada é `IP_DO_HOST:PORTA_NO_HOST:PORTA_NO_CONTAINER`:

| Serviço | Mapeamento atual | Acesso pelo notebook |
|---|---|---|
| API | `127.0.0.1:8000:8000` | `http://localhost:8000/docs` |
| Grafana | `127.0.0.1:3000:3000` | `http://localhost:3000` |

Por exemplo, altere a linha de `app` para `127.0.0.1:8001:8000` e a de `grafana` para `127.0.0.1:3001:3000`. Depois acesse `http://localhost:8001/docs` e `http://localhost:3001`. Você mudou as portas publicadas no computador; a comunicação interna Grafana -> API continua usando `http://app:8000`. As portas 8001/3001 são apenas exemplos; o arquivo fornecido permanece em 8000/3000.

```powershell
# Após editar .env ou os mapeamentos de porta, recriar os serviços.
docker compose up -d --force-recreate

# Após editar código Python, reconstruir a imagem que contém esse código.
docker compose up -d --build app
```

Um simples `restart` reutiliza a configuração do container existente. Recriar aplica as variáveis/portas novas e conserva os dados montados em DATA e no volume nomeado do Grafana.

## Escolher quantos meses importar

```powershell
# Consulta somente os nomes e versões na fonte; não baixa nem altera os manifestos.
docker compose run --rm app python main.py importar --meses 4 --simular

# Carga inicial: um mês percorre todo o pipeline antes do próximo.
docker compose run --rm app python main.py importar --meses 4

# O mesmo comando aceita 6, 12, 36 ou outro inteiro positivo.
docker compose run --rm app python main.py importar --meses 12
```

`--meses 4` inclui o mês atual e os três anteriores, no calendário de São Paulo. Em novembro de 2026, seleciona agosto a novembro. Meses não publicados são informados como indisponíveis e não são substituídos por meses mais antigos. A fonte pode não conservar os 36 meses solicitados; o programa só pode importar competências efetivamente disponíveis.

O parâmetro controla o intervalo de verificação/importação. Ele nunca apaga meses antigos da Prata/Ouro. O valor `MESES=4` no `.env` é usado pelo agendador; um comando manual pode sobrescrevê-lo com `--meses`.

## Atualização e recuperação

- Comparar modificação remota, ETag e tamanho com a versão concluída registrada no manifesto.
- Baixar no máximo `DOWNLOAD_WORKERS` arquivos simultaneamente; extrair e transformar um por vez.
- Excluir o ZIP somente após extração validada; excluir o CSV somente após publicação validada na Prata.
- Conservar metadados mesmo após descarte; arquivo já consumido não é baixado novamente apenas porque foi removido.
- Reprocessar uma origem substitui sua contribuição da competência, incluindo registros removidos. Outras competências e candidatos de outras origens permanecem.
- Gravar falhas por etapa e manter os arquivos necessários para a retomada. A Ouro só é atualizada após concluir a Prata necessária daquele mês.
- Concluir pendências de uma janela anterior antes dos meses novos, mesmo após a virada do mês ou uma redução de `--meses`.
- Detectar republicações locais da Prata, invalidar ZIPs corrompidos e retomar descartes interrompidos.

```powershell
# Repetir a importação verifica versões e retoma pendências.
docker compose run --rm app python main.py importar --meses 4

# Forçar nova baixa/extração/publicação do intervalo selecionado.
docker compose run --rm app python main.py importar --meses 4 --forcar

# Reativar e executar a manutenção sobre o catálogo já conhecido.
docker compose run --rm app python main.py reativar
docker compose run --rm app python main.py manutencao
```

Quando o agendamento está habilitado, a manutenção é verificada a cada 30 minutos enquanto houver pendências. Ela fica inativa quando tudo termina sem erros e é reativada por uma nova carga ou pelo comando manual. Uma trava compartilhada entre processos impede duas cargas simultâneas.

A manutenção reutiliza o catálogo conhecido. Quando a consulta ao portal falha, ela tenta consultar novamente no ciclo de 30 minutos. A API reserva a trava antes de aceitar uma importação e responde HTTP 409 se a CLI ou o agendador já estiver executando.

## Dados e indicadores

A primeira versão transforma os layouts completos de **Empresas, Estabelecimentos e Sócios**. Arquivos auxiliares são catalogados como fora do escopo, sem afirmar que foram ingeridos.

| Camada | Conteúdo |
|---|---|
| `DATA/Raw` | ZIPs temporários e histórico de downloads |
| `DATA/Bronze` | CSVs temporários e histórico de extrações |
| `DATA/Prata` | Três tabelas lógicas por competência e origem |
| `DATA/Ouro` | Duas dimensões e três fatos por competência |

Na Prata, `PipelinePrata.ler_tabela(...)` consolida fragmentos e deduplica por chave. Códigos permanecem texto, capital social é decimal e datas são convertidas explicitamente. Os fragmentos físicos preservam candidatos de diferentes origens para uma correção não perder registros válidos.

A Ouro publica:

- `dim_empresa` e `dim_socio`: nomes e atributos descritivos.
- `fato_resumo_empresarial`: estabelecimentos, ativos, sócios e capital social por empresa/mês.
- `fato_socios_multiplas_empresas`: quantidade distinta de empresas por sócio aproximado/mês, quando maior que um.
- `fato_perfil_empresarial`: meses desde a abertura da matriz, idade média estimada dos sócios PF e capital social.

Capital social não é faturamento. A média por conjunto de empresas é calculada sobre os fatos, sem multiplicação por filiais/sócios. O documento mascarado + nome identifica sócios aproximadamente. Faixas etárias usam o ponto médio; acima de 80 usa 87; faixa desconhecida e pessoas jurídicas são excluídas da média. O tempo desde a abertura não representa tempo efetivamente em situação ativa.

## Observabilidade e segurança

O Grafana consulta apenas os resumos operacionais da API: downloads por execução, duração por etapa, falhas, pendências, volumes atuais e acumulados, linhas e última extração. O dashboard e o alerta de falha são provisionados em `Grafana/`.

As roles são aplicadas na API, com bloqueio de acesso indevido testado. As portas do Compose ficam em localhost. A implantação remota requer HTTPS e volume criptografado para DATA e temporários Spark; veja [CREDENCIAIS.md](CREDENCIAIS.md). O código habilita a criptografia dos temporários de shuffle do Spark. O Compose não configura BitLocker/LUKS nem certificados do servidor automaticamente.

## Testar

```powershell
# Todas as dependências vêm da imagem; amostras sintéticas, sem baixar CNPJ real.
docker compose run --rm -v "${PWD}/tests:/app/tests" app python -B -m unittest discover -s tests -v
```

Configuração inicial: JVM com 4 GB, container da aplicação até 6 GB e Grafana até 512 MB. Medir uma competência real antes de projetar RAM/disco para o histórico completo. Não foi executada uma carga nacional completa durante a implementação.

## Arquivos principais

`main.py` é a entrada única. `Scripts/Pipeline/` contém download, esquemas, Prata, Ouro e metadados; `Scripts/Agendador.py` coordena as etapas; `Scripts/API.py` e `Scripts/Seguranca.py` controlam o acesso.

Para estudar os comentários, siga esta ordem:

1. [main.py](main.py): argumentos do terminal, variáveis de ambiente e escolha do comando.
2. [Agendador.py](Scripts/Agendador.py): fluxo mensal, concorrência dos downloads, pendências e retomada.
3. [Metadados.py](Scripts/Pipeline/Metadados.py): estado atual, eventos históricos, travas e publicação recuperável.
4. [BaixarArquivos.py](Scripts/Pipeline/BaixarArquivos.py): WebDAV, versões remotas, download em blocos e validação ZIP/CRC.
5. [Esquemas.py](Scripts/Pipeline/Esquemas.py): colunas oficiais, chaves, tipos e faixas etárias.
6. [PipelinePrata.py](Scripts/Pipeline/PipelinePrata.py): DataFrames, execução por ações, normalização, deduplicação e Parquet por origem.
7. [PipelineOuro.py](Scripts/Pipeline/PipelineOuro.py): dimensões, fatos, pré-agregações e validação dos indicadores.
8. [Seguranca.py](Scripts/Seguranca.py) e [API.py](Scripts/API.py): autenticação, roles, arquivos autorizados e métricas do Grafana.
9. [Dockerfile](Dockerfile), [compose.yaml](compose.yaml) e [.env.example](.env.example): imagem, serviços, volumes, portas e configuração.

- [ARQUITETURA.md](ARQUITETURA.md): decisões aprovadas.
- [Endpoints_Documentacao.MD](Endpoints_Documentacao.MD): rotas HTTP, permissões, downloads, métricas e limites dos controles de jobs.
- [Parquets_Documentacao.MD](Parquets_Documentacao.MD): tipos, chaves, relacionamentos e três visões com histórico de cinco meses.
- [Layout oficial da Receita Federal](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).
