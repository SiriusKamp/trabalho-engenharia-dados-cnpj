# Arquitetura - CNPJ

Status: desenho aprovado e implementado; validação da carga nacional e da infraestrutura do servidor pendente.

## Objetivo e decisões confirmadas

Aplicação Python com PySpark e Parquet, executada em Docker junto com Grafana. O objetivo é um pipeline legível, com funções diretas e comentários em português, sem framework próprio de pipelines.

- Processar uma competência inteira antes de iniciar a próxima.
- Selecionar competências por `--meses N`, contando o mês atual e N-1 meses anteriores no calendário de São Paulo, e processar da mais antiga para a mais nova. O padrão de teste é 4; aceitar também 6, 12, 36 ou outro inteiro positivo. Informar competências ainda indisponíveis sem fabricar histórico nem substituí-las silenciosamente por meses mais antigos.
- Acrescentar as novas competências sem excluir as antigas da Prata ou da Ouro. Quatro meses é o tamanho do teste inicial, não uma política de retenção.
- Excluir ZIPs Raw após extração validada; excluir os CSVs Bronze somente depois da publicação validada na Prata. Manter os metadados de ambas.
- Notebook de desenvolvimento: 16 GB de RAM. Servidor futuro com espaço para o crescimento do histórico. Sem limite fixo de 70 GB no código.
- Capital social substitui as métricas de receita. Sócios são identificados aproximadamente por documento mascarado + nome normalizado.
- Idade estimada pelo ponto médio das faixas oficiais; faixa acima de 80 anos recebe 87. Código 0, desconhecido e pessoas jurídicas ficam fora da média.

## Estrutura enxuta

```text
DATA/
  Raw/Metadados.json
  Bronze/Metadados.json
  Prata/Metadados.json
  Ouro/Metadados.json
Scripts/
  Pipeline/
    BaixarArquivos.py
    PipelinePrata.py
    PipelineOuro.py
    Metadados.py
    Esquemas.py
  Agendador.py
  API.py
  Seguranca.py
Grafana/
tests/
main.py
Dockerfile
compose.yaml
requirements.txt
.env.example
README.md
ARQUITETURA.md
```

Os dados reais e os segredos ficam fora do Git. A estrutura antiga foi substituída, preservando o histórico Git. A implementação permanece no repositório local.

## Fluxo por competência

```text
Descobrir arquivos oficiais e versões
  -> baixar ZIPs novos ou modificados
  -> extrair na Bronze e validar
  -> normalizar e publicar na Prata
  -> consolidar as três tabelas e validar chaves
  -> atualizar dimensões e fatos da Ouro
  -> concluir a competência e iniciar a seguinte
```

Downloads concorrentes com limite inicial de 3; uma extração e um processamento Spark por vez. Uma fila pequena impede acumular todos os ZIPs e CSVs: arquivos da mesma competência podem avançar para a Prata enquanto outros são baixados. A Ouro aguarda todas as entidades necessárias daquela competência. Uma falha pendente impede avançar para outro mês.

Não criar um cluster para o notebook. Começar com Spark local[2], heap da JVM de 4 GB, limite de aproximadamente 6 GB para o container da aplicação e orçamento inicial de 8 GB para Docker/WSL. São configurações iniciais a medir, não garantias de consumo.

## Fonte e metadados

Consultar o compartilhamento público oficial por WebDAV, descobrir competências e arquivos, sem adivinhar URLs mensais. O primeiro escopo de transformação contém Empresas, Estabelecimentos e Sócios; os outros arquivos são catalogados e aparecem como fora desse escopo, sem fingir que foram processados. Não é necessária a tabela Simples para os três produtos Ouro definidos aqui.

Cada Metadados.json é um arquivo de controle da camada. Dentro de `arquivos`, usar `competencia/nome_do_arquivo` como chave e um objeto como valor: modificação remota, ETag, bytes, checksum, data de download/extração/publicação, estado, tentativas e último erro. Registrar também o histórico resumido das execuções e o estado do agendamento.

Comparar a versão remota com a versão efetivamente concluída, mantendo separado o horário local da carga. A ausência de um ZIP/CSV descartado após sucesso não significa que ele precisa ser baixado novamente.

Gravar os JSONs por substituição de um arquivo temporário, coordenada por um único responsável. A etapa só recebe sucesso depois da validação e publicação. Falhas mantêm a última versão válida e ficam pendentes para a próxima execução. Publicações usam staging temporário e recuperação; não manter cópias permanentes de versões.

## Prata e histórico

Parquet particionado por competência, com três tabelas lógicas: empresas, estabelecimentos e socios. A competência vem da pasta do lote, não de uma coluna mensal existente no cadastro.

| Tabela | Chave lógica |
|---|---|
| empresas | cnpj_basico + competencia |
| estabelecimentos | cnpj_completo + competencia |
| socios | cnpj_basico + documento_mascarado + nome_normalizado + competencia |

O vínculo societário é aproximado; não tentar reconstruir CPF nem apresentar a chave como identificação inequívoca. Para sócio sem documento, usar identificação aproximada por tipo, nome e país, registrando essa condição.

Aplicar o layout oficial completo das três entidades, preservar códigos e identificadores como texto, converter datas explicitamente e capital social para decimal. Metadados por linha: competencia, arquivo_origem, data_insercao, data_atualizacao, atualizado_por e identificador da execução.

Preferir, na deduplicação, nome preenchido, capital social preenchido quando aplicável e menor quantidade de campos de negócio nulos; usar desempate determinístico. Não escolher capital maior como se isso garantisse qualidade. Situação cadastral será preservada, sem uma preferência adicional não solicitada.

Os fragmentos físicos de Parquet mantêm a origem lógica do ZIP. Reprocessar uma origem substitui sua contribuição naquela competência, incluindo remoções. Os três DataFrames consolidados deduplicam também entre fragmentos; não descartar definitivamente candidatos de outra origem e depois perder a possibilidade de recalcular o vencedor. Essa organização é interna; não criar mais entidades de negócio.

Meses novos acrescentam partições; correções da mesma chave e competência substituem os valores daquela competência. Preservar os demais meses e a data de inserção de registros já conhecidos naquela competência. Os fragmentos afetados são publicados antes de marcar sucesso no manifesto; uma competência incompleta não é disponibilizada como Ouro concluída.

## Ouro

Dimensões pequenas para descrições: empresa e sócio, referenciadas pelos fatos e pela competência quando o atributo variar mensalmente. Não repetir nomes em todas as agregações. Os arquivos continuam em Parquet.

1. Resumo empresarial mensal: CNPJ básico + competência, quantidade de estabelecimentos, estabelecimentos ativos e sócios, além de capital social. As médias de capital social são calculadas por conjunto de empresas, sem multiplicar os valores pelo número de filiais ou sócios.
2. Sócios em múltiplas empresas: identificação aproximada do sócio + competência + quantidade distinta de empresas; publicar os casos com quantidade maior que 1. O nome e o documento mascarado ficam na dimensão correspondente.
3. Perfil empresarial mensal: CNPJ básico + competência + meses desde a abertura da matriz + idade média estimada dos sócios PF + capital social. Na ausência de abertura válida da matriz, o tempo fica nulo. Esse tempo representa idade desde a abertura, não duração efetiva em situação ativa. A média de capital social por grupo é derivada no consumo; para um único CNPJ/mês existe um valor de capital, não várias observações financeiras.

Antes dos joins, agregar estabelecimentos e sócios separadamente por empresa e competência para evitar multiplicação de contagens e valores. Calcular os indicadores na referência do mês, não no dia em que uma carga histórica for executada. Conservar nulos em métricas desconhecidas, sem transformá-los em zero.

Recalcular somente competências cuja Prata teve alteração concluída. Comparar as versões concluídas das dependências no Metadados.json; datas por linha são para rastreabilidade, não substituem esse controle. Os três produtos do mês precisam terminar antes de a competência ser marcada como concluída.

## Agendamento e operação

Um agendador na aplicação, sem Airflow: verificar a fonte a cada 15 dias e executar a sequência mensal necessária, incluindo o intervalo inicial solicitado. Uma única execução por vez.

O job de manutenção verifica pendências locais de Prata/Ouro a cada 30 minutos enquanto estiver ativo. Se tudo estiver atualizado e sem erros, fica inativo. Downloads novos o reativam; falhas o mantêm ativo. Persistir esse estado para sobreviver a reinício. Disponibilizar uma função explícita de reativação e execução manual, sem exigir reset do projeto.

Uma consulta remota que falhou também é repetida pela manutenção. Competências pendentes fora do intervalo ficam registradas para auditoria, mas não são executadas; só voltam a ser elegíveis se estiverem na janela de uma nova solicitação. A mesma regra filtra catálogos persistidos usados pela manutenção, inclusive após a virada do mês. A assinatura da Ouro inclui a publicação de cada dependência Prata, permitindo retomar uma republicação local interrompida. A API reserva a trava global antes de responder que uma importação foi aceita.

`GET /operacao/pendentes` permite à engenharia consultar essas pendências sem rede ou Spark. A leitura de datas antigas na Prata projeta somente chaves e `data_insercao`, sem refazer a deduplicação de qualidade de todo o mês a cada ZIP. Cache e shuffle usam um volume Linux nomeado por padrão; DATA permanece montado no host para consumo dos arquivos. No servidor, o armazenamento dos temporários também precisa estar sobre disco protegido conforme o requisito de criptografia.

## Observabilidade

Grafana com dashboard provisionado, consultando resumos JSON pelo plugin Infinity com parser backend. O serviço `metricas` executa o mesmo código da API em modo somente leitura: não inicia Spark/agendador, não expõe operações e monta DATA como somente leitura. Sua atualização não exige reiniciar o serviço `app`, que pode estar importando. Não há um novo framework ou banco de monitoramento.

Falhas, etapas e eventos são lidos dos manifestos a cada consulta. Apenas a ocupação física tem cache de 30 segundos compartilhado entre os painéis, com uma única varredura concorrente. Em Linux, a enumeração usa descritores de diretórios, sem seguir symlinks ou contar staging/backups. No Windows nativo permanece a validação de junctions. Não ler o conteúdo dos Parquets para montar métricas operacionais.

Mostrar downloads por execução, falhas por etapa, duração média por arquivo, extração, transformação e publicação, bytes ocupados em cada camada, bytes transferidos/processados acumulados, última extração, registros lidos e publicados, duplicidades e erros de qualidade. Ocupação atual de Raw/Bronze pode ser zero depois do descarte; o histórico preserva o volume que passou por elas. Alertas de falha e carga pendente usam esses estados.

## Acesso e criptografia

Controle de acesso efetivo na API com usuários e duas roles de dados: engenharia opera o pipeline e consulta Raw/Bronze/Prata; analise consulta Ouro. A identidade técnica do serviço pode ler e gravar as quatro camadas. O Grafana usa uma credencial restrita às métricas, sem acesso aos arquivos das outras camadas. Configurar e testar negativas de acesso; documentar uma matriz de permissões não é suficiente.

Consultar uma camada na primeira versão significa listar tabelas/arquivos por competência, consultar seu estado e baixar os arquivos autorizados. Raw/Bronze descartadas continuam visíveis pelos metadados, sem prometer download de conteúdo inexistente. A API não oferece SQL arbitrário nem inicia Spark por requisição; os indicadores já estão calculados na Ouro. Essa definição mantém o acesso simples e verificável sem construir uma aplicação de consultas adicional.

Os diretórios são acessíveis no servidor apenas à conta do serviço e à administração do host. Administradores do host continuam tendo acesso privilegiado; as roles da API não revogam permissões administrativas do sistema operacional. Não expor diretórios ou a interface Spark diretamente na rede.

Para criptografia em repouso, usar volume criptografado do sistema operacional para DATA e temporários Spark: BitLocker no Windows ou LUKS no Linux. É um requisito de configuração do servidor, a ser documentado e verificado, não uma capacidade fornecida pelo Docker Compose. Não criar um formato próprio de arquivos criptografados incompatível com a leitura Parquet. O Spark pode também criptografar seus arquivos temporários de shuffle/spill com a opção apropriada.

Usar HTTPS para a fonte e para os acessos remotos à aplicação/Grafana, com TLS configurado no ambiente de publicação. Segredos e certificados ficam fora do Git. Grafana pode iniciar localmente com admin/admin como solicitado; a configuração do servidor aceita credenciais próprias. Não registrar documentos ou nomes de sócios nos logs operacionais.

Documentar no repositório as credenciais públicas de demonstração de engenharia, analise e monitoramento, além de admin/admin do Grafana, e as variáveis para substituí-las no servidor. Não versionar credenciais privadas do servidor. A API oferece acesso autenticado a arquivos e metadados, sem mecanismo de consulta SQL.

## Validação e limites do desenho

Testar com amostras: esquema, conversão decimal/data, deduplicação, competências distintas, correção de origem com remoção, reexecução sem duplicação, falha com retomada, metadados concorrentes e bloqueio por role. Depois executar uma competência real e medir RAM, disco e duração antes de importar as quatro. Testes com amostra não serão apresentados como teste de toda a base.

Não adicionar exclusão automática de meses antigos, prazo de retenção, limite fixo de disco, banco transacional externo ou cluster Spark. O serviço `metricas` compartilha a imagem/código existente e separa somente a execução do monitoramento para permitir atualizá-lo durante uma carga.

## Alternativas e referências

PySpark + Parquet foi escolhido pelo usuário. DuckDB foi considerado pelo menor custo operacional, mas descartado em favor de PySpark e DataFrames. Parquet por competência e origem atende à publicação por arquivo sem acrescentar Delta Lake/Iceberg nesta primeira versão. Essas tecnologias ficam fora do escopo inicial.

- [Layout oficial do CNPJ](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).
- [Compartilhamento oficial do CNPJ](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9).
- [Configuração do Spark](https://spark.apache.org/docs/latest/configuration.html).
- [Segurança e criptografia de temporários Spark](https://spark.apache.org/docs/latest/security.html).
- [Grafana Infinity: JSON por API e alertas](https://grafana.com/docs/plugins/yesoreyeram-infinity-datasource/latest/).
