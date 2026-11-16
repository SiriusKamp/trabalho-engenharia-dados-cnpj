# Estado do projeto e trilha do trabalho avaliativo

## Implementado

- Aplicação Python com PySpark e Docker, organizada em DATA, Scripts, Grafana e tests.
- Importação por `--meses N`: mês atual e N-1 anteriores, com padrão 4 e sem teto fixo.
- Consulta ao catálogo WebDAV oficial, comparando modificação, tamanho e ETag.
- Até três downloads concorrentes e apenas uma competência em processamento por vez.
- Verificação de tamanho/ZIP, checksum durante a baixa e extração segura com validação CRC.
- Metadados JSON por camada, histórico de execuções, estados de falha e retomada.
- Descarte de Raw após extração válida e Bronze após publicação válida na Prata.
- Três tabelas Prata em Parquet com esquema completo, tipos explícitos e deduplicação determinística.
- Contribuição por arquivo de origem para refletir correções e remoções sem apagar outras competências.
- Ouro mensal com dimensões de empresas/sócios e três fatos; pré-agregação evita multiplicar valores nos joins.
- Identificação aproximada de sócios; idade pelo ponto médio das faixas e valor 87 para maiores de 80.
- Rotina de verificação a cada 15 dias, manutenção ativa a cada 30 minutos, reativação manual e trava de execução.
- API com roles de engenharia, análise e monitoramento; credenciais de demonstração documentadas.
- Dashboard Grafana, fonte Infinity e alerta de falha pendente.
- Criptografia dos temporários Spark; instruções para volume criptografado e HTTPS no servidor.

Os metadados existentes no repositório são modelos vazios. Não foi baixada a base nacional durante o desenvolvimento.

## Como testar as quatro competências

1. Construir os containers conforme o README.
2. Executar `docker compose run --rm app python main.py importar --meses 4 --simular`.
3. Conferir competências disponíveis, fontes e espaço do servidor.
4. Executar `docker compose run --rm app python main.py importar --meses 4`.
5. Conferir metadados e indicadores após o primeiro mês; o programa continuará nos meses seguintes em ordem.
6. Repetir o comando para demonstrar que arquivos concluídos e inalterados não são baixados novamente.
7. Conferir os dashboards, a auditoria e os resultados quantitativos.

Quatro meses é apenas o recorte de teste. Cada mês novo é acrescentado ao histórico; não existe exclusão automática de meses antigos. O programa não fabrica competências que a Receita não disponibiliza.

## Evidências para a avaliação

| Critério | Peso | Evidência |
|---|---:|---|
| Arquitetura | 20% | ARQUITETURA.md, fluxo das camadas, escolha de PySpark e trade-offs |
| Pipeline funcional | 30% | Execução real de Raw até Ouro, três tabelas Prata e cinco produtos Ouro |
| Segurança | 20% | Testes das roles, mascaramento, credenciais externas e comprovação de TLS/volume criptografado do servidor |
| Observabilidade | 15% | Dashboard, alerta, metadados de falha/retomada, duração e volumes |
| Apresentação técnica | 15% | Demonstração de 20 minutos, resultados e justificativa das decisões |

## Próximos passos no servidor

- Configurar volume criptografado para DATA e temporários; essa proteção depende do host, não é fornecida por um bind mount Docker.
- Substituir credenciais de demonstração no `.env` e configurar HTTPS para acesso remoto.
- Preparar permissões do diretório para o usuário técnico UID 10001 e limitar acesso externo aos serviços.
- Medir tempo, RAM e disco em uma competência real antes de dimensionar o histórico de 6, 12 ou 36 meses.
- Definir `AGENDAMENTO_ATIVO=1` quando a rotina automática puder começar a baixar dados.
- Produzir a análise de negócio e os slides a partir dos números reais da Ouro.

## Limites de interpretação

Capital social não representa faturamento. A idade média é estimada por faixa. A identificação de sócios por documento mascarado e nome pode ter colisões. Tempo desde abertura da matriz não é tempo efetivamente em situação ativa. Os fatos têm competência mensal extraída do lote publicado; as linhas originais do cadastro não contêm esse histórico por si só.

[Material da disciplina e rubrica](https://static.plataforma.grupoa.education/ucv_prod/posunisales/ce245987-95d0-4774-8bf0-0767c9c56cd5.html#45).
