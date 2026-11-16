# Credenciais e permissões

Estas são credenciais públicas de demonstração, incluídas por solicitação do usuário. Os valores reais do servidor devem ser definidos no `.env`, que não é versionado.

| Acesso | Usuário | Senha de demonstração | Permissões |
|---|---|---|---|
| API: engenharia | `engenharia` | `Engenharia123!` | Raw, Bronze, Prata, metadados dessas camadas e operação do pipeline |
| API: análise | `analise` | `Analise123!` | Apenas listagem/download da Ouro |
| API: monitoramento | `monitor` | `Monitor123!` | Apenas métricas operacionais para o Grafana |
| Grafana: administração | `admin` | `admin` | Configuração de dashboards e datasource |

O serviço dentro do container executa como usuário Linux `cnpj` (UID 10001), sem senha de login. Ele possui acesso técnico às quatro camadas para executar o pipeline. Esse acesso não é uma credencial de usuário da API.

## Configuração

Copie `.env.example` para `.env` e ajuste:

```dotenv
ENGENHARIA_USUARIO=engenharia
ENGENHARIA_SENHA=Engenharia123!
ANALISE_USUARIO=analise
ANALISE_SENHA=Analise123!
MONITORAMENTO_USUARIO=monitor
MONITORAMENTO_SENHA=Monitor123!
GRAFANA_USUARIO=admin
GRAFANA_SENHA=admin
```

O datasource Grafana recebe a mesma credencial de monitoramento configurada na aplicação. Depois de alterar o `.env`, recrie os serviços com `docker compose up -d --force-recreate`. A senha inicial do Grafana é aplicada na primeira criação de seu banco; para uma instalação já inicializada, altere-a no próprio Grafana ou com seu comando administrativo, sem apagar o volume de dados.

## Exemplos locais

```powershell
curl.exe -u engenharia:Engenharia123! http://localhost:8000/camadas/Prata
curl.exe -u analise:Analise123! http://localhost:8000/camadas/Ouro
curl.exe -u monitor:Monitor123! http://localhost:8000/metricas/resumo
```

O usuário de análise deve receber HTTP 403 ao tentar ler Prata, Bronze ou Raw. O usuário de monitoramento não acessa arquivos nem dispara importações.

## Servidor

O Compose publica as portas apenas em localhost. Para acesso remoto, configure HTTPS com certificado válido no ambiente de publicação; HTTP Basic deve trafegar protegido por TLS. A API aceita `TLS_CERTIFICADO` e `TLS_CHAVE`; o Grafana aceita as variáveis `GRAFANA_PROTOCOLO`, `GRAFANA_CERTIFICADO` e `GRAFANA_CHAVE`.

O caminho mais simples com o Compose fornecido é terminar HTTPS no proxy do servidor, mantendo a comunicação interna entre Grafana e API em `http://app:8000`. Se optar por HTTPS nativo na API, ajuste também a URL e `allowedHosts` em `Grafana/datasource.yaml` e as URLs das consultas em `Grafana/dashboard.json` e `Grafana/alertas.yaml`. O certificado precisa ser confiável nos containers e válido para os nomes usados nas consultas e na verificação de saúde. Essa implantação HTTPS ainda deve ser validada no servidor.

Mantenha DATA e temporários Spark em volume criptografado (BitLocker no Windows ou LUKS no Linux). O Docker não criptografa automaticamente um bind mount. A configuração/validação desse volume pertence ao servidor e não será aplicada automaticamente ao computador pelo repositório. Limite acesso direto aos arquivos à conta do serviço e administradores do host; as roles da API não restringem administradores do sistema operacional.
