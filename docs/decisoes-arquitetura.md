# Decisões de arquitetura - Cenário CNPJ

## Perguntas de negócio da primeira versão

| Pergunta | Fonte | Métrica | Consumidor |
|---|---|---|---|
| Quais UFs concentram mais estabelecimentos ativos? | Estabelecimentos | contagem por UF com situação 2 | análise regional |
| Como se distribuem os portes? | Empresas | contagem por código de porte | perfil empresarial |
| Qual a distribuição de faixa etária dos sócios? | Sócios | contagem por faixa | perfil societário |

## Escolhas e trade-offs

| Decisão | Motivo | Custo/limite |
|---|---|---|
| PySpark para a escala real | 60+ milhões de registros exigem processamento distribuído | mais custo operacional e necessidade de particionar |
| Bronze/Silver/Gold | separa auditoria, tratamento e consumo | exige contratos entre camadas |
| Parquet no caminho de escala | leitura colunar e compressão | não é o CSV original e exige catálogo/contrato |
| Mascaramento antes da Gold | reduz exposição de PII | limita análises que precisem do identificador completo |
| Batch mensal | acompanha a atualização do CNPJ | não atende cenários de baixa latência |

## Premissas de segurança

- O bruto não deve ser usado diretamente por consumidores de negócio.
- Logs não carregam CPF/CNPJ de sócios em claro.
- Chaves e credenciais reais ficam fora do Git.
- Em cloud, IAM, criptografia e retenção devem ser implementados no serviço escolhido e demonstrados com evidência.
