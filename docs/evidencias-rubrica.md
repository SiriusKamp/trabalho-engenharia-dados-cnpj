# Matriz de evidências da avaliação

| Peso | O professor precisa ver | Evidência no repo | Como demonstrar |
|---:|---|---|---|
| 20% | Componentes, fluxos, tecnologias e trade-offs | `docs/conceitos-visuais.md`, diagrama final e decisões | Explicar uma escolha e o custo/benefício dela |
| 30% | Ingestão → Bronze → Silver → Gold ponta a ponta | `src/cnpj_lakehouse/pipeline.py`, `src/cnpj_lakehouse/pyspark_job.py`, `artifacts/` | Executar `python scripts/run_demo.py` e abrir os artefatos |
| 20% | IAM, criptografia, mascaramento de PII e auditoria | `src/cnpj_lakehouse/security.py`, `quality.json`, `audit.json` | Mostrar CPF/CNPJ mascarado e teste de não exposição |
| 15% | SLIs, alertas e testes de qualidade | `quality.json`, `metrics.json`, `tests/` | Rodar testes e provocar uma falha de qualidade controlada |
| 15% | Apresentação técnica de 20 min + arguição | `docs/trilha-execucao.md` e roteiro da apresentação | Seguir a ordem: problema, arquitetura, pipeline, segurança, resultados |

## Checklist antes da entrega

- [ ] Data de referência e fonte registradas.
- [ ] Diagrama contém fluxo, componentes, tecnologias e trade-offs.
- [ ] Bronze mantém o bruto e Silver aplica o contrato.
- [ ] Gold responde três perguntas com números reproduzíveis.
- [ ] Reexecução não duplica registros.
- [ ] PII está mascarada na Silver e ausente da Gold.
- [ ] Testes e métricas foram executados em uma segunda rodada.
- [ ] Apresentação cabe em 20 minutos e inclui limitações/custos.
