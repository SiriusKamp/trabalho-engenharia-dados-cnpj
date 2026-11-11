# Trabalho de Engenharia de Dados em Larga Escala - CNPJ

Starter repo local-first para o trabalho avaliativo da aula. O cenário escolhido é o CNPJ, com uma execução pequena e sintética que representa o fluxo completo:

```text
CSV de exemplo -> Bronze -> Silver -> Gold -> qualidade/métricas/auditoria
```

## Executar

Requer Python 3.11 ou superior. O demo não baixa dados nem exige PySpark:

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
python scripts/run_demo.py
```

Depois da execução, abra `artifacts/gold/answers.json`, `artifacts/quality.json`, `artifacts/metrics.json` e `artifacts/audit.json`.

## Caminho para os dados reais

1. Baixe um lote oficial do CNPJ e registre referência, data, tamanho e checksum.
2. Organize os CSVs em `datalake/cnpj/{empresas,estabelecimentos,socios}`.
3. Instale o extra opcional: `python -m pip install -e ".[spark]"`.
4. Adapte `jobs/cnpj_pyspark_job.py` ao layout efetivo do lote.
5. Meça volume, duração, partições, rejeições e custo.

Não coloque ZIPs, CSVs reais ou PII no Git.

## Documentação de estudo

- `docs/trilha-execucao.md`: passos em ordem para maximizar a pontuação.
- `docs/conceitos-visuais.md`: fluxos Mermaid para explicar os conceitos.
- `docs/evidencias-rubrica.md`: checklist dos 100 pontos.
- `docs/superpowers/specs/2026-11-11-cnpj-lakehouse-design.md`: decisões e limites da base.
- `jobs/cnpj_pyspark_job.py`: caminho de escala com PySpark.
