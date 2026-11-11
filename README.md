# Trabalho de Engenharia de Dados - CNPJ

Starter repo enxuto para o Cenário A da avaliação.

## Estrutura

```text
data/       dados de exemplo e destino dos ZIPs reais
docs/       trilha, conceitos e evidências da rubrica
scripts/    execução do demo e download do CNPJ
src/        pipeline local e caminho opcional PySpark
tests/      testes automatizados
```

## Executar o demo

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
python scripts/run_demo.py
```

## Baixar os ZIPs do CNPJ

Primeiro confira a lista de URLs sem baixar nada:

```powershell
python scripts/download_cnpj.py --data-ref 2026-01 --dry-run
```

Quando estiver certo da referência, execute:

```powershell
python scripts/download_cnpj.py --data-ref 2026-01
```

Os ZIPs serão gravados em `data/raw/cnpj/2026-01/`. O script não extrai nem transforma os arquivos: eles permanecem como Bronze. Para forçar novo download:

```powershell
python scripts/download_cnpj.py --data-ref 2026-01 --force
```

## Próximos passos da avaliação

- `docs/trilha-execucao.md`: ordem recomendada para pontuação.
- `docs/conceitos-visuais.md`: diagramas Bronze → Silver → Gold.
- `docs/evidencias-rubrica.md`: checklist dos critérios.
- `docs/decisoes-arquitetura.md`: perguntas e trade-offs.

O módulo `src/cnpj_lakehouse/pyspark_job.py` é opcional e deve ser usado somente depois de validar o layout do lote baixado.
