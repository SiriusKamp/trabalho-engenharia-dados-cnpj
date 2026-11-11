# Plataforma CNPJ Lakehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entregar um repositório inicial executável para o trabalho de Engenharia de Dados em Larga Escala, com pipeline local CNPJ e documentação orientada à rubrica.

**Architecture:** O demo local lê CSV sintético, preserva a Bronze, normaliza e mascara na Silver, agrega perguntas de negócio na Gold e registra qualidade, métricas e auditoria. Um job PySpark separado documenta a transição para o dataset real sem tornar a execução inicial dependente de Spark.

**Tech Stack:** Python 3.11+, biblioteca padrão para o demo, PySpark como caminho de escala, unittest para testes sem dependências obrigatórias, Mermaid para diagramas.

**Spec:** docs/superpowers/specs/2026-11-11-cnpj-lakehouse-design.md

## Global Constraints

- Não baixar dados externos durante o demo.
- Não expor identificadores de sócios na Silver ou Gold sem mascaramento.
- Reexecução deve ser idempotente.
- O projeto deve começar com dados sintéticos pequenos e deixar o caminho de escala explícito.
- Toda evidência deve apontar para um item da rubrica do slide de avaliação.

## Review Focus

- Reexecução do pipeline: deve substituir artefatos de uma execução anterior, sem duplicar linhas.
- Valores vazios e delimitadores: devem ser normalizados sem quebrar o CSV.
- PII de sócios: deve ser mascarada de forma determinística e não aparecer na Gold.
- Dados inválidos: a qualidade deve registrar falhas e impedir a publicação da Gold.
- Ausência de dependências opcionais: o demo deve continuar executável sem PySpark instalado.

### Task 1: Contratos, testes e dados de exemplo

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `data/sample/empresas.csv`
- Create: `data/sample/estabelecimentos.csv`
- Create: `data/sample/socios.csv`
- Create: `tests/test_pipeline.py`
- Create: `tests/test_security.py`

**Interfaces:**
- Os testes definirão as funções `normalize_text`, `mask_document`, `run_pipeline` e o formato mínimo dos resultados.

- [ ] **Step 1: Write the failing tests** para normalização, mascaramento, idempotência e ausência de PII na Gold.
- [ ] **Step 2: Run `PYTHONPATH=src python -m unittest discover -s tests -v`** e confirmar falha por módulos ainda inexistentes.
- [ ] **Step 3: Add package metadata, ignore rules and three small CSV fixtures.**
- [ ] **Step 4: Commit the red test contract.**

### Task 2: Implementar Bronze, Silver, Gold e segurança

**Files:**
- Create: `src/cnpj_lakehouse/__init__.py`
- Create: `src/cnpj_lakehouse/pipeline.py`
- Create: `src/cnpj_lakehouse/security.py`
- Modify: `tests/test_pipeline.py`
- Modify: `tests/test_security.py`

**Interfaces:**
- `normalize_text(value: str | None) -> str`
- `mask_document(value: str | None) -> str`
- `run_pipeline(input_dir: Path, output_dir: Path) -> dict[str, object]`

- [ ] **Step 1: Implement the smallest code that satisfies the failing tests.**
- [ ] **Step 2: Run `PYTHONPATH=src python -m unittest discover -s tests -v` and confirm green.**
- [ ] **Step 3: Refactor only duplicated path and CSV handling while keeping tests green.**
- [ ] **Step 4: Commit the executable local pipeline.**

### Task 3: Qualidade, métricas e auditoria

**Files:**
- Create: `src/cnpj_lakehouse/quality.py`
- Create: `src/cnpj_lakehouse/observability.py`
- Create: `tests/test_quality.py`
- Modify: `src/cnpj_lakehouse/pipeline.py`

**Interfaces:**
- `validate_silver(rows: list[dict[str, str]]) -> dict[str, object]`
- `build_run_metrics(...) -> dict[str, object]`
- `run_pipeline(...)` must write `quality.json`, `metrics.json` and `audit.json`.

- [ ] **Step 1: Write failing tests for a valid Silver and a rejected invalid Silver.**
- [ ] **Step 2: Run focused tests and confirm red.**
- [ ] **Step 3: Implement quality and observability artifacts.**
- [ ] **Step 4: Run `PYTHONPATH=src python -m unittest discover -s tests -v` and confirm green.**
- [ ] **Step 5: Commit quality and observability.**

### Task 4: CLI, PySpark extension and documentation

**Files:**
- Create: `scripts/run_demo.py`
- Create: `jobs/cnpj_pyspark_job.py`
- Create: `configs/local.json`
- Create: `README.md`
- Create: `docs/trilha-execucao.md`
- Create: `docs/conceitos-visuais.md`
- Create: `docs/evidencias-rubrica.md`

**Interfaces:**
- `python scripts/run_demo.py` invokes `run_pipeline` using the sample data and `artifacts/`.

- [ ] **Step 1: Add the demo entry point and run it.**
- [ ] **Step 2: Add the PySpark production-path template with Bronze/Silver/Gold contracts.**
- [ ] **Step 3: Add the execution trail, visual explanations, rubric evidence checklist and Mermaid diagrams.**
- [ ] **Step 4: Run tests and the demo from a clean artifacts directory.**
- [ ] **Step 5: Commit the initial repository.**

### Task 5: Final verification

**Files:**
- Verify all project files and Git status.

- [ ] **Step 1: Run `python -m pytest -q`.**
- [ ] **Step 2: Run `python scripts/run_demo.py` twice.**
- [ ] **Step 3: Inspect generated JSONL/JSON artifacts for counts, masking and no duplication.**
- [ ] **Step 4: Review the repository against the five rubric weights.**
