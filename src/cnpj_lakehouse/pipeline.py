import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from .observability import build_audit_event, build_run_metrics
from .quality import validate_silver
from .security import mask_document


def normalize_text(value: str | None) -> str:
    """Padroniza texto para contratos simples da camada Silver."""

    return (value or "").strip().upper()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream, delimiter=";"))


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _to_float(value: str | None) -> float:
    try:
        return float((value or "0").replace(",", "."))
    except ValueError:
        return 0.0


def run_pipeline(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    """Executa o fluxo Bronze -> Silver -> Gold em uma pasta local."""

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    raw_empresas = _read_csv(input_dir / "empresas.csv")
    raw_estabelecimentos = _read_csv(input_dir / "estabelecimentos.csv")
    raw_socios = _read_csv(input_dir / "socios.csv")

    _write_jsonl(output_dir / "bronze" / "empresas.jsonl", raw_empresas)
    _write_jsonl(output_dir / "bronze" / "estabelecimentos.jsonl", raw_estabelecimentos)
    _write_jsonl(output_dir / "bronze" / "socios.jsonl", raw_socios)

    empresas = [
        {
            "cnpj_basico": row["cnpj_basico"].strip(),
            "razao_social": normalize_text(row["razao_social"]),
            "natureza_juridica": row["natureza_juridica"].strip(),
            "capital_social": _to_float(row["capital_social"]),
            "porte": row["porte"].strip(),
        }
        for row in raw_empresas
    ]
    estabelecimentos = [
        {
            "cnpj_basico": row["cnpj_basico"].strip(),
            "cnpj_ordem": row["cnpj_ordem"].strip(),
            "cnpj_dv": row["cnpj_dv"].strip(),
            "nome_fantasia": normalize_text(row["nome_fantasia"]),
            "situacao_cadastral": int(row["situacao_cadastral"] or 0),
            "uf": normalize_text(row["uf"]),
            "cnae_fiscal_principal": row["cnae_fiscal_principal"].strip(),
        }
        for row in raw_estabelecimentos
    ]
    socios = [
        {
            "cnpj_basico": row["cnpj_basico"].strip(),
            "nome_socio": normalize_text(row["nome_socio"]),
            "cnpj_cpf_socio": mask_document(row["cnpj_cpf_socio"]),
            "qualificacao_socio": row["qualificacao_socio"].strip(),
            "faixa_etaria": row["faixa_etaria"].strip(),
        }
        for row in raw_socios
    ]

    _write_jsonl(output_dir / "silver" / "empresas.jsonl", empresas)
    _write_jsonl(output_dir / "silver" / "estabelecimentos.jsonl", estabelecimentos)
    _write_jsonl(output_dir / "silver" / "socios.jsonl", socios)

    quality_report = validate_silver(empresas + estabelecimentos + socios)
    _write_json(output_dir / "quality.json", quality_report)
    if not quality_report["valid"]:
        raise ValueError(f"Silver reprovada nos checks: {quality_report['failed']}")

    ativos = [row for row in estabelecimentos if row["situacao_cadastral"] == 2]
    top_ufs = [
        {"uf": uf, "total_estabelecimentos": total}
        for uf, total in sorted(Counter(row["uf"] for row in ativos).items(), key=lambda item: (-item[1], item[0]))
    ]
    distribuicao_porte = [
        {"porte": porte, "total": total}
        for porte, total in sorted(Counter(row["porte"] for row in empresas).items(), key=lambda item: (-item[1], item[0]))
    ]
    faixa_socios = [
        {"faixa_etaria": faixa, "total": total}
        for faixa, total in sorted(Counter(row["faixa_etaria"] for row in socios).items())
    ]
    gold_rows = {"top_ufs": top_ufs, "porte": distribuicao_porte, "faixa_socios": faixa_socios}
    _write_json(output_dir / "gold" / "answers.json", gold_rows)

    metrics = build_run_metrics(
        {"empresas": len(raw_empresas), "estabelecimentos": len(raw_estabelecimentos), "socios": len(raw_socios)},
        {"empresas": len(empresas), "estabelecimentos": len(estabelecimentos), "socios": len(socios)},
        gold_rows,
    )
    _write_json(output_dir / "metrics.json", metrics)
    _write_json(output_dir / "audit.json", build_audit_event("success", metrics, quality_report))

    return {
        "bronze_rows": {"empresas": len(raw_empresas), "estabelecimentos": len(raw_estabelecimentos), "socios": len(raw_socios)},
        "silver_rows": {"empresas": len(empresas), "estabelecimentos": len(estabelecimentos), "socios": len(socios)},
        "gold_rows": gold_rows,
    }
