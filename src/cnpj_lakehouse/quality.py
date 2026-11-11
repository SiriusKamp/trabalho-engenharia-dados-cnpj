import re
from typing import Any


def validate_silver(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Executa checks simples que podem bloquear a publicação da Gold."""

    checks = {
        "cnpj_basico_non_empty": all(bool(row.get("cnpj_basico")) for row in rows),
        "uf_two_chars": all(len(row["uf"]) == 2 for row in rows if "uf" in row),
        "documents_masked": all(
            re.fullmatch(r"\*+\d{4}", row["cnpj_cpf_socio"]) is not None
            for row in rows
            if "cnpj_cpf_socio" in row and row["cnpj_cpf_socio"]
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {"valid": not failed, "checks": checks, "failed": failed, "rows_checked": len(rows)}
