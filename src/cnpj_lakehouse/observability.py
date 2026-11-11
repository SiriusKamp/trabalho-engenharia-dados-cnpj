from datetime import datetime, timezone
from typing import Any


def build_run_metrics(
    bronze_rows: dict[str, int],
    silver_rows: dict[str, int],
    gold_rows: dict[str, list[dict[str, Any]]],
    status: str = "success",
) -> dict[str, Any]:
    return {
        "status": status,
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "gold_result_counts": {name: len(rows) for name, rows in gold_rows.items()},
    }


def build_audit_event(status: str, metrics: dict[str, Any], quality: dict[str, Any]) -> dict[str, Any]:
    return {
        "event": "cnpj_pipeline_run",
        "status": status,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
        "quality": quality,
    }
