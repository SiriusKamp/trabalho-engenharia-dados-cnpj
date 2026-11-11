import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cnpj_lakehouse.pipeline import run_pipeline  # noqa: E402


def main() -> None:
    config = json.loads((ROOT / "configs" / "local.json").read_text(encoding="utf-8"))
    result = run_pipeline(ROOT / config["input_dir"], ROOT / config["output_dir"])
    print(json.dumps({"status": "success", **result}, ensure_ascii=False, indent=2))
    print(f"Artefatos gerados em: {ROOT / config['output_dir']}")


if __name__ == "__main__":
    main()
