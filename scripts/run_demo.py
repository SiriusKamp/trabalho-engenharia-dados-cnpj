import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cnpj_lakehouse.pipeline import run_pipeline  # noqa: E402


def main() -> None:
    result = run_pipeline(ROOT / "data" / "sample", ROOT / "artifacts")
    print("Pipeline executado com sucesso.")
    print(result["gold_rows"])


if __name__ == "__main__":
    main()
