"""Baixa os arquivos ZIP oficiais do CNPJ.

Exemplo seguro para testar sem baixar:

    python scripts/download_cnpj.py --data-ref 2026-01 --dry-run

Download real:

    python scripts/download_cnpj.py --data-ref 2026-01

Os arquivos são mantidos compactados para preservar a Bronze. A extração e o
processamento ficam como etapas posteriores do pipeline.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# Endereço usado no material da aula. A referência mensal é acrescentada
# depois da URL base, por exemplo: .../2026-01/Empresas0.zip.
BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj"

# O CNPJ é dividido em partes numeradas. Se a Receita alterar a quantidade
# de partes em um novo lote, ajuste PARTES antes de executar o download.
PARTES = range(10)


def arquivos_cnpj() -> list[str]:
    """Monta a lista de ZIPs principais indicada no slide."""

    arquivos = [f"Empresas{i}.zip" for i in PARTES]
    arquivos += [f"Estabelecimentos{i}.zip" for i in PARTES]
    arquivos += [f"Socios{i}.zip" for i in PARTES]
    arquivos.append("Simples.zip")
    return arquivos


def baixar(url: str, destino: Path, sobrescrever: bool = False) -> str:
    """Baixa um arquivo em partes e retorna o status da operação."""

    if destino.exists() and destino.stat().st_size > 0 and not sobrescrever:
        return "já existe"

    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(destino.suffix + ".part")
    requisicao = Request(url, headers={"User-Agent": "cnpj-lakehouse-student/1.0"})

    try:
        with urlopen(requisicao, timeout=120) as resposta, temporario.open("wb") as arquivo:
            while bloco := resposta.read(1024 * 1024):
                arquivo.write(bloco)
        temporario.replace(destino)
        return "baixado"
    except (HTTPError, URLError, TimeoutError) as erro:
        if temporario.exists():
            temporario.unlink()
        raise RuntimeError(f"falha ao baixar {url}: {erro}") from erro


def main() -> int:
    parser = argparse.ArgumentParser(description="Baixa os ZIPs do CNPJ por referência mensal.")
    parser.add_argument("--data-ref", default="2026-01", help="Referência mensal, por exemplo 2026-01.")
    parser.add_argument("--destination", type=Path, default=Path("data/raw/cnpj"), help="Pasta dos ZIPs baixados.")
    parser.add_argument("--dry-run", action="store_true", help="Mostra URLs sem fazer download.")
    parser.add_argument("--force", action="store_true", help="Baixa novamente arquivos já existentes.")
    args = parser.parse_args()

    destino = args.destination / args.data_ref
    for nome in arquivos_cnpj():
        url = f"{BASE_URL}/{args.data_ref}/{nome}"
        arquivo_destino = destino / nome

        if args.dry_run:
            print(f"[simulação] {url} -> {arquivo_destino}")
            continue

        try:
            status = baixar(url, arquivo_destino, sobrescrever=args.force)
            print(f"[{status}] {nome}")
        except RuntimeError as erro:
            print(f"[erro] {erro}")
            return 1

    print(f"Concluído. Arquivos em: {destino}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
