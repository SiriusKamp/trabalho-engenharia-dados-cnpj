"""JSON de controle por camada e publicação recuperável de diretórios."""

from datetime import datetime, timezone
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
from threading import RLock


CAMADAS = ("Raw", "Bronze", "Prata", "Ouro")
# RLock coordena threads do mesmo processo. trava_arquivo também coordena
# processos diferentes, como a API e uma importação aberta pelo terminal.
_LOCK = RLock()


@contextmanager
def trava_arquivo(caminho: Path, bloquear: bool = True):
    """Lock liberado pelo sistema mesmo se o processo for interrompido."""
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("a+b") as arquivo:
        # No Windows a trava usa um byte existente; no Linux usa o arquivo inteiro.
        if arquivo.tell() == 0:
            arquivo.write(b"0")
            arquivo.flush()
        arquivo.seek(0)
        import os
        if os.name == "nt":
            import msvcrt
            modo = msvcrt.LK_LOCK if bloquear else msvcrt.LK_NBLCK
            try:
                msvcrt.locking(arquivo.fileno(), modo, 1)
            except OSError as erro:
                raise RuntimeError("Outra execução está usando o pipeline") from erro
        else:
            import fcntl
            try:
                fcntl.flock(arquivo.fileno(), fcntl.LOCK_EX | (0 if bloquear else fcntl.LOCK_NB))
            except BlockingIOError as erro:
                raise RuntimeError("Outra execução está usando o pipeline") from erro
        try:
            # O código dentro de "with trava_arquivo(...)" executa neste ponto.
            yield
        finally:
            arquivo.seek(0)
            if os.name == "nt":
                msvcrt.locking(arquivo.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(arquivo.fileno(), fcntl.LOCK_UN)


def agora() -> str:
    # UTC e ISO 8601 permitem comparar datas sem depender do fuso do notebook.
    return datetime.now(timezone.utc).isoformat()


class Metadados:
    # Formato: arquivos guarda o estado atual por origem; execucoes guarda
    # tentativas históricas; estado, na Raw, guarda catálogo e agendamento.
    def __init__(self, raiz: Path):
        self.raiz = Path(raiz).resolve()

    def _caminho(self, camada: str) -> Path:
        if camada not in CAMADAS:
            raise ValueError("Camada inválida")
        return self.raiz / camada / "Metadados.json"

    def ler(self, camada: str) -> dict:
        with _LOCK:
            caminho = self._caminho(camada)
            if not caminho.exists():
                # A primeira leitura funciona mesmo antes da primeira importação.
                return {"arquivos": {}, "execucoes": [], **({"estado": {}} if camada == "Raw" else {})}
            return json.loads(caminho.read_text(encoding="utf-8"))

    def _salvar(self, camada: str, dados: dict) -> None:
        caminho = self._caminho(camada)
        caminho.parent.mkdir(parents=True, exist_ok=True)
        temporario = caminho.with_suffix(".json.tmp")
        # Escrever e depois substituir evita expor um JSON pela metade aos leitores.
        temporario.write_text(json.dumps(dados, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporario.replace(caminho)

    def atualizar(self, camada: str, chave: str, **campos) -> dict:
        # O ciclo ler -> alterar -> salvar fica sob a mesma trava para um download
        # não sobrescrever a atualização de outro download concorrente.
        with _LOCK, trava_arquivo(self.raiz / ".metadados.lock"):
            dados = self.ler(camada)
            registro = dados.setdefault("arquivos", {}).setdefault(chave, {})
            registro.update(campos)
            registro["atualizado_em"] = agora()
            self._salvar(camada, dados)
            return dict(registro)

    def evento(self, camada: str, evento: dict) -> None:
        # Acrescentar eventos mantém a trilha de falhas mesmo após uma retomada bem-sucedida.
        with _LOCK, trava_arquivo(self.raiz / ".metadados.lock"):
            dados = self.ler(camada)
            dados.setdefault("execucoes", []).append({"registrado_em": agora(), **evento})
            self._salvar(camada, dados)

    def estado(self, **campos) -> dict:
        # Sem argumentos, consulta. Com argumentos nomeados, atualiza só esses campos.
        with _LOCK, trava_arquivo(self.raiz / ".metadados.lock"):
            dados = self.ler("Raw")
            estado = dados.setdefault("estado", {})
            if campos:
                estado.update(campos)
                self._salvar("Raw", dados)
            return dict(estado)


def publicar_diretorio(staging: Path, destino: Path) -> None:
    """Mantém a última versão legível se a troca for interrompida.

    Os diretórios são irmãos no mesmo volume. A execução do pipeline tem um
    lock único; não há duas publicações simultâneas para o mesmo destino.
    """
    staging, destino = Path(staging).resolve(), Path(destino).resolve()
    if staging == destino or staging.parent != destino.parent:
        raise ValueError("Staging e destino devem ser diretórios irmãos distintos")
    backup = destino.with_name(destino.name + ".anterior")
    # .anterior é uma proteção temporária da troca, não uma versão histórica.
    if backup.exists() and not destino.exists():
        backup.rename(destino)
    if not staging.is_dir():
        raise FileNotFoundError(staging)
    if backup.exists():
        shutil.rmtree(backup)
    try:
        # A sequência é: versão atual -> backup; staging validado -> versão atual.
        # Quem publica já validou o conteúdo antes de chamar esta função.
        if destino.exists():
            destino.rename(backup)
        staging.rename(destino)
    except Exception:
        # Se a segunda troca falhar, restaura a última versão legível.
        if backup.exists() and not destino.exists():
            backup.rename(destino)
        raise
    if backup.exists():
        shutil.rmtree(backup)
