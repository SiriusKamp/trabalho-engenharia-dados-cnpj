"""Autenticação por perfil e limites de acesso ao volume DATA."""

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import secrets
import stat

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials


CAMADAS = ("Raw", "Bronze", "Prata", "Ouro")
# Autenticação responde "quem é?"; esta matriz responde "o que pode acessar?".
# O perfil de monitoramento tem endpoints próprios, sem acesso aos arquivos.
PERMISSOES = {"engenharia": frozenset(("Raw", "Bronze", "Prata")),
              "analise": frozenset(("Ouro",)), "monitoramento": frozenset()}
_DEMONSTRACAO = (
    # Estes valores são públicos para estudo. Variáveis do ambiente substituem
    # cada usuário/senha; o .env privado fica fora do Git.
    ("engenharia", "ENGENHARIA", "engenharia", "Engenharia123!"),
    ("analise", "ANALISE", "analise", "Analise123!"),
    ("monitoramento", "MONITORAMENTO", "monitor", "Monitor123!"),
)


@dataclass(frozen=True)
class Identidade:
    # frozen impede alterar o perfil desta identidade depois da autenticação.
    usuario: str
    perfil: str


def criar_autenticacao():
    """Captura o ambiente na criação da aplicação; guarda somente hashes em memória."""
    basic = HTTPBasic(realm="cnpj")
    sal = secrets.token_bytes(16)

    def derivar(senha):
        # PBKDF2 repete a derivação para tornar a comparação de senhas mais cara.
        # Isso protege a representação em memória; HTTPS protege o trânsito da credencial.
        return hashlib.pbkdf2_hmac("sha256", senha.encode("utf-8"), sal, 120_000)

    usuarios = []
    nomes = set()
    for perfil, prefixo, usuario_demo, senha_demo in _DEMONSTRACAO:
        usuario = os.environ.get(prefixo + "_USUARIO", usuario_demo)
        senha = os.environ.get(prefixo + "_SENHA", senha_demo)
        if not usuario or not senha or usuario in nomes:
            raise ValueError("Configure usuários únicos e credenciais não vazias para cada perfil.")
        nomes.add(usuario)
        usuarios.append((hashlib.sha256(usuario.encode("utf-8")).digest(),
                         derivar(senha), Identidade(usuario, perfil)))

    def autenticar(credenciais: HTTPBasicCredentials = Depends(basic)) -> Identidade:
        # FastAPI injeta as credenciais do cabeçalho HTTP Basic em cada requisição.
        usuario_hash = hashlib.sha256(credenciais.username.encode("utf-8")).digest()
        senha_hash = derivar(credenciais.password)
        identidade = None
        # Compara os dois valores de tamanho fixo em todos os perfis, sem short circuit.
        for esperado_usuario, esperado_senha, candidata in usuarios:
            usuario_ok = secrets.compare_digest(usuario_hash, esperado_usuario)
            senha_ok = secrets.compare_digest(senha_hash, esperado_senha)
            if usuario_ok & senha_ok:
                identidade = candidata
        if identidade is None:
            # 401 pede credenciais válidas. Depois de autenticado, uma permissão
            # negada usa 403 nas funções de autorização abaixo.
            raise HTTPException(401, "Credenciais inválidas", headers={"WWW-Authenticate": 'Basic realm="cnpj"'})
        return identidade

    return autenticar


def exigir_perfil(identidade: Identidade, perfil: str):
    if identidade.perfil != perfil:
        raise HTTPException(403, "Perfil sem permissão para esta operação")


def autorizar_camada(identidade: Identidade, camada: str):
    if camada not in CAMADAS:
        raise HTTPException(404, "Camada inexistente")
    if camada not in PERMISSOES[identidade.perfil]:
        raise HTTPException(403, "Perfil sem permissão para esta camada")


def diretorio_camada(raiz: Path, camada: str) -> Path:
    """Não aceita que uma camada ou a raiz seja redirecionada por symlink/junction."""
    destino = raiz / camada
    for caminho in (raiz, destino):
        if caminho.is_symlink() or (hasattr(caminho, "is_junction") and caminho.is_junction()):
            raise HTTPException(403, "Links não são disponibilizados")
    if not destino.resolve().is_relative_to(raiz.resolve()):
        raise HTTPException(403, "Caminho fora de DATA")
    return destino


def nome_publico(parte: str) -> bool:
    """Oculta manifestos, locks, staging e arquivos auxiliares, inclusive ADS no Windows."""
    return bool(parte and parte not in (".", "..") and
                not parte.startswith((".", "_")) and
                not parte.lower().startswith("metadados.json") and
                not parte.lower().endswith((".lock", ".tmp", ".part", ".anterior", ".novo")) and
                not parte.endswith((".", " ")) and
                not any(ord(caractere) < 32 or caractere in ':\\' for caractere in parte))


def caminho_download(raiz: Path, camada: str, arquivo: str) -> Path:
    # Autorizar a camada não basta: o caminho também precisa permanecer dentro
    # dela. Bloquear ../, caminhos absolutos e links impede escapar para outra pasta.
    pasta = diretorio_camada(raiz, camada)
    partes = arquivo.split("/")
    if (PurePosixPath(arquivo).is_absolute() or PureWindowsPath(arquivo).is_absolute()
            or not all(nome_publico(parte) for parte in partes)):
        raise HTTPException(403, "Caminho não permitido")
    destino = pasta
    for parte in partes:
        destino = destino / parte
        if destino.is_symlink() or (hasattr(destino, "is_junction") and destino.is_junction()):
            raise HTTPException(403, "Links não são disponibilizados")
    if not destino.resolve().is_relative_to(pasta.resolve()):
        raise HTTPException(403, "Caminho fora da camada")
    return destino


def abrir_download(raiz: Path, camada: str, arquivo: str):
    """Abre antes de responder: o streaming usa o descritor validado, nunca reabre o path."""
    destino = caminho_download(raiz, camada, arquivo)
    descritor = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        if os.open in os.supports_dir_fd:
            # Linux: prende cada diretório por descritor e rejeita links em todos os níveis.
            pasta_fd = os.open(raiz, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                partes = (camada, *arquivo.split("/"))
                for parte in partes[:-1]:
                    proximo = os.open(parte, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=pasta_fd)
                    os.close(pasta_fd)
                    pasta_fd = proximo
                descritor = os.open(partes[-1], flags | getattr(os, "O_NONBLOCK", 0), dir_fd=pasta_fd)
            finally:
                os.close(pasta_fd)
        else:
            descritor = os.open(destino, flags)
        atributos = os.fstat(descritor)
        # Conferir o arquivo realmente aberto evita confiar apenas no nome
        # validado antes: outra operação pode ter substituído o caminho.
        caminho_download(raiz, camada, arquivo)
        if not stat.S_ISREG(atributos.st_mode) or not os.path.samestat(atributos, destino.stat()):
            raise HTTPException(403, "Arquivo não permitido")
        stream = os.fdopen(descritor, "rb")
        descritor = None
        return stream, atributos.st_size
    except HTTPException:
        raise
    except (OSError, ValueError):
        raise HTTPException(404, "Arquivo indisponível") from None
    finally:
        if descritor is not None:
            os.close(descritor)
