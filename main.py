"""Entrada única: python main.py importar --meses 4 --simular."""

import argparse
import json
import logging
import os

# Comece o estudo por este arquivo. Ele interpreta o comando do terminal e
# encaminha o trabalho para a API ou para Scripts/Agendador.py.
# O Compose lê .env e injeta as variáveis no container; os.getenv() apenas
# consulta esse ambiente. Executar Python diretamente não carrega .env sozinho.

def argumentos(argv=None):
    # Cada subcomando tem uma responsabilidade: importar, retomar, reativar ou servir HTTP.
    parser = argparse.ArgumentParser(description="Pipeline CNPJ: Raw -> Bronze -> Prata -> Ouro")
    comandos = parser.add_subparsers(dest="comando", required=True)
    importar = comandos.add_parser("importar", help="Processar as competências do intervalo solicitado")
    importar.add_argument("--meses", type=int, default=int(os.getenv("MESES", "4")), help="Mês atual e N-1 anteriores; padrão 4")
    importar.add_argument("--simular", action="store_true", help="Consultar o catálogo sem baixar, extrair ou alterar metadados")
    importar.add_argument("--forcar", action="store_true", help="Reprocessar os arquivos do intervalo, mesmo sem mudança remota")
    importar.add_argument("--usuario", default="operador-cli", help="Responsável registrado na auditoria")
    # --usuario identifica o operador na auditoria da CLI; a autenticação é feita na API.
    comandos.add_parser("manutencao", help="Retomar pendências do catálogo já descoberto")
    comandos.add_parser("reativar", help="Reativar o job de manutenção")
    for nome, descricao in (("servir", "Iniciar API e, quando habilitado, agendador"),
                            ("metricas", "Iniciar somente o monitoramento, sem operação ou agendador")):
        servir = comandos.add_parser(nome, help=descricao)
        # A porta aberta no notebook é definida por HOST:CONTAINER no Compose.
        servir.add_argument("--host", default="0.0.0.0")
        servir.add_argument("--porta", type=int, default=8000)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = argumentos(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        if args.comando in ("servir", "metricas"):
            # Este é o comando padrão do Dockerfile: mantém a API disponível.
            # O agendador só começa se AGENDAMENTO_ATIVO estiver habilitado.
            import uvicorn
            from Scripts.API import criar_app
            uvicorn.run(criar_app(somente_metricas=args.comando == "metricas"), host=args.host, port=args.porta,
                        ssl_certfile=os.getenv("TLS_CERTIFICADO") or None,
                        ssl_keyfile=os.getenv("TLS_CHAVE") or None)
            return 0
        from Scripts.Agendador import executar, manutencao, reativar
        # Importar coordena Raw -> Bronze -> Prata -> Ouro. A sessão Spark só é
        # criada pelo coordenador quando realmente houver transformação pendente.
        if args.comando == "importar":
            resultado = executar(args.meses, simular=args.simular, forcar=args.forcar, usuario=args.usuario)
        elif args.comando == "manutencao":
            resultado = manutencao()
        else:
            resultado = reativar()
        print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
        # Código de saída 1 permite que terminal/jobs reconheçam uma falha.
        return 1 if resultado.get("status") == "erro" else 0
    except (ValueError, RuntimeError, OSError) as erro:
        logging.error("%s", erro)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
