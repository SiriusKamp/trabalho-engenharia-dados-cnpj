FROM python:3.12-slim-bookworm AS dependencias

# A imagem inclui o ambiente Linux necessário. O notebook precisa de Docker,
# enquanto Python/Java/PySpark usados pela aplicação ficam dentro do container.
# Spark 4 funciona com Java 17; o container evita a dependência de winutils.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home cnpj
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    SPARK_LOCAL_IP=127.0.0.1 DATA_DIR=/app/DATA
WORKDIR /app
# Instalar dependências antes de copiar o código aproveita o cache do build:
# alterar apenas um .py não exige baixar e reinstalar todo o PySpark.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM dependencias AS aplicacao
# O código é copiado para a imagem. Após editar Python, use
# docker compose up -d --build app para reconstruir e aplicar a versão nova.
COPY --chown=cnpj:cnpj Scripts ./Scripts
COPY --chown=cnpj:cnpj main.py ./main.py
RUN mkdir -p /app/DATA /app/.spark-tmp && chown -R cnpj:cnpj /app
USER cnpj
# tini encaminha sinais de encerramento e recolhe processos filhos da JVM.
ENTRYPOINT ["/usr/bin/tini", "--"]
# Subir o container mantém a API rodando. Para uma carga manual no container
# existente: docker compose exec app python main.py importar --meses 4
CMD ["python", "main.py", "servir"]
