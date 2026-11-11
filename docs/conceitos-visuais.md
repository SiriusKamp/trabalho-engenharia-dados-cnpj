# Conceitos visuais

## 1. Fluxo principal do lakehouse

```mermaid
flowchart LR
    A[Arquivos oficiais CNPJ] --> B[Ingestão<br/>manifesto + checksum]
    B --> C[Bronze<br/>bruto e auditável]
    C --> D[Silver<br/>tipado, limpo, deduplicado]
    D --> E[Gold<br/>KPIs e perguntas de negócio]
    E --> F[BI / relatório / ML]
    D -. qualidade e segurança .-> Q[checks + mascaramento]
    Q --> E
```

**Como explicar:** Bronze preserva a verdade original; Silver organiza e protege; Gold entrega uma resposta pronta para consumo. Se a Gold estiver errada, voltamos à Silver e, se necessário, à Bronze para reprocessar.

## 2. O que muda em cada camada

```mermaid
flowchart TB
    subgraph Bronze
        B1[CSV original]
        B2[Sem regra de negócio]
        B3[Serve auditoria e reprocessamento]
    end
    subgraph Silver
        S1[Schema aplicado]
        S2[Tipos e chaves validados]
        S3[PII mascarada]
    end
    subgraph Gold
        G1[Top UFs]
        G2[Portes]
        G3[Perfil societário]
    end
    B1 --> S1 --> G1
    B2 --> S2 --> G2
    B3 --> S3 --> G3
```

## 3. Pipeline como linha de montagem

```mermaid
flowchart LR
    I[1. Ler arquivos] --> P[2. Parsear e tipar]
    P --> V{3. Qualidade aprovada?}
    V -- não --> R[Rejeitar e registrar motivo]
    V -- sim --> M[4. Mascarar e deduplicar]
    M --> A[5. Agregar respostas]
    A --> O[6. Publicar e medir]
    O --> X[7. Auditoria da execução]
```

## 4. Por que reprocessamento precisa ser idempotente

```mermaid
sequenceDiagram
    participant Fonte
    participant Bronze
    participant Silver
    participant Gold
    Fonte->>Bronze: lote 2026-01
    Bronze->>Silver: run_001
    Silver->>Gold: publica resultados
    Fonte->>Bronze: mesmo lote 2026-01 novamente
    Bronze->>Silver: MERGE/overwrite da mesma referência
    Silver->>Gold: mesmo resultado, sem duplicação
```

## 5. Batch do CNPJ versus streaming

```mermaid
flowchart LR
    CNPJ[Atualização mensal do CNPJ] --> BATCH[Batch: fecha lote e processa]
    GPS[Eventos contínuos de mobilidade] --> STREAM[Streaming: processa enquanto chega]
    BATCH --> M1[mais simples de reprocessar]
    STREAM --> M2[menor latência, maior complexidade]
```

O trabalho CNPJ é naturalmente batch. Kafka e Structured Streaming aparecem na aula como repertório de arquitetura; não precisam ser adicionados ao Cenário A sem uma necessidade real de baixa latência.
