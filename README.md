# Distributed Wagering Processor — Jungle Gaming Challenge 🦧

Serviço financeiro distribuído para processamento de transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) construído com **Bun 1.x**, **TypeScript (strict mode)**, **NestJS**, **MikroORM**, **PostgreSQL 16** e **AWS SQS (LocalStack)**, seguindo **DDD (Domain-Driven Design)**, **Hexagonal Architecture** e padrões **Transactional Inbox / Outbox**.

📄 **Decisões arquiteturais detalhadas, diagramas, garantias de schema e relatório de carga:** consulte o arquivo [**ARCHITECTURE.md**](./ARCHITECTURE.md).

---

## 1. Requisitos e Stack

- **Runtime / Package Manager / Test Runner:** [Bun 1.x](https://bun.sh/) (`>= 1.1`)
- **Framework:** NestJS 10 + TypeScript 5 (`strict: true`)
- **ORM & Migrations:** MikroORM 6 (`@mikro-orm/postgresql` + `@mikro-orm/migrations`)
- **Banco de Dados:** PostgreSQL 16
- **Mensageria:** AWS SQS FIFO (`wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `domain-integration-events.fifo`) via LocalStack
- **Orquestração Local:** Docker Compose (PostgreSQL + LocalStack + 3 réplicas da aplicação)

---

## 2. Como Executar o Projeto

### 2.1. Subindo toda a infraestrutura + 3 instâncias via Docker Compose

```bash
docker compose up --build -d
```

Isso inicializará:
- `postgres`: PostgreSQL 16 na porta `5432`
- `localstack`: AWS SQS com criação automática das filas FIFO e RedrivePolicy (DLQ) na porta `4566`
- `app-1` (`http://localhost:3000`), `app-2` (`http://localhost:3001`) e `app-3` (`http://localhost:3002`)

### 2.2. Executando localmente com Bun

```bash
# 1. Instalar dependências
bun install

# 2. Executar migrations no PostgreSQL (up / down)
bun run migration:up
# Para reverter a última migration:
bun run migration:down

# 3. Iniciar a aplicação
bun run start
```

---

## 3. Executando os Testes (Unidade, Integração, Concorrência e Carga)

> **Nota sobre execução sem Docker:** A suíte de testes detecta automaticamente se o PostgreSQL (`localhost:5432`) e o LocalStack (`localhost:4566`) estão ativos via Docker. Caso o Docker não esteja rodando na máquina local, o *test harness* (`test/helpers/test-infra.ts`) inicializa automaticamente em processo uma engine **PostgreSQL 16 real (via PGlite + wire protocol TCP)** — que executa 100% das migrations, constraints `CHECK`/`UNIQUE`, índices parciais e triggers PL/pgSQL — além de um endpoint HTTP AWS SQS compatível com o `@aws-sdk/client-sqs`.

```bash
# Rodar todas as suítes (Unidade + Integração + Concorrência)
bun test

# Rodar apenas testes de Unidade (Domínio puro: Money, Wallet, Ledger, WagerTransaction, Hash, Outbox)
bun run test:unit

# Rodar apenas testes de Integração (Migrations, Constraints SQL, Trigger de Imutabilidade, HTTP, Inbox/Outbox, DLQ)
bun run test:integration

# Rodar os 8 cenários obrigatórios de Concorrência e Tolerância a Falhas (3 instâncias reais em paralelo)
bun run test:concurrency

# Rodar o Teste de Carga (300 transações em 3 instâncias + Hot Wallet + Reconciliação)
bun run test:load

# Verificar tipagem estrita do TypeScript
bun run typecheck
```

---

## 4. Endpoints da API HTTP

### 4.1. Wallets

- **`POST /wallets`** — Abre uma nova carteira (gera transação interna `OPENING` + lançamento `CREDIT` no ledger quando `initialBalance > 0.00`).
  ```bash
  curl -X POST http://localhost:3000/wallets \
    -H "Content-Type: application/json" \
    -d '{
      "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
      "initialBalance": { "amount": "1000.00", "currency": "BRL" }
    }'
  ```
- **`GET /wallets/:walletId`** — Consulta saldo atual e versão da carteira.
- **`GET /wallets/:walletId/ledger?cursor=...&limit=50`** — Lista lançamentos imutáveis do ledger com paginação por cursor opaco e estável.
- **`POST /wallets/:walletId/reconciliation`** — Reconstrói o saldo a partir do ledger e verifica consistência contábil (`wallet.balance == calculatedBalance`).

### 4.2. Wagering Transactions

- **`POST /wagering/transactions`** — Submete uma transação (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`). Exige header `Idempotency-Key`.
  ```bash
  curl -X POST http://localhost:3000/wagering/transactions \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: provider-a:transaction-123" \
    -d '{
      "providerId": "provider-a",
      "externalTransactionId": "transaction-123",
      "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
      "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
      "roundId": "round-987",
      "gameId": "fortune-chimp",
      "kind": "BET",
      "money": { "amount": "25.00", "currency": "BRL" }
    }'
  ```
  **Mapeamento Semântico de Status HTTP:**
  - `200 OK`: Transação `PROCESSED` (incluindo replay idempotente com `idempotentReplay: true` e o saldo observado originalmente).
  - `202 Accepted`: Transação aceita em `PENDING_REFERENCE` (aguardando transação referenciada fora de ordem).
  - `400 Bad Request`: Payload inválido, escala de `Money` incorreta, `kind: OPENING` ou ausência de `Idempotency-Key`.
  - `404 Not Found`: Carteira ou transação não encontrada.
  - `409 Conflict`: Mesma `Idempotency-Key` reutilizada com payload divergente (`IDEMPOTENCY_KEY_CONFLICT`) ou carteira duplicada para `playerId + currency`.
  - `422 Unprocessable Entity`: Transação `REJECTED` por regra de negócio (`INSUFFICIENT_FUNDS`, `NEGATIVE_BALANCE_ON_REVERSAL`, `DUPLICATE_REVERSAL`, `INVALID_REFERENCE_KIND`, `REFERENCE_MISMATCH`, `CURRENCY_MISMATCH`).
  - `503 Service Unavailable`: Falha transitória de infraestrutura (seguro para retry com a mesma `Idempotency-Key`).

- **`GET /wagering/transactions/:transactionId`** — Consulta transação pelo ID interno.
- **`GET /providers/:providerId/wagering/transactions/:externalTransactionId`** — Consulta transação pelo ID externo no provedor.

### 4.3. Observabilidade & Health Checks

- **`GET /health/live`** — Liveness probe (público).
- **`GET /health/ready`** — Readiness probe verificando conectividade real com PostgreSQL e SQS (público).
- **`GET /metrics`** — Métricas no formato Prometheus (`wagering_transactions_total`, `wagering_duplicates_detected_total`, `wagering_retries_total`, `wagering_dlq_messages_total`, `wagering_lock_conflicts_total`, `wagering_outbox_pending_messages`, `wagering_outbox_max_lag_seconds`, `wagering_processing_duration_ms`, `wagering_reconciliation_divergences_total`).
