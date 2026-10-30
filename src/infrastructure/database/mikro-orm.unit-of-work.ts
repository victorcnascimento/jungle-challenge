import { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import Decimal from 'decimal.js';
import type { MetricsPort } from '../../application/ports/observability.port';
import type {
  InboxRepositoryPort,
  LedgerPage,
  LedgerRepositoryPort,
  OutboxRepositoryPort,
  TransactionalRepositories,
  UnitOfWorkPort,
  WagerTransactionRepositoryPort,
  WalletRepositoryPort,
} from '../../application/ports/unit-of-work.port';
import { InboxMessage } from '../../domain/messaging/inbox-message.entity';
import { OutboxMessage } from '../../domain/messaging/outbox-message.entity';
import { DuplicateWalletError } from '../../domain/shared/domain-errors';
import { FailureCode } from '../../domain/shared/failure-code.enum';
import { Money } from '../../domain/shared/money.vo';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/wagering/wager-transaction.entity';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../../domain/wallet/wallet-ledger-entry.entity';
import { Wallet } from '../../domain/wallet/wallet.aggregate';

/**
 * Formata um valor decimal vindo do driver PostgreSQL com escala fixa de 2 casas decimais
 * utilizando exclusivamente Decimal (decimal.js), sem jamais converter para Number/float (Restrição 5.1).
 */
function formatNumeric(value: string | Decimal): string {
  return new Decimal(String(value)).toFixed(2);
}

/**
 * Codifica o sequence_number sequencial do ledger em um cursor opaco em Base64URL.
 */
function encodeOpaqueCursor(sequenceNumber: string | number): string {
  return Buffer.from(`seq:${sequenceNumber}`, 'utf8').toString('base64url');
}

/**
 * Decodifica e valida o cursor opaco em Base64URL para recuperar o sequence_number.
 */
function decodeOpaqueCursor(cursor: string): string {
  try {
    const decoded = Buffer.from(cursor, 'base64url').toString('utf8');
    if (!decoded.startsWith('seq:')) {
      throw new Error('Invalid cursor prefix');
    }
    const seq = decoded.slice(4);
    if (!/^\d+$/.test(seq)) {
      throw new Error('Invalid cursor sequence');
    }
    return seq;
  } catch {
    throw new Error('Invalid pagination cursor');
  }
}

interface RawWalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance_amount: string;
  version: number;
  created_at: Date | string;
  updated_at: Date | string;
}

/**
 * Repositório PostgreSQL de Wallets operando sobre a conexão transacional do EntityManager.
 */
class MikroOrmWalletRepository implements WalletRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  /** Busca uma Wallet pelo ID sem adquirir lock de escrita. */
  async findById(id: string): Promise<Wallet | null> {
    const rows = await this.em.execute<RawWalletRow[]>(
      `SELECT id, player_id, currency, balance_amount, version, created_at, updated_at
       FROM wallets
       WHERE id = ?`,
      [id],
    );
    return rows[0] ? this.toDomain(rows[0]) : null;
  }

  /**
   * Busca uma Wallet pelo ID adquirindo Lock Pessimista de linha (SELECT ... FOR UPDATE)
   * para serializar operações financeiras concorrentes na mesma walletId.
   */
  async findByIdForUpdate(id: string): Promise<Wallet | null> {
    const rows = await this.em.execute<RawWalletRow[]>(
      `SELECT id, player_id, currency, balance_amount, version, created_at, updated_at
       FROM wallets
       WHERE id = ?
       FOR UPDATE`,
      [id],
    );
    return rows[0] ? this.toDomain(rows[0]) : null;
  }

  /** Busca uma Wallet existente pelo par único (playerId, currency). */
  async findByPlayerAndCurrency(
    playerId: string,
    currency: string,
  ): Promise<Wallet | null> {
    const rows = await this.em.execute<RawWalletRow[]>(
      `SELECT id, player_id, currency, balance_amount, version, created_at, updated_at
       FROM wallets
       WHERE player_id = ? AND currency = ?`,
      [playerId, currency],
    );
    return rows[0] ? this.toDomain(rows[0]) : null;
  }

  /** Insere uma nova Wallet e converte violação de constraint UNIQUE (23505) em DuplicateWalletError. */
  async insert(wallet: Wallet): Promise<void> {
    try {
      await this.em.execute(
        `INSERT INTO wallets (id, player_id, currency, balance_amount, version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [
          wallet.id,
          wallet.playerId,
          wallet.currency,
          wallet.balance.amount,
          wallet.version,
          wallet.createdAt,
          wallet.updatedAt,
        ],
      );
    } catch (err: unknown) {
      const pgCode = (err as { code?: string })?.code;
      if (pgCode === '23505') {
        throw new DuplicateWalletError(wallet.playerId, wallet.currency);
      }
      throw err;
    }
  }

  /** Atualiza o saldo materializado, a versão e o timestamp updated_at da Wallet. */
  async update(wallet: Wallet): Promise<void> {
    await this.em.execute(
      `UPDATE wallets
       SET balance_amount = ?, version = ?, updated_at = ?
       WHERE id = ?`,
      [wallet.balance.amount, wallet.version, wallet.updatedAt, wallet.id],
    );
  }

  /** Reidrata a entidade de domínio Wallet a partir da linha retornada pelo PostgreSQL. */
  private toDomain(row: RawWalletRow): Wallet {
    const currency = row.currency.trim();
    const amountStr = formatNumeric(row.balance_amount);
    return Wallet.rehydrate({
      id: row.id,
      playerId: row.player_id,
      currency,
      balance: Money.from({ amount: amountStr, currency }),
      version: Number(row.version),
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
    });
  }
}

/**
 * Repositório PostgreSQL para o Ledger imutável (append-only) de carteiras.
 */
class MikroOrmLedgerRepository implements LedgerRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  /** Insere um novo lançamento contábil na tabela wallet_ledger_entries. */
  async insert(entry: WalletLedgerEntry): Promise<void> {
    await this.em.execute(
      `INSERT INTO wallet_ledger_entries
       (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.direction,
        entry.money.amount,
        entry.money.currency,
        entry.balanceBefore.amount,
        entry.balanceAfter.amount,
        entry.createdAt,
      ],
    );
  }

  /** Busca uma página de lançamentos do Ledger ordenada por sequence_number usando cursor opaco. */
  async findByWalletCursor(
    walletId: string,
    cursor?: string,
    limit: number = 50,
  ): Promise<LedgerPage> {
    const afterSequence = cursor ? decodeOpaqueCursor(cursor) : '0';
    const rows = await this.em.execute<
      Array<{
        id: string;
        sequence_number: string;
        wallet_id: string;
        transaction_id: string;
        direction: string;
        amount: string;
        currency: string;
        balance_before: string;
        balance_after: string;
        created_at: Date;
      }>
    >(
      `SELECT id, sequence_number, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
       FROM wallet_ledger_entries
       WHERE wallet_id = ? AND sequence_number > ?
       ORDER BY sequence_number ASC
       LIMIT ?`,
      [walletId, afterSequence, limit + 1],
    );

    const hasMore = rows.length > limit;
    const pageRows = hasMore ? rows.slice(0, limit) : rows;
    const lastRow = pageRows[pageRows.length - 1];

    return {
      items: pageRows.map((row) => this.rowToDomain(row)),
      nextCursor:
        hasMore && lastRow
          ? encodeOpaqueCursor(lastRow.sequence_number)
          : null,
    };
  }

  /** Retorna todos os lançamentos contábeis de uma carteira em ordem cronológica estrita para reconciliação. */
  async findAllByWalletOrdered(walletId: string): Promise<WalletLedgerEntry[]> {
    const rows = await this.em.execute<
      Array<{
        id: string;
        sequence_number: string;
        wallet_id: string;
        transaction_id: string;
        direction: string;
        amount: string;
        currency: string;
        balance_before: string;
        balance_after: string;
        created_at: Date;
      }>
    >(
      `SELECT id, sequence_number, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
       FROM wallet_ledger_entries
       WHERE wallet_id = ?
       ORDER BY sequence_number ASC`,
      [walletId],
    );

    return rows.map((row) => this.rowToDomain(row));
  }

  /** Reidrata um WalletLedgerEntry a partir da linha do PostgreSQL. */
  private rowToDomain(row: {
    id: string;
    wallet_id: string;
    transaction_id: string;
    direction: string;
    amount: string;
    currency: string;
    balance_before: string;
    balance_after: string;
    created_at: Date;
  }): WalletLedgerEntry {
    const currency = row.currency.trim();
    return WalletLedgerEntry.rehydrate({
      id: row.id,
      walletId: row.wallet_id,
      transactionId: row.transaction_id,
      direction: row.direction as LedgerDirection,
      money: Money.from({
        amount: formatNumeric(row.amount),
        currency,
      }),
      balanceBefore: Money.from({
        amount: formatNumeric(row.balance_before),
        currency,
      }),
      balanceAfter: Money.from({
        amount: formatNumeric(row.balance_after),
        currency,
      }),
      createdAt: new Date(row.created_at),
    });
  }
}

/**
 * Repositório PostgreSQL de WagerTransactions.
 */
class MikroOrmWagerTransactionRepository
  implements WagerTransactionRepositoryPort
{
  constructor(private readonly em: EntityManager) {}

  /** Busca uma transação de aposta pelo seu ID interno. */
  async findById(id: string): Promise<WagerTransaction | null> {
    const rows = await this.em.execute<Array<Record<string, unknown>>>(
      `SELECT * FROM wager_transactions WHERE id = ?`,
      [id],
    );
    return rows[0] ? this.rawRowToDomain(rows[0]) : null;
  }

  /** Busca uma transação pelo ID aplicando lock pessimista FOR UPDATE. */
  async findByIdForUpdate(id: string): Promise<WagerTransaction | null> {
    const rows = await this.em.execute<Array<Record<string, unknown>>>(
      `SELECT * FROM wager_transactions WHERE id = ? FOR UPDATE`,
      [id],
    );
    return rows[0] ? this.rawRowToDomain(rows[0]) : null;
  }

  /** Busca uma transação pela sua chave de idempotência única (idempotency_key). */
  async findByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<WagerTransaction | null> {
    const rows = await this.em.execute<Array<Record<string, unknown>>>(
      `SELECT * FROM wager_transactions WHERE idempotency_key = ?`,
      [idempotencyKey],
    );
    return rows[0] ? this.rawRowToDomain(rows[0]) : null;
  }

  /** Busca uma transação pelo par único (provider_id, external_transaction_id). */
  async findByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | null> {
    const rows = await this.em.execute<Array<Record<string, unknown>>>(
      `SELECT * FROM wager_transactions WHERE provider_id = ? AND external_transaction_id = ?`,
      [providerId, externalTransactionId],
    );
    return rows[0] ? this.rawRowToDomain(rows[0]) : null;
  }

  /** Verifica se já existe alguma reversão (REFUND ou ROLLBACK) com status PROCESSED para a referência informada. */
  async hasProcessedReversalFor(
    referenceTransactionId: string,
    _kind: WagerTransactionKind,
  ): Promise<boolean> {
    const rows = await this.em.execute<Array<{ cnt: string }>>(
      `SELECT COUNT(*)::text AS cnt
       FROM wager_transactions
       WHERE reference_transaction_id = ?
         AND status = 'PROCESSED'
         AND kind IN ('REFUND', 'ROLLBACK')`,
      [referenceTransactionId],
    );
    return Number(rows[0]?.cnt ?? 0) > 0;
  }

  /**
   * Seleciona transações em PENDING_REFERENCE vencidas usando FOR UPDATE SKIP LOCKED
   * para permitir múltiplos workers concorrentes sem colisão.
   */
  async claimDuePendingReferences(
    now: Date,
    limit: number,
  ): Promise<WagerTransaction[]> {
    const rows = await this.em.execute<Array<Record<string, unknown>>>(
      `SELECT *
       FROM wager_transactions
       WHERE status = 'PENDING_REFERENCE'
         AND (next_retry_at IS NULL OR next_retry_at <= ?)
       ORDER BY next_retry_at ASC NULLS FIRST, created_at ASC
       LIMIT ?
       FOR UPDATE SKIP LOCKED`,
      [now, limit],
    );

    return rows.map((r) => this.rawRowToDomain(r));
  }

  /** Insere uma nova WagerTransaction na tabela wager_transactions. */
  async insert(tx: WagerTransaction): Promise<void> {
    await this.em.execute(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        reference_external_transaction_id, reference_transaction_id,
        status, failure_code, observed_balance_amount, retry_attempts, next_retry_at,
        created_at, processed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tx.id,
        tx.providerId,
        tx.externalTransactionId,
        tx.idempotencyKey,
        tx.payloadHash,
        tx.walletId,
        tx.playerId,
        tx.roundId,
        tx.gameId,
        tx.kind,
        tx.money.amount,
        tx.money.currency,
        tx.referenceExternalTransactionId ?? null,
        tx.referenceTransactionId ?? null,
        tx.status,
        tx.failureCode ?? null,
        tx.observedBalance ? tx.observedBalance.amount : null,
        tx.retryAttempts,
        tx.nextRetryAt ?? null,
        tx.createdAt,
        tx.processedAt ?? null,
      ],
    );
  }

  /** Atualiza o estado, referência resolvida, código de falha e agendamentos de retry de uma WagerTransaction. */
  async update(tx: WagerTransaction): Promise<void> {
    await this.em.execute(
      `UPDATE wager_transactions
       SET status = ?,
           reference_transaction_id = ?,
           failure_code = ?,
           observed_balance_amount = ?,
           retry_attempts = ?,
           next_retry_at = ?,
           processed_at = ?
       WHERE id = ?`,
      [
        tx.status,
        tx.referenceTransactionId ?? null,
        tx.failureCode ?? null,
        tx.observedBalance ? tx.observedBalance.amount : null,
        tx.retryAttempts,
        tx.nextRetryAt ?? null,
        tx.processedAt ?? null,
        tx.id,
      ],
    );
  }

  /** Reidrata a entidade WagerTransaction a partir de uma linha da tabela wager_transactions. */
  private rawRowToDomain(r: Record<string, unknown>): WagerTransaction {
    const currency = String(r.currency).trim();
    return WagerTransaction.rehydrate({
      id: String(r.id),
      providerId: String(r.provider_id),
      externalTransactionId: String(r.external_transaction_id),
      idempotencyKey: String(r.idempotency_key),
      payloadHash: String(r.payload_hash).trim(),
      walletId: String(r.wallet_id),
      playerId: String(r.player_id),
      roundId: String(r.round_id),
      gameId: String(r.game_id),
      kind: String(r.kind) as WagerTransactionKind,
      money: Money.from({
        amount: formatNumeric(String(r.amount)),
        currency,
      }),
      referenceExternalTransactionId: r.reference_external_transaction_id
        ? String(r.reference_external_transaction_id)
        : undefined,
      referenceTransactionId: r.reference_transaction_id
        ? String(r.reference_transaction_id)
        : undefined,
      status: String(r.status) as WagerTransactionStatus,
      failureCode: r.failure_code
        ? (String(r.failure_code) as FailureCode)
        : undefined,
      observedBalance:
        r.observed_balance_amount !== null &&
        r.observed_balance_amount !== undefined
          ? Money.from({
              amount: formatNumeric(String(r.observed_balance_amount)),
              currency,
            })
          : undefined,
      retryAttempts: Number(r.retry_attempts ?? 0),
      nextRetryAt: r.next_retry_at
        ? new Date(String(r.next_retry_at))
        : undefined,
      createdAt: new Date(String(r.created_at)),
      processedAt: r.processed_at
        ? new Date(String(r.processed_at))
        : undefined,
    });
  }
}

/**
 * Repositório PostgreSQL para o Transactional Inbox (deduplicação de mensagens SQS).
 */
class MikroOrmInboxRepository implements InboxRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  /** Busca uma mensagem na Inbox pela chave composta (consumer_name, message_id). */
  async findByKey(
    consumerName: string,
    messageId: string,
  ): Promise<InboxMessage | null> {
    const rows = await this.em.execute<
      Array<{
        consumer_name: string;
        message_id: string;
        payload_hash: string;
        received_at: Date | string;
        processed_at: Date | string | null;
      }>
    >(
      `SELECT consumer_name, message_id, payload_hash, received_at, processed_at
       FROM inbox_messages
       WHERE consumer_name = ? AND message_id = ?`,
      [consumerName, messageId],
    );
    const row = rows[0];
    if (!row) {
      return null;
    }
    return InboxMessage.rehydrate({
      consumerName: row.consumer_name,
      messageId: row.message_id,
      payloadHash: row.payload_hash.trim(),
      receivedAt: new Date(row.received_at),
      processedAt: row.processed_at ? new Date(row.processed_at) : undefined,
    });
  }

  /** Insere ou atualiza (UPSERT idempotente) um registro na tabela inbox_messages. */
  async insert(inbox: InboxMessage): Promise<void> {
    await this.em.execute(
      `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (consumer_name, message_id) DO UPDATE
       SET processed_at = EXCLUDED.processed_at`,
      [
        inbox.consumerName,
        inbox.messageId,
        inbox.payloadHash,
        inbox.receivedAt,
        inbox.processedAt ?? null,
      ],
    );
  }

  /** Atualiza o timestamp processed_at de uma mensagem na Inbox. */
  async update(inbox: InboxMessage): Promise<void> {
    await this.em.execute(
      `UPDATE inbox_messages
       SET processed_at = ?
       WHERE consumer_name = ? AND message_id = ?`,
      [inbox.processedAt ?? null, inbox.consumerName, inbox.messageId],
    );
  }
}

/**
 * Repositório PostgreSQL para o Transactional Outbox.
 */
class MikroOrmOutboxRepository implements OutboxRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  /** Insere múltiplos eventos de integração na tabela outbox_messages dentro da transação SQL atual. */
  async insertMany(messages: OutboxMessage[]): Promise<void> {
    for (const msg of messages) {
      await this.em.execute(
        `INSERT INTO outbox_messages
         (id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at, published_at)
         VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?)`,
        [
          msg.id,
          msg.aggregateId,
          msg.eventType,
          JSON.stringify(msg.payload),
          msg.occurredAt,
          msg.attempts,
          msg.nextAttemptAt ?? null,
          msg.publishedAt ?? null,
        ],
      );
    }
  }

  /**
   * Seleciona mensagens pendentes e vencidas na Outbox usando FOR UPDATE SKIP LOCKED
   * para permitir múltiplos publishers concorrentes sem duplicação.
   */
  async claimDueMessages(now: Date, limit: number): Promise<OutboxMessage[]> {
    const rows = await this.em.execute<
      Array<{
        id: string;
        aggregate_id: string;
        event_type: string;
        payload: Record<string, unknown> | string;
        occurred_at: Date;
        attempts: number;
        next_attempt_at: Date | null;
        published_at: Date | null;
      }>
    >(
      `SELECT id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at, published_at
       FROM outbox_messages
       WHERE published_at IS NULL
         AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
       ORDER BY next_attempt_at ASC NULLS FIRST, occurred_at ASC
       LIMIT ?
       FOR UPDATE SKIP LOCKED`,
      [now, limit],
    );

    return rows.map((r) =>
      OutboxMessage.rehydrate({
        id: r.id,
        aggregateId: r.aggregate_id,
        eventType: r.event_type,
        payload:
          typeof r.payload === 'string'
            ? (JSON.parse(r.payload) as Record<string, unknown>)
            : r.payload,
        occurredAt: new Date(r.occurred_at),
        attempts: Number(r.attempts),
        nextAttemptAt: r.next_attempt_at
          ? new Date(r.next_attempt_at)
          : undefined,
        publishedAt: r.published_at ? new Date(r.published_at) : undefined,
      }),
    );
  }

  /** Atualiza o contador de tentativas, agendamento de retry e data de publicação da mensagem na Outbox. */
  async update(message: OutboxMessage): Promise<void> {
    await this.em.execute(
      `UPDATE outbox_messages
       SET attempts = ?, next_attempt_at = ?, published_at = ?
       WHERE id = ?`,
      [
        message.attempts,
        message.nextAttemptAt ?? null,
        message.publishedAt ?? null,
        message.id,
      ],
    );
  }

  /** Calcula a quantidade de mensagens pendentes na Outbox e a idade em segundos da mensagem mais antiga. */
  async getLagStats(
    now: Date,
  ): Promise<{ pendingCount: number; maxLagSeconds: number }> {
    const rows = await this.em.execute<
      Array<{ pending_count: string; oldest_occurred_at: Date | null }>
    >(
      `SELECT COUNT(*)::text AS pending_count,
              MIN(occurred_at) AS oldest_occurred_at
       FROM outbox_messages
       WHERE published_at IS NULL`,
    );

    const pendingCount = Number(rows[0]?.pending_count ?? 0);
    const oldest = rows[0]?.oldest_occurred_at
      ? new Date(rows[0].oldest_occurred_at)
      : null;
    const maxLagSeconds =
      pendingCount > 0 && oldest
        ? Math.max(0, (now.getTime() - oldest.getTime()) / 1000)
        : 0;

    return { pendingCount, maxLagSeconds };
  }
}

/**
 * Implementação do UnitOfWorkPort utilizando EntityManager.fork() e EntityManager.transactional() do MikroORM.
 */
export class MikroOrmUnitOfWork implements UnitOfWorkPort {
  constructor(
    private readonly orm: MikroORM,
    private readonly metrics?: MetricsPort,
  ) {}

  /**
   * Executa um bloco de trabalho dentro de uma transação ACID do PostgreSQL (BEGIN ... COMMIT/ROLLBACK)
   * e contabiliza conflitos de lock/deadlock na métrica Prometheus quando ocorrem.
   */
  async runInTransaction<T>(
    work: (repos: TransactionalRepositories) => Promise<T>,
  ): Promise<T> {
    const forkedEm = this.orm.em.fork({ clear: true });
    try {
      return await forkedEm.transactional(async (txEm) => {
        const repos = this.createRepos(txEm);
        return work(repos);
      });
    } catch (error: unknown) {
      const pgCode = (error as { code?: string })?.code;
      if (pgCode === '40P01' || pgCode === '55P03' || pgCode === '40001') {
        this.metrics?.incrementLockConflict();
      }
      throw error;
    }
  }

  /** Retorna repositórios de leitura com um EntityManager isolado (fork). */
  getReadRepositories(): TransactionalRepositories {
    const forkedEm = this.orm.em.fork({ clear: true });
    return this.createRepos(forkedEm);
  }

  /** Instancia o conjunto de repositórios amarrados ao EntityManager fornecido. */
  private createRepos(em: EntityManager): TransactionalRepositories {
    return {
      wallets: new MikroOrmWalletRepository(em),
      ledger: new MikroOrmLedgerRepository(em),
      transactions: new MikroOrmWagerTransactionRepository(em),
      inbox: new MikroOrmInboxRepository(em),
      outbox: new MikroOrmOutboxRepository(em),
    };
  }
}
