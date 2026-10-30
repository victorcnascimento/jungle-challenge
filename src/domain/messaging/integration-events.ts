import { randomUUID } from 'crypto';
import { FailureCode } from '../shared/failure-code.enum';
import { MoneyProps } from '../shared/money.vo';
import {
  WagerTransaction,
  WagerTransactionKind,
} from '../wagering/wager-transaction.entity';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../wallet/wallet-ledger-entry.entity';
import { Wallet } from '../wallet/wallet.aggregate';

export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
  data: T;
}

export interface EventContext {
  eventId?: string;
  correlationId: string;
  causationId?: string;
  occurredAt?: Date;
}

export interface SerializedIntegrationEvent<T> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string; // ISO-8601
  version: number;
  data: T;
}

/**
 * Classe abstrata base para todos os eventos de integração gravados na Outbox (Seção 11).
 * Força que cada subclasse defina explicitamente seu eventType e version.
 */
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  public readonly eventId: string;
  public readonly aggregateId: string;
  public readonly correlationId: string;
  public readonly causationId?: string;
  public readonly occurredAt: Date;
  public readonly data: Readonly<T>;

  /**
   * Construtor protegido que inicializa os metadados de rastreamento e congela o payload data.
   */
  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = Object.freeze({ ...props.data });
  }

  /**
   * Serializa o evento no formato de envelope JSON estável e versionável para gravação na Outbox.
   */
  toJSON(): SerializedIntegrationEvent<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

// ============================================================================
// 1. WalletBalanceChanged — emitido SOMENTE quando o saldo muda
// ============================================================================

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  /**
   * Fábrica que constrói o evento WalletBalanceChanged a partir do estado atualizado da Wallet e do lançamento no Ledger.
   */
  static from(
    wallet: Wallet,
    entry: WalletLedgerEntry,
    ctx: EventContext,
  ): WalletBalanceChanged {
    return new WalletBalanceChanged({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: wallet.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}

// ============================================================================
// 2. WagerTransactionProcessed — emitido para qualquer transação aplicada, inclusive LOSS
// ============================================================================

export interface WagerTransactionProcessedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  balance: MoneyProps;
  referenceTransactionId?: string;
  processedAt: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  /**
   * Fábrica que constrói o evento WagerTransactionProcessed para qualquer transação efetivada com sucesso.
   */
  static from(
    tx: WagerTransaction,
    balance: MoneyProps,
    ctx: EventContext,
  ): WagerTransactionProcessed {
    const occurredAt = ctx.occurredAt ?? tx.processedAt ?? new Date();
    return new WagerTransactionProcessed({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: tx.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt,
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        balance,
        ...(tx.referenceTransactionId
          ? { referenceTransactionId: tx.referenceTransactionId }
          : {}),
        processedAt: (tx.processedAt ?? occurredAt).toISOString(),
      },
    });
  }
}

// ============================================================================
// 3. WagerTransactionRejected — emitido quando transação é rejeitada por regra de negócio
// ============================================================================

export interface WagerTransactionRejectedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  failureCode: FailureCode;
  balance: MoneyProps;
  rejectedAt: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  /**
   * Fábrica que constrói o evento WagerTransactionRejected carregando o FailureCode da rejeição.
   */
  static from(
    tx: WagerTransaction,
    balance: MoneyProps,
    ctx: EventContext,
  ): WagerTransactionRejected {
    const occurredAt = ctx.occurredAt ?? tx.processedAt ?? new Date();
    return new WagerTransactionRejected({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: tx.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt,
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        failureCode: tx.failureCode ?? FailureCode.PermanentInfrastructureFailure,
        balance,
        rejectedAt: (tx.processedAt ?? occurredAt).toISOString(),
      },
    });
  }
}

// ============================================================================
// 4. WagerTransactionPendingReference — emitido quando a referência está ausente (fora de ordem)
// ============================================================================

export interface WagerTransactionPendingReferenceData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  referenceExternalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  retryAttempts: number;
  nextRetryAt?: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  /**
   * Fábrica que constrói o evento WagerTransactionPendingReference quando uma operação referenciada chega fora de ordem.
   */
  static from(
    tx: WagerTransaction,
    ctx: EventContext,
  ): WagerTransactionPendingReference {
    const occurredAt = ctx.occurredAt ?? new Date();
    return new WagerTransactionPendingReference({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: tx.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt,
      data: {
        transactionId: tx.id,
        providerId: tx.providerId,
        externalTransactionId: tx.externalTransactionId,
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? '',
        walletId: tx.walletId,
        playerId: tx.playerId,
        roundId: tx.roundId,
        gameId: tx.gameId,
        kind: tx.kind,
        money: tx.money.toJSON(),
        retryAttempts: tx.retryAttempts,
        ...(tx.nextRetryAt ? { nextRetryAt: tx.nextRetryAt.toISOString() } : {}),
      },
    });
  }
}
