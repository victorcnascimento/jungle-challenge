import {
  InvalidTransactionContractError,
  InvalidTransactionStateError,
} from '../shared/domain-errors';
import { FailureCode } from '../shared/failure-code.enum';
import { Money } from '../shared/money.vo';
import { LedgerDirection } from '../wallet/wallet-ledger-entry.entity';

/**
 * Tipos de transação financeira suportados pelo domínio (Seção 6.3).
 */
export enum WagerTransactionKind {
  Opening = 'OPENING',   // interno: crédito de abertura da wallet
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

/**
 * Estados do ciclo de vida de uma WagerTransaction (Seção 6.3).
 * PROCESSED, REJECTED e FAILED são estados terminais imutáveis.
 */
export enum WagerTransactionStatus {
  Pending = 'PENDING',                   // aceita, ainda não aplicada
  PendingReference = 'PENDING_REFERENCE', // aguardando a transação referenciada
  Processed = 'PROCESSED',               // aplicada (terminal)
  Rejected = 'REJECTED',                 // violação de regra de negócio (terminal)
  Failed = 'FAILED',                     // erro permanente de infraestrutura (terminal, auditável)
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt?: Date;
}

export interface CreateOpeningTransactionProps {
  id: string;
  walletId: string;
  playerId: string;
  money: Money;
  payloadHash: string;
  createdAt?: Date;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  observedBalance?: Money;
  retryAttempts?: number;
  nextRetryAt?: Date;
}

/**
 * Entidade rica de domínio que representa uma transação de aposta e sua máquina de estados (Seção 6.3).
 */
export class WagerTransaction {
  public static readonly MAX_PENDING_REFERENCE_RETRIES = 5;
  public static readonly BASE_RETRY_DELAY_MS = 1000;

  /**
   * Construtor privado que encapsula o estado interno da transação.
   */
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** id no provedor — não o id interno */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _observedBalance?: Money,
    private _retryAttempts: number = 0,
    private _nextRetryAt?: Date,
  ) {}

  /**
   * Fábrica para transações submetidas via API ou fila SQS.
   * Nasce em PENDING. Valida a proibição de OPENING externo e a exigência de referência por kind (Seção 6.3).
   */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening) {
      throw new InvalidTransactionContractError(
        'OPENING transaction kind is strictly internal and cannot be submitted via API or queue.',
      );
    }

    if (
      (props.kind === WagerTransactionKind.Refund ||
        props.kind === WagerTransactionKind.Rollback) &&
      (!props.referenceExternalTransactionId ||
        props.referenceExternalTransactionId.trim().length === 0)
    ) {
      throw new InvalidTransactionContractError(
        `${props.kind} transactions require a non-empty referenceExternalTransactionId.`,
      );
    }

    if (props.kind !== WagerTransactionKind.Loss && !props.money.isPositive()) {
      throw new InvalidTransactionContractError(
        `${props.kind} transaction amount must be strictly greater than 0.00.`,
      );
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.createdAt ?? new Date(),
      WagerTransactionStatus.Pending,
    );
  }

  /**
   * Fábrica interna exclusiva para abertura de Wallet com saldo inicial > 0 (Seções 6.3 e 9).
   */
  static createOpening(props: CreateOpeningTransactionProps): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new InvalidTransactionContractError(
        'OPENING transaction requires positive initial balance.',
      );
    }

    const now = props.createdAt ?? new Date();
    const tx = new WagerTransaction(
      props.id,
      'internal-system',
      `opening:${props.walletId}`,
      `opening:${props.walletId}`,
      props.payloadHash,
      props.walletId,
      props.playerId,
      `opening-round:${props.walletId}`,
      'wallet-opening',
      WagerTransactionKind.Opening,
      props.money,
      undefined,
      now,
      WagerTransactionStatus.Pending,
    );
    tx.markProcessed(undefined, now, props.money);
    return tx;
  }

  /**
   * Reidratação a partir da persistência — não revalida transições (Seção 6.0).
   */
  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
      state.observedBalance,
      state.retryAttempts ?? 0,
      state.nextRetryAt,
    );
  }

  /** Retorna o status atual da máquina de estados da transação. */
  get status(): WagerTransactionStatus {
    return this._status;
  }

  /** Retorna o ID interno da transação referenciada após ser resolvida. */
  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  /** Retorna o código de falha estruturado quando a transação é REJECTED ou FAILED. */
  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  /** Retorna a data/hora em que a transação atingiu um estado terminal. */
  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  /** Retorna o saldo observado na Wallet no instante em que a transação foi processada (usado em replays). */
  get observedBalance(): Money | undefined {
    return this._observedBalance;
  }

  /** Retorna a quantidade de tentativas já realizadas para resolver uma referência ausente. */
  get retryAttempts(): number {
    return this._retryAttempts;
  }

  /** Retorna a data/hora agendada para a próxima tentativa de resolução de referência. */
  get nextRetryAt(): Date | undefined {
    return this._nextRetryAt;
  }

  // ---- Transições de Estado ----

  /**
   * Transiciona a transação para PROCESSED (estado terminal), gravando a referência resolvida
   * e o saldo observado naquele exato momento.
   */
  markProcessed(
    referenceTransactionId: string | undefined,
    at: Date,
    observedBalance?: Money,
  ): void {
    this.assertNotTerminal(WagerTransactionStatus.Processed);

    if (this.requiresReference() && !referenceTransactionId) {
      throw new InvalidTransactionContractError(
        `Cannot mark ${this.kind} as PROCESSED without resolved referenceTransactionId.`,
      );
    }

    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._nextRetryAt = undefined;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
  }

  /**
   * Transiciona a transação para PENDING_REFERENCE quando a transação referenciada ainda não chegou (fora de ordem).
   */
  markPendingReference(now: Date = new Date(), observedBalance?: Money): void {
    this.assertNotTerminal(WagerTransactionStatus.PendingReference);

    if (!this.referenceExternalTransactionId) {
      throw new InvalidTransactionContractError(
        'Cannot transition to PENDING_REFERENCE without referenceExternalTransactionId.',
      );
    }

    this._status = WagerTransactionStatus.PendingReference;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
    this.scheduleNextReferenceCheck(now);
  }

  /**
   * Incrementa a contagem de tentativas de resolução da referência e agenda o próximo retry com backoff exponencial.
   * Retorna false caso o limite de tentativas tenha sido esgotado.
   */
  scheduleNextReferenceCheck(now: Date = new Date()): boolean {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(
        this._status,
        WagerTransactionStatus.PendingReference,
      );
    }

    this._retryAttempts += 1;
    if (this._retryAttempts > WagerTransaction.MAX_PENDING_REFERENCE_RETRIES) {
      return false;
    }

    const delayMs =
      WagerTransaction.BASE_RETRY_DELAY_MS * Math.pow(2, this._retryAttempts - 1);
    this._nextRetryAt = new Date(now.getTime() + delayMs);
    return true;
  }

  /**
   * Verifica se o número de tentativas de resolução de referência já atingiu o limite máximo.
   */
  hasExhaustedReferenceRetries(): boolean {
    return this._retryAttempts >= WagerTransaction.MAX_PENDING_REFERENCE_RETRIES;
  }

  /**
   * Transiciona a transação para REJECTED (estado terminal) devido a violação de regra de negócio,
   * registrando o FailureCode correspondente e o saldo observado.
   */
  reject(code: FailureCode, at: Date = new Date(), observedBalance?: Money): void {
    this.assertNotTerminal(WagerTransactionStatus.Rejected);
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._processedAt = at;
    this._nextRetryAt = undefined;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
  }

  /**
   * Transiciona a transação para FAILED (estado terminal) em caso de falha permanente auditável.
   */
  fail(code: FailureCode, at: Date = new Date(), observedBalance?: Money): void {
    this.assertNotTerminal(WagerTransactionStatus.Failed);
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._processedAt = at;
    this._nextRetryAt = undefined;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
  }

  // ---- Consultas de Domínio ----

  /**
   * Indica se a transação já atingiu um estado terminal imutável (PROCESSED, REJECTED ou FAILED).
   */
  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  /**
   * Indica se o tipo de transação altera o saldo da carteira (false para LOSS).
   */
  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss && !this.money.isZero();
  }

  /**
   * Indica se a operação exige obrigatoriamente uma transação de referência (true para REFUND e ROLLBACK).
   */
  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  /**
   * Verifica se o hash canônico de um novo payload coincide com o payloadHash original desta transação.
   */
  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /**
   * Determina a direção contábil do lançamento no ledger (DEBIT ou CREDIT).
   * Para ROLLBACK, inverte a direção da transação referenciada.
   */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;

      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;

      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new InvalidTransactionContractError(
            'ROLLBACK requires a resolved reference transaction to determine ledger direction.',
          );
        }
        if (reference.kind === WagerTransactionKind.Bet) {
          return LedgerDirection.Credit;
        }
        if (
          reference.kind === WagerTransactionKind.Win ||
          reference.kind === WagerTransactionKind.Refund
        ) {
          return LedgerDirection.Debit;
        }
        throw new InvalidTransactionContractError(
          `Cannot determine ROLLBACK ledger direction for reference kind "${reference.kind}".`,
        );
      }

      case WagerTransactionKind.Loss:
        throw new InvalidTransactionContractError(
          'LOSS transactions do not affect balance and have no ledger direction.',
        );
    }
  }

  /**
   * Valida todas as invariantes de negócio entre esta transação e a transação referenciada (Seção 7, Regras 2, 3 e 5).
   * Retorna null se válida, ou o FailureCode específico se houver violação.
   */
  validateReference(reference: WagerTransaction): FailureCode | null {
    // Regra 7.2: mesmo provider, player, wallet, moeda e rodada
    if (
      reference.providerId !== this.providerId ||
      reference.playerId !== this.playerId ||
      reference.walletId !== this.walletId ||
      reference.money.currency !== this.money.currency ||
      reference.roundId !== this.roundId
    ) {
      return FailureCode.ReferenceMismatch;
    }

    // Regra 7.3: tipos permitidos de referência
    if (this.kind === WagerTransactionKind.Refund) {
      if (reference.kind !== WagerTransactionKind.Bet) {
        return FailureCode.InvalidReferenceKind;
      }
    } else if (this.kind === WagerTransactionKind.Rollback) {
      const allowedForRollback = [
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Refund,
      ];
      if (!allowedForRollback.includes(reference.kind)) {
        return FailureCode.InvalidReferenceKind;
      }
    } else if (this.kind === WagerTransactionKind.Win) {
      if (reference.kind !== WagerTransactionKind.Bet) {
        return FailureCode.InvalidReferenceKind;
      }
    }

    // A transação referenciada precisa ter sido efetivamente PROCESSED
    if (reference.status !== WagerTransactionStatus.Processed) {
      return FailureCode.ReferenceNotProcessed;
    }

    // Regra 7.5: O valor de REFUND/ROLLBACK deve ser igual ao valor da referência
    if (this.requiresReference() && !this.money.equals(reference.money)) {
      return FailureCode.ReferenceMismatch;
    }

    return null;
  }

  /**
   * Garante que a transação não está em um estado terminal antes de aplicar uma transição.
   */
  private assertNotTerminal(targetStatus: WagerTransactionStatus): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this._status, targetStatus);
    }
  }
}
