import { randomUUID } from 'crypto';
import { InboxMessage } from '../../domain/messaging/inbox-message.entity';
import {
  IntegrationEvent,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/messaging/integration-events';
import { OutboxMessage } from '../../domain/messaging/outbox-message.entity';
import {
  computeWagerPayloadHash,
  sha256Canonical,
} from '../../domain/shared/canonical-hash';
import {
  EntityNotFoundError,
  IdempotencyConflictError,
  InvalidTransactionContractError,
} from '../../domain/shared/domain-errors';
import { FailureCode } from '../../domain/shared/failure-code.enum';
import { Money, MoneyProps } from '../../domain/shared/money.vo';
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
import { LoggerPort, MetricsPort } from '../ports/observability.port';
import {
  TransactionalRepositories,
  UnitOfWorkPort,
} from '../ports/unit-of-work.port';

export interface ProcessWagerTransactionCommand {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  channel?: 'http' | 'sqs';
  inbox?: {
    messageId: string;
    consumerName: string;
  };
}

export interface ProcessWagerTransactionResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: FailureCode;
}

/**
 * Caso de uso central compartilhado pela API HTTP e pelo Consumidor SQS (Seções 7, 8, 9, 10 e 11).
 * Garante idempotência persistente, controle de concorrência por walletId e atomicidade com Ledger, Inbox e Outbox.
 */
export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {}

  /**
   * Executa o processamento completo de uma transação de aposta dentro de uma única transação SQL.
   */
  async execute(
    command: ProcessWagerTransactionCommand,
  ): Promise<ProcessWagerTransactionResult> {
    const startedAt = performance.now();
    const channel = command.channel ?? 'http';
    const correlationId = command.correlationId ?? randomUUID();

    if (!command.idempotencyKey || command.idempotencyKey.trim().length === 0) {
      throw new InvalidTransactionContractError('Idempotency-Key is required.');
    }

    const money = Money.from(command.money);
    const payloadHash = computeWagerPayloadHash({
      providerId: command.providerId,
      externalTransactionId: command.externalTransactionId,
      playerId: command.playerId,
      walletId: command.walletId,
      roundId: command.roundId,
      gameId: command.gameId,
      kind: command.kind,
      money: money.toJSON(),
      referenceExternalTransactionId: command.referenceExternalTransactionId,
    });

    // Pré-validação rápida de contrato antes de abrir transação SQL
    WagerTransaction.create({
      id: '00000000-0000-0000-0000-000000000000',
      providerId: command.providerId,
      externalTransactionId: command.externalTransactionId,
      idempotencyKey: command.idempotencyKey,
      payloadHash,
      walletId: command.walletId,
      playerId: command.playerId,
      roundId: command.roundId,
      gameId: command.gameId,
      kind: command.kind,
      money,
      referenceExternalTransactionId: command.referenceExternalTransactionId,
    });

    try {
      const result = await this.uow.runInTransaction(async (repos) => {
        const now = new Date();

        // 1. Se vier pela fila SQS, deduplicar via Transactional Inbox por (consumerName, messageId)
        if (command.inbox) {
          const existingInbox = await repos.inbox.findByKey(
            command.inbox.consumerName,
            command.inbox.messageId,
          );
          if (existingInbox && existingInbox.isProcessed()) {
            this.metrics.incrementDuplicateDetected('inbox');
            this.logger.info('Duplicate SQS message detected via Inbox', {
              correlationId,
              messageId: command.inbox.messageId,
              walletId: command.walletId,
              providerId: command.providerId,
            });

            const existingTx = await repos.transactions.findByIdempotencyKey(
              command.idempotencyKey,
            );
            const wallet = await repos.wallets.findById(command.walletId);
            if (existingTx && wallet) {
              return this.buildReplayResult(existingTx, wallet);
            }
          }
        }

        // 2. Verificar rapidamente se a Idempotency-Key já existe antes mesmo de bloquear a Wallet
        // (protege o caso em que o atacante/cliente envia a mesma Idempotency-Key trocando para uma walletId diferente ou inexistente)
        const preExistingByKey = await repos.transactions.findByIdempotencyKey(
          command.idempotencyKey,
        );
        if (preExistingByKey && !preExistingByKey.matchesPayload(payloadHash)) {
          throw new IdempotencyConflictError(command.idempotencyKey);
        }

        // 3. Adquirir Lock Pessimista na Wallet (SELECT ... FOR UPDATE por walletId).
        // Serializa todas as requisições concorrentes para a mesma carteira (incluindo 50 replays paralelos).
        const wallet = await repos.wallets.findByIdForUpdate(command.walletId);

        // 4. Verificar Idempotência Persistente por idempotencyKey sob o lock da Wallet
        const existingByKey = await repos.transactions.findByIdempotencyKey(
          command.idempotencyKey,
        );
        if (existingByKey) {
          if (!existingByKey.matchesPayload(payloadHash)) {
            throw new IdempotencyConflictError(command.idempotencyKey);
          }

          this.metrics.incrementDuplicateDetected(channel);
          if (command.inbox) {
            await this.ensureInboxMarkedProcessed(
              repos,
              command.inbox.consumerName,
              command.inbox.messageId,
              payloadHash,
              now,
            );
          }

          const refWallet =
            wallet ?? (await repos.wallets.findById(existingByKey.walletId));
          const fallbackBalance = refWallet
            ? refWallet.balance
            : Money.zero(money.currency);

          this.logger.info('Idempotent replay detected', {
            correlationId,
            messageId: command.inbox?.messageId,
            transactionId: existingByKey.id,
            walletId: existingByKey.walletId,
            providerId: command.providerId,
          });

          return this.buildReplayResult(existingByKey, fallbackBalance);
        }

        // 4. Verificar se (providerId, externalTransactionId) já foi usado com outra Idempotency-Key
        const existingByExternal =
          await repos.transactions.findByProviderAndExternalId(
            command.providerId,
            command.externalTransactionId,
          );
        if (existingByExternal) {
          if (!existingByExternal.matchesPayload(payloadHash)) {
            throw new IdempotencyConflictError(command.idempotencyKey);
          }
          this.metrics.incrementDuplicateDetected(channel);
          if (command.inbox) {
            await this.ensureInboxMarkedProcessed(
              repos,
              command.inbox.consumerName,
              command.inbox.messageId,
              payloadHash,
              now,
            );
          }
          const refWallet =
            wallet ?? (await repos.wallets.findById(existingByExternal.walletId));
          const fallbackBalance = refWallet
            ? refWallet.balance
            : Money.zero(money.currency);
          return this.buildReplayResult(existingByExternal, fallbackBalance);
        }

        if (!wallet) {
          throw new EntityNotFoundError('Wallet', command.walletId);
        }

        if (wallet.playerId !== command.playerId) {
          throw new InvalidTransactionContractError(
            `Wallet "${command.walletId}" does not belong to player "${command.playerId}".`,
          );
        }

        // 5. Criar WagerTransaction em PENDING
        const tx = WagerTransaction.create({
          id: randomUUID(),
          providerId: command.providerId,
          externalTransactionId: command.externalTransactionId,
          idempotencyKey: command.idempotencyKey,
          payloadHash,
          walletId: command.walletId,
          playerId: command.playerId,
          roundId: command.roundId,
          gameId: command.gameId,
          kind: command.kind,
          money,
          referenceExternalTransactionId: command.referenceExternalTransactionId,
          createdAt: now,
        });

        // 6. Aplicar regras de negócio sobre a Wallet bloqueada
        const { events, ledgerEntry } = await this.applyBusinessRules(
          repos,
          wallet,
          tx,
          correlationId,
          now,
        );

        // 7. Persistir transação ANTES do ledger (para satisfazer a FK wallet_ledger_entries.transaction_id),
        // seguido do ledger, inbox e outbox atomicamente na mesma transação SQL
        await repos.transactions.insert(tx);

        if (ledgerEntry) {
          await repos.ledger.insert(ledgerEntry);
        }

        if (command.inbox) {
          await this.ensureInboxMarkedProcessed(
            repos,
            command.inbox.consumerName,
            command.inbox.messageId,
            payloadHash,
            now,
          );
        }

        if (events.length > 0) {
          await repos.outbox.insertMany(
            events.map((ev) => OutboxMessage.enqueue(ev)),
          );
        }

        this.metrics.incrementTransactionStatus(
          tx.kind,
          tx.status,
          tx.failureCode,
        );

        this.logger.info('Wager transaction processed', {
          correlationId,
          messageId: command.inbox?.messageId,
          transactionId: tx.id,
          walletId: wallet.id,
          providerId: tx.providerId,
          kind: tx.kind,
          status: tx.status,
          failureCode: tx.failureCode,
        });

        return {
          transactionId: tx.id,
          status: tx.status,
          balance: (tx.observedBalance ?? wallet.balance).toJSON(),
          idempotentReplay: false,
          ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
        };
      });

      const durationMs = performance.now() - startedAt;
      this.metrics.observeProcessingLatency(channel, command.kind, durationMs);
      return result;
    } catch (error: unknown) {
      const durationMs = performance.now() - startedAt;
      this.metrics.observeProcessingLatency(channel, command.kind, durationMs);

      // Tratamento determinístico de corrida na constraint UNIQUE (código 23505 do PostgreSQL)
      // caso duas requisições concorrentes usem a mesma Idempotency-Key em carteiras distintas:
      const pgCode = (error as { code?: string })?.code;
      if (pgCode === '23505') {
        const readRepos = this.uow.getReadRepositories();
        const winnerTx =
          (await readRepos.transactions.findByIdempotencyKey(
            command.idempotencyKey,
          )) ??
          (await readRepos.transactions.findByProviderAndExternalId(
            command.providerId,
            command.externalTransactionId,
          ));

        if (winnerTx) {
          if (!winnerTx.matchesPayload(payloadHash)) {
            throw new IdempotencyConflictError(command.idempotencyKey);
          }
          this.metrics.incrementDuplicateDetected(channel);
          const refWallet = await readRepos.wallets.findById(winnerTx.walletId);
          return this.buildReplayResult(
            winnerTx,
            refWallet ? refWallet.balance : Money.zero(money.currency),
          );
        }
      }

      throw error;
    }
  }

  /**
   * Executa a transição financeira e contábil para uma WagerTransaction sob lock da Wallet.
   * Reutilizado também pelo worker de reprocessamento de PENDING_REFERENCE.
   */
  public async applyBusinessRules(
    repos: TransactionalRepositories,
    wallet: Wallet,
    tx: WagerTransaction,
    correlationId: string,
    now: Date,
  ): Promise<{
    events: IntegrationEvent<unknown>[];
    ledgerEntry?: WalletLedgerEntry;
  }> {
    const events: IntegrationEvent<unknown>[] = [];

    // Validar compatibilidade de moeda com a Wallet
    if (wallet.currency !== tx.money.currency) {
      tx.reject(FailureCode.CurrencyMismatch, now, wallet.balance);
      events.push(
        WagerTransactionRejected.from(tx, wallet.balance.toJSON(), {
          correlationId,
          occurredAt: now,
        }),
      );
      return { events };
    }

    // Resolver transação de referência quando informada
    let referenceTx: WagerTransaction | null = null;
    if (tx.referenceExternalTransactionId) {
      referenceTx = await repos.transactions.findByProviderAndExternalId(
        tx.providerId,
        tx.referenceExternalTransactionId,
      );

      // Se a referência ainda não chegou ou ainda não atingiu estado terminal → PENDING_REFERENCE (Regra 7.8)
      if (
        !referenceTx ||
        referenceTx.status === WagerTransactionStatus.Pending ||
        referenceTx.status === WagerTransactionStatus.PendingReference
      ) {
        tx.markPendingReference(now, wallet.balance);
        events.push(
          WagerTransactionPendingReference.from(tx, {
            correlationId,
            occurredAt: now,
          }),
        );
        return { events };
      }

      // Validar regras de referência (Regras 7.2, 7.3, 7.5)
      const refFailure = tx.validateReference(referenceTx);
      if (refFailure !== null) {
        tx.reject(refFailure, now, wallet.balance);
        events.push(
          WagerTransactionRejected.from(tx, wallet.balance.toJSON(), {
            correlationId,
            occurredAt: now,
          }),
        );
        return { events };
      }

      // Regra 7.4: Uma referência não pode ser revertida duas vezes
      if (tx.requiresReference()) {
        const alreadyReversed = await repos.transactions.hasProcessedReversalFor(
          referenceTx.id,
          tx.kind,
        );
        if (alreadyReversed) {
          tx.reject(FailureCode.DuplicateReversal, now, wallet.balance);
          events.push(
            WagerTransactionRejected.from(tx, wallet.balance.toJSON(), {
              correlationId,
              occurredAt: now,
            }),
          );
          return { events };
        }
      }
    }

    // Caso LOSS: não altera saldo nem gera lançamento no ledger (Seções 6.4 e 7)
    if (tx.kind === WagerTransactionKind.Loss) {
      tx.markProcessed(referenceTx?.id, now, wallet.balance);
      events.push(
        WagerTransactionProcessed.from(tx, wallet.balance.toJSON(), {
          correlationId,
          occurredAt: now,
        }),
      );
      return { events };
    }

    const direction = tx.ledgerDirectionFor(referenceTx ?? undefined);

    if (direction === LedgerDirection.Debit) {
      // Verificar se há saldo suficiente para o débito
      if (!wallet.canAfford(tx.money)) {
        // Regra 7.9: Reversão que produziria saldo negativo tem failureCode distinto de BET sem saldo
        const failureCode =
          tx.kind === WagerTransactionKind.Rollback
            ? FailureCode.NegativeBalanceOnReversal
            : FailureCode.InsufficientFunds;

        tx.reject(failureCode, now, wallet.balance);
        events.push(
          WagerTransactionRejected.from(tx, wallet.balance.toJSON(), {
            correlationId,
            occurredAt: now,
          }),
        );
        return { events };
      }

      const ledgerEntry = wallet.debit({
        entryId: randomUUID(),
        transactionId: tx.id,
        money: tx.money,
        occurredAt: now,
      });

      tx.markProcessed(referenceTx?.id, now, wallet.balance);

      await repos.wallets.update(wallet);

      events.push(
        WagerTransactionProcessed.from(tx, wallet.balance.toJSON(), {
          correlationId,
          occurredAt: now,
        }),
        WalletBalanceChanged.from(wallet, ledgerEntry, {
          correlationId,
          occurredAt: now,
        }),
      );
      return { events, ledgerEntry };
    }

    // direction === LedgerDirection.Credit (WIN, REFUND, ou ROLLBACK de BET)
    const ledgerEntry = wallet.credit({
      entryId: randomUUID(),
      transactionId: tx.id,
      money: tx.money,
      occurredAt: now,
    });

    tx.markProcessed(referenceTx?.id, now, wallet.balance);

    await repos.wallets.update(wallet);

    events.push(
      WagerTransactionProcessed.from(tx, wallet.balance.toJSON(), {
        correlationId,
        occurredAt: now,
      }),
      WalletBalanceChanged.from(wallet, ledgerEntry, {
        correlationId,
        occurredAt: now,
      }),
    );

    return { events, ledgerEntry };
  }

  /**
   * Consulta uma transação pelo seu ID interno ou lança EntityNotFoundError.
   */
  public async getTransactionById(
    transactionId: string,
  ): Promise<WagerTransaction> {
    const tx = await this.uow
      .getReadRepositories()
      .transactions.findById(transactionId);
    if (!tx) {
      throw new EntityNotFoundError('WagerTransaction', transactionId);
    }
    return tx;
  }

  /**
   * Consulta uma transação pelo par (providerId, externalTransactionId) ou lança EntityNotFoundError.
   */
  public async getTransactionByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction> {
    const tx = await this.uow
      .getReadRepositories()
      .transactions.findByProviderAndExternalId(
        providerId,
        externalTransactionId,
      );
    if (!tx) {
      throw new EntityNotFoundError(
        'WagerTransaction',
        `${providerId}:${externalTransactionId}`,
      );
    }
    return tx;
  }

  /**
   * Registra ou atualiza a mensagem na tabela inbox_messages como processada dentro da transação SQL atual.
   */
  private async ensureInboxMarkedProcessed(
    repos: TransactionalRepositories,
    consumerName: string,
    messageId: string,
    payloadHash: string,
    now: Date,
  ): Promise<void> {
    const existing = await repos.inbox.findByKey(consumerName, messageId);
    if (!existing) {
      const inbox = InboxMessage.receive({
        consumerName,
        messageId,
        payloadHash: sha256Canonical({ consumerName, messageId, payloadHash }),
        receivedAt: now,
      });
      inbox.markProcessed(now);
      await repos.inbox.insert(inbox);
    } else if (!existing.isProcessed()) {
      existing.markProcessed(now);
      await repos.inbox.update(existing);
    }
  }

  /**
   * Monta a resposta de replay idempotente preservando o status original e o saldo observado na época (Regra 7.7).
   */
  private buildReplayResult(
    existingTx: WagerTransaction,
    walletOrBalance: Wallet | Money,
  ): ProcessWagerTransactionResult {
    const fallbackMoney =
      walletOrBalance instanceof Wallet
        ? walletOrBalance.balance
        : walletOrBalance;
    const observedBalance = existingTx.observedBalance ?? fallbackMoney;

    return {
      transactionId: existingTx.id,
      status: existingTx.status,
      balance: observedBalance.toJSON(),
      idempotentReplay: true,
      ...(existingTx.failureCode ? { failureCode: existingTx.failureCode } : {}),
    };
  }
}
