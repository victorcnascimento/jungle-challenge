import { randomUUID } from 'crypto';
import {
  WagerTransactionRejected,
} from '../../domain/messaging/integration-events';
import { OutboxMessage } from '../../domain/messaging/outbox-message.entity';
import { FailureCode } from '../../domain/shared/failure-code.enum';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction.entity';
import { LoggerPort, MetricsPort } from '../ports/observability.port';
import { UnitOfWorkPort } from '../ports/unit-of-work.port';
import { ProcessWagerTransactionUseCase } from './process-wager-transaction.use-case';

/**
 * Worker de reprocessamento de transações em PENDING_REFERENCE (Seção 7.1).
 * Utiliza FOR UPDATE SKIP LOCKED e backoff exponencial até o limite máximo de tentativas.
 */
export class RetryPendingReferencesUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {}

  /**
   * Seleciona um lote de transações em PENDING_REFERENCE cujo horário de retry já venceu:
   * - Se a transação referenciada já chegou e é terminal, aplica as regras de negócio e finaliza.
   * - Se ainda não chegou e restam tentativas, reagenda com backoff exponencial.
   * - Se esgotou o limite de tentativas, rejeita com REFERENCE_NOT_FOUND_TIMEOUT e publica evento na Outbox.
   */
  async executeBatch(batchSize: number = 25): Promise<number> {
    const now = new Date();

    return this.uow.runInTransaction(async (repos) => {
      const dueTransactions = await repos.transactions.claimDuePendingReferences(
        now,
        batchSize,
      );

      let processedCount = 0;

      for (const tx of dueTransactions) {
        const correlationId = randomUUID();
        this.metrics.incrementRetry('pending_reference');

        const wallet = await repos.wallets.findByIdForUpdate(tx.walletId);
        if (!wallet) {
          continue;
        }

        const referenceTx = tx.referenceExternalTransactionId
          ? await repos.transactions.findByProviderAndExternalId(
              tx.providerId,
              tx.referenceExternalTransactionId,
            )
          : null;

        // Se a referência chegou e já não está pendente, aplica as regras de negócio!
        if (
          referenceTx &&
          referenceTx.status !== WagerTransactionStatus.Pending &&
          referenceTx.status !== WagerTransactionStatus.PendingReference
        ) {
          const { events, ledgerEntry } =
            await this.processWagerUseCase.applyBusinessRules(
              repos,
              wallet,
              tx,
              correlationId,
              now,
            );

          await repos.transactions.update(tx);
          if (ledgerEntry) {
            await repos.ledger.insert(ledgerEntry);
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

          this.logger.info('Resolved previously PENDING_REFERENCE transaction', {
            correlationId,
            transactionId: tx.id,
            walletId: tx.walletId,
            providerId: tx.providerId,
            status: tx.status,
            failureCode: tx.failureCode,
          });

          processedCount += 1;
          continue;
        }

        // Referência ainda ausente: verificar se esgotou o limite de tentativas (Seção 7.1)
        const hasMoreRetries = tx.scheduleNextReferenceCheck(now);
        if (!hasMoreRetries) {
          tx.reject(FailureCode.ReferenceNotFoundTimeout, now, wallet.balance);
          const rejectedEvent = WagerTransactionRejected.from(
            tx,
            wallet.balance.toJSON(),
            { correlationId, occurredAt: now },
          );

          await repos.transactions.update(tx);
          await repos.outbox.insertMany([OutboxMessage.enqueue(rejectedEvent)]);

          this.metrics.incrementTransactionStatus(
            tx.kind,
            tx.status,
            tx.failureCode,
          );

          this.logger.warn(
            'PENDING_REFERENCE transaction exhausted retries and was REJECTED',
            {
              correlationId,
              transactionId: tx.id,
              walletId: tx.walletId,
              providerId: tx.providerId,
              failureCode: tx.failureCode,
            },
          );
        } else {
          await repos.transactions.update(tx);
          this.logger.debug(
            'Rescheduled PENDING_REFERENCE transaction with exponential backoff',
            {
              correlationId,
              transactionId: tx.id,
              walletId: tx.walletId,
              providerId: tx.providerId,
              retryAttempts: tx.retryAttempts,
            },
          );
        }

        processedCount += 1;
      }

      return processedCount;
    });
  }
}
