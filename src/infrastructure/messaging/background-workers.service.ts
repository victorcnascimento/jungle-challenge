import { OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { LoggerPort } from '../../application/ports/observability.port';
import { PublishOutboxMessagesUseCase } from '../../application/use-cases/publish-outbox.use-case';
import { RetryPendingReferencesUseCase } from '../../application/use-cases/retry-pending-references.use-case';
import { SqsMessagingService } from './sqs.service';

/**
 * Serviço de infraestrutura que gerencia o ciclo de vida dos workers agendados em background:
 * 1. Publicação periódica da Transactional Outbox
 * 2. Reprocessamento periódico de transações em PENDING_REFERENCE
 * 3. Loop de consumo da fila SQS wager-transactions.fifo
 */
export class BackgroundWorkersService implements OnModuleInit, OnModuleDestroy {
  private outboxTimer: ReturnType<typeof setInterval> | null = null;
  private pendingRefTimer: ReturnType<typeof setInterval> | null = null;
  private isRunningOutbox = false;
  private isRunningPendingRef = false;

  constructor(
    private readonly publishOutboxUseCase: PublishOutboxMessagesUseCase,
    private readonly retryPendingRefUseCase: RetryPendingReferencesUseCase,
    private readonly sqsService: SqsMessagingService,
    private readonly logger: LoggerPort,
  ) {}

  /**
   * Inicializa os timers dos workers em background e o consumidor SQS quando o módulo NestJS sobe.
   */
  onModuleInit(): void {
    if (process.env.DISABLE_BACKGROUND_WORKERS === 'true') {
      return;
    }

    const outboxIntervalMs = Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 250);
    const pendingRefIntervalMs = Number(
      process.env.PENDING_REF_POLL_INTERVAL_MS ?? 500,
    );

    this.outboxTimer = setInterval(async () => {
      if (this.isRunningOutbox) {
        return;
      }
      this.isRunningOutbox = true;
      try {
        await this.publishOutboxUseCase.executeBatch(50);
      } catch (err) {
        this.logger.warn('Outbox worker batch error', {
          error: err instanceof Error ? err.message : String(err),
        });
      } finally {
        this.isRunningOutbox = false;
      }
    }, outboxIntervalMs);

    this.pendingRefTimer = setInterval(async () => {
      if (this.isRunningPendingRef) {
        return;
      }
      this.isRunningPendingRef = true;
      try {
        await this.retryPendingRefUseCase.executeBatch(25);
      } catch (err) {
        this.logger.warn('PendingReference worker batch error', {
          error: err instanceof Error ? err.message : String(err),
        });
      } finally {
        this.isRunningPendingRef = false;
      }
    }, pendingRefIntervalMs);

    this.sqsService.startConsumerLoop(250);
  }

  /**
   * Interrompe todos os timers e aguarda a conclusão limpa das mensagens SQS em andamento durante o shutdown.
   */
  async onModuleDestroy(): Promise<void> {
    if (this.outboxTimer) {
      clearInterval(this.outboxTimer);
      this.outboxTimer = null;
    }
    if (this.pendingRefTimer) {
      clearInterval(this.pendingRefTimer);
      this.pendingRefTimer = null;
    }
    await this.sqsService.stopConsumerLoop();
  }
}
