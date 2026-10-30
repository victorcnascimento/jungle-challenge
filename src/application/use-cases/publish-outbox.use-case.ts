import {
  EventPublisherPort,
  LoggerPort,
  MetricsPort,
} from '../ports/observability.port';
import { UnitOfWorkPort } from '../ports/unit-of-work.port';

/**
 * Worker de publicação do Transactional Outbox (Seção 11).
 * Múltiplas instâncias podem executar este UseCase concorrentemente com segurança
 * pois claimDueMessages utiliza SELECT ... FOR UPDATE SKIP LOCKED.
 */
export class PublishOutboxMessagesUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly eventPublisher: EventPublisherPort,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {}

  /**
   * Busca um lote de mensagens pendentes na Outbox, publica cada uma no broker de eventos,
   * marca como publicada (ou reagenda com backoff exponencial em caso de falha) e atualiza a métrica de lag.
   */
  async executeBatch(batchSize: number = 50): Promise<number> {
    const now = new Date();

    const publishedCount = await this.uow.runInTransaction(async (repos) => {
      const dueMessages = await repos.outbox.claimDueMessages(now, batchSize);
      let count = 0;

      for (const message of dueMessages) {
        try {
          await this.eventPublisher.publishOutboxMessage(message);
          message.markPublished(new Date());
          await repos.outbox.update(message);
          count += 1;
        } catch (error) {
          message.scheduleRetry(new Date());
          await repos.outbox.update(message);
          this.metrics.incrementRetry('outbox');
          this.logger.warn('Failed to publish outbox message; scheduled retry', {
            walletId: message.aggregateId,
            outboxId: message.id,
            eventType: message.eventType,
            attempts: message.attempts,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }

      const lagStats = await repos.outbox.getLagStats(new Date());
      this.metrics.setOutboxLag(lagStats.pendingCount, lagStats.maxLagSeconds);

      return count;
    });

    return publishedCount;
  }
}
