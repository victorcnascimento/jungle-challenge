import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ListQueuesCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import {
  EventPublisherPort,
  LoggerPort,
  MetricsPort,
} from '../../application/ports/observability.port';
import { ProcessWagerTransactionUseCase } from '../../application/use-cases/process-wager-transaction.use-case';
import { OutboxMessage } from '../../domain/messaging/outbox-message.entity';
import {
  DomainError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InvalidMoneyError,
  InvalidTransactionContractError,
} from '../../domain/shared/domain-errors';
import { WagerTransactionKind } from '../../domain/wagering/wager-transaction.entity';

export interface SqsWagerMessageEnvelope {
  messageId: string;
  type: 'WagerTransactionRequested';
  occurredAt: string;
  data: {
    providerId: string;
    externalTransactionId: string;
    idempotencyKey?: string;
    playerId: string;
    walletId: string;
    roundId: string;
    gameId: string;
    kind: WagerTransactionKind;
    money: { amount: string; currency: string };
    referenceExternalTransactionId?: string;
  };
}

/**
 * Adaptador AWS SQS para:
 * 1. Publicação de IntegrationEvents a partir do Transactional Outbox (EventPublisherPort).
 * 2. Consumo resiliente da fila wager-transactions.fifo com:
 *    - Deduplicação via InboxMessage (consumerName, messageId)
 *    - ACK somente após o commit da transação SQL
 *    - Separação entre erro de negócio (terminal -> ACK), erro transitório (retry com backoff) e erro permanente (DLQ)
 *    - Graceful Shutdown em SIGTERM (conclui mensagens em voo ou devolve a visibilidade)
 */
export class SqsMessagingService implements EventPublisherPort {
  public static readonly CONSUMER_NAME = 'sqs-wager-consumer';
  public static readonly MAX_RECEIVE_ATTEMPTS = 5;

  public readonly client: SQSClient;
  public readonly wagerQueueUrl: string;
  public readonly wagerDlqUrl: string;
  public readonly eventsQueueUrl: string;

  private isPolling = false;
  private isShuttingDown = false;
  private inFlightPromise: Promise<void> | null = null;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {
    const endpoint = process.env.AWS_ENDPOINT ?? 'http://localhost:4566';
    const region = process.env.AWS_REGION ?? 'us-east-1';

    this.client = new SQSClient({
      region,
      endpoint,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
    });

    this.wagerQueueUrl =
      process.env.SQS_WAGER_QUEUE_URL ??
      `${endpoint}/000000000000/wager-transactions.fifo`;
    this.wagerDlqUrl =
      process.env.SQS_WAGER_DLQ_URL ??
      `${endpoint}/000000000000/wager-transactions-dlq.fifo`;
    this.eventsQueueUrl =
      process.env.SQS_EVENTS_QUEUE_URL ??
      `${endpoint}/000000000000/domain-integration-events.fifo`;
  }

  /**
   * Publica um evento do Transactional Outbox para a fila SQS de eventos de integração.
   */
  async publishOutboxMessage(message: OutboxMessage): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.eventsQueueUrl,
        MessageBody: JSON.stringify(message.payload),
        MessageGroupId: message.aggregateId,
        MessageDeduplicationId: message.id,
      }),
    );
  }

  /**
   * Verifica conectividade com o SQS para o endpoint /health/ready.
   */
  async isReachable(): Promise<boolean> {
    try {
      await this.client.send(new ListQueuesCommand({ MaxResults: 1 }));
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Inicia o loop de consumo de mensagens da fila wager-transactions.fifo.
   */
  startConsumerLoop(pollIntervalMs: number = 250): void {
    if (this.isPolling || process.env.DISABLE_SQS_CONSUMER === 'true') {
      return;
    }
    this.isPolling = true;
    this.isShuttingDown = false;
    this.scheduleNextPoll(pollIntervalMs);
  }

  /**
   * Graceful shutdown em SIGTERM: interrompe novas leituras e aguarda a conclusão das mensagens em andamento.
   */
  async stopConsumerLoop(): Promise<void> {
    this.isShuttingDown = true;
    this.isPolling = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.inFlightPromise) {
      await this.inFlightPromise;
    }
  }

  /**
   * Processa um único lote de mensagens da fila SQS (também invocável diretamente em testes de integração/concorrência).
   */
  async pollAndProcessBatch(
    maxMessages: number = 10,
    waitTimeSeconds: number = 1,
  ): Promise<number> {
    const response = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.wagerQueueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        AttributeNames: ['All'],
        MessageAttributeNames: ['All'],
      }),
    );

    const messages = response.Messages ?? [];
    let handledCount = 0;

    for (const sqsMsg of messages) {
      if (this.isShuttingDown) {
        // Devolve visibilidade imediatamente caso o processo esteja desligando antes de iniciar esta mensagem
        if (sqsMsg.ReceiptHandle) {
          await this.releaseMessageVisibility(sqsMsg.ReceiptHandle, 0);
        }
        continue;
      }

      await this.handleSingleSqsMessage(sqsMsg);
      handledCount += 1;
    }

    return handledCount;
  }

  /**
   * Processa uma mensagem SQS individual garantindo:
   * - ACK somente após o commit
   * - Classificação entre erro de negócio, erro permanente (DLQ) e erro transitório (backoff / DLQ após limite)
   */
  public async handleSingleSqsMessage(
    sqsMsg: Message,
    options?: { simulateCrashBeforeAck?: boolean },
  ): Promise<void> {
    const receiptHandle = sqsMsg.ReceiptHandle;
    if (!receiptHandle) {
      return;
    }

    const receiveCount = Number(
      sqsMsg.Attributes?.ApproximateReceiveCount ?? '1',
    );

    let parsed: SqsWagerMessageEnvelope;
    try {
      parsed = JSON.parse(sqsMsg.Body ?? '{}') as SqsWagerMessageEnvelope;
      if (
        !parsed ||
        parsed.type !== 'WagerTransactionRequested' ||
        !parsed.data ||
        !parsed.messageId
      ) {
        throw new InvalidTransactionContractError(
          'Malformed SQS WagerTransactionRequested envelope.',
        );
      }
    } catch (parseErr) {
      // Erro permanente de contrato/JSON: encaminha para a DLQ e remove da fila principal
      await this.sendToDlq(sqsMsg.Body ?? '', 'malformed_envelope');
      await this.ackMessage(receiptHandle);
      return;
    }

    const messageId = parsed.messageId || sqsMsg.MessageId || 'unknown-msg';
    const data = parsed.data;
    const idempotencyKey =
      data.idempotencyKey ?? `${data.providerId}:${data.externalTransactionId}`;

    try {
      await this.processWagerUseCase.execute({
        idempotencyKey,
        providerId: data.providerId,
        externalTransactionId: data.externalTransactionId,
        playerId: data.playerId,
        walletId: data.walletId,
        roundId: data.roundId,
        gameId: data.gameId,
        kind: data.kind,
        money: data.money,
        referenceExternalTransactionId: data.referenceExternalTransactionId,
        correlationId: messageId,
        channel: 'sqs',
        inbox: {
          consumerName: SqsMessagingService.CONSUMER_NAME,
          messageId,
        },
      });

      // Hook para teste obrigatório de concorrência #5: "worker morto depois do commit e antes do ack"
      if (options?.simulateCrashBeforeAck) {
        await this.releaseMessageVisibility(receiptHandle, 0);
        throw new Error('SIMULATED_CRASH_AFTER_COMMIT_BEFORE_ACK');
      }

      // ACK somente após o commit da transação SQL!
      await this.ackMessage(receiptHandle);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message === 'SIMULATED_CRASH_AFTER_COMMIT_BEFORE_ACK'
      ) {
        throw error;
      }

      // 1. Conflito de Idempotência: estado terminal de negócio para esta mensagem -> loga e faz ACK
      if (error instanceof IdempotencyConflictError) {
        this.logger.warn('Idempotency conflict on SQS message; acknowledging', {
          messageId,
          walletId: data.walletId,
          providerId: data.providerId,
        });
        await this.ackMessage(receiptHandle);
        return;
      }

      // 2. Erro permanente (payload inválido, moeda inválida, wallet inexistente): envia para DLQ e faz ACK
      if (
        error instanceof InvalidMoneyError ||
        error instanceof InvalidTransactionContractError ||
        error instanceof EntityNotFoundError ||
        error instanceof DomainError
      ) {
        this.logger.error(
          'Permanent domain/contract error on SQS message; routing to DLQ',
          {
            messageId,
            walletId: data.walletId,
            providerId: data.providerId,
            errorCode: error.code,
          },
        );
        await this.sendToDlq(sqsMsg.Body ?? '', error.code);
        await this.ackMessage(receiptHandle);
        return;
      }

      // 3. Erro transitório de infraestrutura (ex.: banco indisponível, timeout de conexão)
      this.metrics.incrementRetry('sqs');
      if (receiveCount >= SqsMessagingService.MAX_RECEIVE_ATTEMPTS) {
        this.logger.error(
          'Transient error exceeded max receive attempts; routing to DLQ',
          {
            messageId,
            walletId: data.walletId,
            providerId: data.providerId,
            receiveCount,
          },
        );
        await this.sendToDlq(sqsMsg.Body ?? '', 'max_retries_exceeded');
        await this.ackMessage(receiptHandle);
        return;
      }

      const backoffSeconds = Math.min(Math.pow(2, receiveCount), 30);
      this.logger.warn(
        'Transient error processing SQS message; applying visibility backoff',
        {
          messageId,
          walletId: data.walletId,
          providerId: data.providerId,
          receiveCount,
          backoffSeconds,
        },
      );
      await this.releaseMessageVisibility(receiptHandle, backoffSeconds);
    }
  }

  private scheduleNextPoll(intervalMs: number): void {
    if (!this.isPolling || this.isShuttingDown) {
      return;
    }

    this.pollTimer = setTimeout(async () => {
      try {
        this.inFlightPromise = this.pollAndProcessBatch(10, 1).then(() => {});
        await this.inFlightPromise;
      } catch (err) {
        this.logger.warn('SQS consumer poll error', {
          error: err instanceof Error ? err.message : String(err),
        });
      } finally {
        this.inFlightPromise = null;
        this.scheduleNextPoll(intervalMs);
      }
    }, intervalMs);
  }

  private async ackMessage(receiptHandle: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: this.wagerQueueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  private async releaseMessageVisibility(
    receiptHandle: string,
    visibilityTimeoutSeconds: number,
  ): Promise<void> {
    try {
      await this.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: this.wagerQueueUrl,
          ReceiptHandle: receiptHandle,
          VisibilityTimeout: visibilityTimeoutSeconds,
        }),
      );
    } catch {
      // Ignora falha ao atualizar visibilidade durante shutdown
    }
  }

  private async sendToDlq(rawBody: string, reason: string): Promise<void> {
    this.metrics.incrementDlqMessage(reason);
    const dedupId = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.wagerDlqUrl,
        MessageBody: rawBody,
        MessageGroupId: 'dlq-group',
        MessageDeduplicationId: dedupId,
        MessageAttributes: {
          DlqReason: {
            DataType: 'String',
            StringValue: reason,
          },
        },
      }),
    );
  }

  async getApproximateQueueCount(queueUrl: string): Promise<number> {
    const res = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ['ApproximateNumberOfMessages'],
      }),
    );
    return Number(res.Attributes?.ApproximateNumberOfMessages ?? '0');
  }
}
