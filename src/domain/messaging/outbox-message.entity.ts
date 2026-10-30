import { randomUUID } from 'crypto';
import { IntegrationEvent } from './integration-events';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

/**
 * Entidade do padrão Transactional Outbox (Seção 6.5 e Seção 11).
 * Garante que eventos de integração só sejam publicados após o commit da transação SQL.
 */
export class OutboxMessage {
  public static readonly BASE_BACKOFF_MS = 1000;
  public static readonly MAX_BACKOFF_MS = 60000;

  /**
   * Construtor privado que encapsula o estado de publicação e retentativas do evento na Outbox.
   */
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  /**
   * Enfileira um IntegrationEvent na Outbox para ser persistido na mesma transação SQL da mudança de estado.
   */
  static enqueue(event: IntegrationEvent<unknown>, id?: string): OutboxMessage {
    const serialized = event.toJSON() as unknown as Readonly<Record<string, unknown>>;
    return new OutboxMessage(
      id ?? event.eventId ?? randomUUID(),
      event.aggregateId,
      event.eventType,
      Object.freeze({ ...serialized }),
      event.occurredAt,
      0,
      event.occurredAt,
      undefined,
    );
  }

  /**
   * Reidrata um registro da tabela outbox_messages vindo do banco de dados.
   */
  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      Object.freeze({ ...state.payload }),
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
    );
  }

  /** Retorna o número de tentativas de publicação já realizadas para este evento. */
  get attempts(): number {
    return this._attempts;
  }

  /** Retorna a data/hora agendada para a próxima tentativa de envio ao broker. */
  get nextAttemptAt(): Date | undefined {
    return this._nextRetryOrAttemptAt;
  }

  private get _nextRetryOrAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  /** Retorna a data/hora em que o evento foi publicado com sucesso no broker. */
  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  /** Verifica se a mensagem ainda aguarda publicação (publishedAt nulo/indefinido). */
  isPending(): boolean {
    return this._publishedAt === undefined || this._publishedAt === null;
  }

  /** Verifica se a mensagem está pendente e já atingiu o horário agendado para publicação. */
  isDue(now: Date): boolean {
    if (!this.isPending()) {
      return false;
    }
    if (!this._nextAttemptAt) {
      return true;
    }
    return this._nextAttemptAt.getTime() <= now.getTime();
  }

  /** Marca a mensagem da Outbox como publicada com sucesso no broker. */
  markPublished(at: Date): void {
    this._publishedAt = at;
    this._nextAttemptAt = undefined;
  }

  /**
   * Incrementa attempts e calcula o próximo nextAttemptAt usando backoff exponencial limitado a 60s.
   */
  scheduleRetry(now: Date): void {
    this._attempts += 1;
    const exponentialDelay = Math.min(
      OutboxMessage.BASE_BACKOFF_MS * Math.pow(2, this._attempts - 1),
      OutboxMessage.MAX_BACKOFF_MS,
    );
    this._nextAttemptAt = new Date(now.getTime() + exponentialDelay);
  }
}
