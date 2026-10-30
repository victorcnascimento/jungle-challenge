import { InvalidTransactionStateError } from '../shared/domain-errors';

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt?: Date;
}

export interface InboxMessageState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date;
}

/**
 * Entidade de deduplicação persistente para consumidores de mensageria (Transactional Inbox Pattern - Seção 6.5).
 */
export class InboxMessage {
  /**
   * Construtor privado para encapsular o registro de recebimento de mensagem da fila.
   */
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  /**
   * Fábrica estática que registra a chegada inicial de uma mensagem para um consumidor específico.
   */
  static receive(props: ReceiveInboxProps): InboxMessage {
    return new InboxMessage(
      props.messageId,
      props.consumerName,
      props.payloadHash,
      props.receivedAt ?? new Date(),
      undefined,
    );
  }

  /**
   * Reconstrói o estado da mensagem da Inbox a partir do banco de dados sem revalidar transições.
   */
  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  /** Retorna a data/hora em que a mensagem foi efetivamente processada na transação SQL. */
  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  /** Verifica se a mensagem já foi marcada como processada anteriormente. */
  isProcessed(): boolean {
    return this._processedAt !== undefined && this._processedAt !== null;
  }

  /**
   * Marca a mensagem da Inbox como processada. Lança erro se já estiver processada.
   */
  markProcessed(at: Date): void {
    if (this.isProcessed()) {
      throw new InvalidTransactionStateError('PROCESSED', 'PROCESSED');
    }
    this._processedAt = at;
  }
}
