import { OutboxMessage } from '../../domain/messaging/outbox-message.entity';

export interface StructuredLogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  [key: string]: unknown;
}

export const LOGGER_PORT = Symbol('LOGGER_PORT');

export interface LoggerPort {
  info(message: string, context?: StructuredLogContext): void;
  warn(message: string, context?: StructuredLogContext): void;
  error(message: string, context?: StructuredLogContext): void;
  debug(message: string, context?: StructuredLogContext): void;
}

export const METRICS_PORT = Symbol('METRICS_PORT');

export interface MetricsPort {
  incrementTransactionStatus(
    kind: string,
    status: string,
    failureCode?: string,
  ): void;
  incrementDuplicateDetected(channel: 'http' | 'sqs' | 'inbox'): void;
  incrementRetry(worker: 'sqs' | 'outbox' | 'pending_reference'): void;
  incrementDlqMessage(reason: string): void;
  incrementLockConflict(): void;
  observeProcessingLatency(
    channel: 'http' | 'sqs' | 'retry_worker',
    kind: string,
    durationMs: number,
  ): void;
  setOutboxLag(pendingCount: number, maxLagSeconds: number): void;
  incrementReconciliationDivergence(walletId: string): void;
  getMetricsText(): Promise<string>;
  getContentType(): string;
}

export const EVENT_PUBLISHER_PORT = Symbol('EVENT_PUBLISHER_PORT');

export interface EventPublisherPort {
  publishOutboxMessage(message: OutboxMessage): Promise<void>;
}

export const PROVIDER_IDENTITY_PORT = Symbol('PROVIDER_IDENTITY_PORT');

export interface AuthenticatedProviderPrincipal {
  providerId: string;
  subject: string;
  scopes: string[];
}

/**
 * Porta de extensão explícita para validação de identidade de provedores via IdP externo (Keycloak / Zitadel OIDC).
 * Conforme Seção 2 do desafio.
 */
export interface ProviderIdentityPort {
  verifyAuthorizationHeader(
    authHeader?: string,
  ): Promise<AuthenticatedProviderPrincipal | null>;
}
