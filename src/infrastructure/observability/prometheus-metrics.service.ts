import { Counter, Gauge, Histogram, Registry } from 'prom-client';
import { MetricsPort } from '../../application/ports/observability.port';

/**
 * Serviço de métricas Prometheus cobrindo todos os indicadores exigidos na Seção 12:
 * - transações por status e kind
 * - duplicatas detectadas
 * - retries
 * - mensagens em DLQ
 * - conflitos de lock
 * - outbox lag (quantidade pendente e atraso máximo em segundos)
 * - latência de processamento
 * - divergências de reconciliação
 */
export class PrometheusMetricsService implements MetricsPort {
  public readonly registry: Registry;

  private readonly transactionsTotal: Counter<'kind' | 'status' | 'failure_code'>;
  private readonly duplicatesTotal: Counter<'channel'>;
  private readonly retriesTotal: Counter<'worker'>;
  private readonly dlqMessagesTotal: Counter<'reason'>;
  private readonly lockConflictsTotal: Counter;
  private readonly processingLatencyMs: Histogram<'channel' | 'kind'>;
  private readonly outboxPendingGauge: Gauge;
  private readonly outboxMaxLagSecondsGauge: Gauge;
  private readonly reconciliationDivergenceTotal: Counter;

  /**
   * Inicializa um Registry isolado do Prometheus e registra todos os contadores, gauges e histogramas.
   */
  constructor() {
    this.registry = new Registry();

    this.transactionsTotal = new Counter({
      name: 'wagering_transactions_total',
      help: 'Total number of wagering transactions processed by kind, status and failure_code',
      labelNames: ['kind', 'status', 'failure_code'],
      registers: [this.registry],
    });

    this.duplicatesTotal = new Counter({
      name: 'wagering_duplicates_detected_total',
      help: 'Total number of duplicate requests or messages detected by channel',
      labelNames: ['channel'],
      registers: [this.registry],
    });

    this.retriesTotal = new Counter({
      name: 'wagering_retries_total',
      help: 'Total number of retries scheduled by worker type',
      labelNames: ['worker'],
      registers: [this.registry],
    });

    this.dlqMessagesTotal = new Counter({
      name: 'wagering_dlq_messages_total',
      help: 'Total number of messages routed to Dead Letter Queue (DLQ)',
      labelNames: ['reason'],
      registers: [this.registry],
    });

    this.lockConflictsTotal = new Counter({
      name: 'wagering_lock_conflicts_total',
      help: 'Total number of database lock or serialization conflicts',
      registers: [this.registry],
    });

    this.processingLatencyMs = new Histogram({
      name: 'wagering_processing_duration_ms',
      help: 'Transaction processing latency in milliseconds',
      labelNames: ['channel', 'kind'],
      buckets: [1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500],
      registers: [this.registry],
    });

    this.outboxPendingGauge = new Gauge({
      name: 'wagering_outbox_pending_messages',
      help: 'Current number of unpublished messages in the transactional outbox',
      registers: [this.registry],
    });

    this.outboxMaxLagSecondsGauge = new Gauge({
      name: 'wagering_outbox_max_lag_seconds',
      help: 'Age in seconds of the oldest unpublished message in the transactional outbox',
      registers: [this.registry],
    });

    this.reconciliationDivergenceTotal = new Counter({
      name: 'wagering_reconciliation_divergences_total',
      help: 'Total number of wallet balance vs ledger divergences detected during reconciliation',
      registers: [this.registry],
    });
  }

  /** Incrementa o contador de transações finalizadas agrupado por tipo, status e código de falha. */
  incrementTransactionStatus(
    kind: string,
    status: string,
    failureCode: string = 'NONE',
  ): void {
    this.transactionsTotal.inc({
      kind,
      status,
      failure_code: failureCode,
    });
  }

  /** Incrementa o contador de requisições/mensagens duplicadas detectadas por canal (http, sqs, inbox). */
  incrementDuplicateDetected(channel: 'http' | 'sqs' | 'inbox'): void {
    this.duplicatesTotal.inc({ channel });
  }

  /** Incrementa o contador de retentativas agendadas por worker (sqs, outbox, pending_reference). */
  incrementRetry(worker: 'sqs' | 'outbox' | 'pending_reference'): void {
    this.retriesTotal.inc({ worker });
  }

  /** Incrementa o contador de mensagens encaminhadas para a Dead Letter Queue (DLQ) pelo motivo informado. */
  incrementDlqMessage(reason: string): void {
    this.dlqMessagesTotal.inc({ reason });
  }

  /** Incrementa o contador de conflitos de lock ou deadlocks detectados no PostgreSQL. */
  incrementLockConflict(): void {
    this.lockConflictsTotal.inc();
  }

  /** Registra a duração em milissegundos do processamento de uma transação no histograma de latência. */
  observeProcessingLatency(
    channel: 'http' | 'sqs' | 'retry_worker',
    kind: string,
    durationMs: number,
  ): void {
    this.processingLatencyMs.observe({ channel, kind }, durationMs);
  }

  /** Atualiza os gauges de quantidade de mensagens pendentes e atraso máximo (lag) na Outbox. */
  setOutboxLag(pendingCount: number, maxLagSeconds: number): void {
    this.outboxPendingGauge.set(pendingCount);
    this.outboxMaxLagSecondsGauge.set(maxLagSeconds);
  }

  /** Incrementa o contador global de divergências contábeis detectadas durante reconciliações. */
  incrementReconciliationDivergence(_walletId: string): void {
    this.reconciliationDivergenceTotal.inc();
  }

  /** Serializa todas as métricas registradas no formato texto padrão do Prometheus. */
  async getMetricsText(): Promise<string> {
    return this.registry.metrics();
  }

  /** Retorna o cabeçalho Content-Type esperado pelo scraper do Prometheus. */
  getContentType(): string {
    return this.registry.contentType;
  }
}
