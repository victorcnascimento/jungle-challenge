import { MikroORM } from '@mikro-orm/postgresql';
import { Controller, Get, HttpStatus, Inject, Res } from '@nestjs/common';
import {
  METRICS_PORT,
  type MetricsPort,
} from '../../application/ports/observability.port';
import { SqsMessagingService } from '../../infrastructure/messaging/sqs.service';

/**
 * Endpoints públicos de Health Check (liveness / readiness) e Métricas Prometheus (Seções 9 e 12).
 * Não exigem autenticação (Seção 2).
 */
@Controller()
export class HealthController {
  constructor(
    @Inject(MikroORM)
    private readonly orm: MikroORM,
    @Inject(SqsMessagingService)
    private readonly sqsService: SqsMessagingService,
    @Inject(METRICS_PORT)
    private readonly metrics: MetricsPort,
  ) {}

  /**
   * Endpoint GET /health/live: verifica se o processo da aplicação está vivo e respondendo.
   */
  @Get('health/live')
  live() {
    return {
      status: 'ok',
      instanceId: process.env.INSTANCE_ID ?? `pid-${process.pid}`,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Endpoint GET /health/ready: verifica se o PostgreSQL e o AWS SQS estão alcançáveis.
   * Retorna 200 OK quando ambos estão saudáveis ou 503 Service Unavailable caso contrário.
   */
  @Get('health/ready')
  async ready(
    @Res()
    res: {
      status: (code: number) => { json: (payload: unknown) => void };
    },
  ) {
    let postgresUp = false;
    try {
      await this.orm.em.getConnection().execute('SELECT 1');
      postgresUp = true;
    } catch {
      postgresUp = false;
    }

    const sqsUp = await this.sqsService.isReachable();
    const isReady = postgresUp && sqsUp;

    res
      .status(isReady ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
      .json({
        status: isReady ? 'ready' : 'degraded',
        checks: {
          postgres: postgresUp ? 'up' : 'down',
          sqs: sqsUp ? 'up' : 'down',
        },
        timestamp: new Date().toISOString(),
      });
  }

  /**
   * Endpoint GET /metrics: expõe todas as métricas operacionais no formato texto do Prometheus.
   */
  @Get('metrics')
  async getMetrics(
    @Res()
    res: {
      setHeader: (name: string, value: string) => void;
      status: (code: number) => { send: (body: string) => void };
    },
  ) {
    const body = await this.metrics.getMetricsText();
    res.setHeader('Content-Type', this.metrics.getContentType());
    res.status(HttpStatus.OK).send(body);
  }
}
