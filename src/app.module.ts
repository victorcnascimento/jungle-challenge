import { MikroORM } from '@mikro-orm/postgresql';
import { DynamicModule, Module } from '@nestjs/common';
import {
  EVENT_PUBLISHER_PORT,
  type EventPublisherPort,
  LOGGER_PORT,
  type LoggerPort,
  METRICS_PORT,
  type MetricsPort,
  PROVIDER_IDENTITY_PORT,
  type ProviderIdentityPort,
} from './application/ports/observability.port';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from './application/ports/unit-of-work.port';
import { OpenWalletUseCase } from './application/use-cases/open-wallet.use-case';
import { ProcessWagerTransactionUseCase } from './application/use-cases/process-wager-transaction.use-case';
import { PublishOutboxMessagesUseCase } from './application/use-cases/publish-outbox.use-case';
import { ReconcileWalletUseCase } from './application/use-cases/reconcile-wallet.use-case';
import { RetryPendingReferencesUseCase } from './application/use-cases/retry-pending-references.use-case';
import {
  ExternalIdpProviderIdentityAdapter,
  ProviderAuthGuard,
} from './infrastructure/auth/provider-auth.guard';
import { createMikroOrmOptions } from './infrastructure/database/mikro-orm.config';
import { MikroOrmUnitOfWork } from './infrastructure/database/mikro-orm.unit-of-work';
import { BackgroundWorkersService } from './infrastructure/messaging/background-workers.service';
import { SqsMessagingService } from './infrastructure/messaging/sqs.service';
import { PrometheusMetricsService } from './infrastructure/observability/prometheus-metrics.service';
import { StructuredLoggerService } from './infrastructure/observability/structured-logger.service';
import { HealthController } from './presentation/http/health.controller';
import { WageringController } from './presentation/http/wagering.controller';
import { WalletsController } from './presentation/http/wallets.controller';

export interface AppModuleOptions {
  orm?: MikroORM;
  uow?: UnitOfWorkPort;
  eventPublisher?: EventPublisherPort;
}

@Module({})
export class AppModule {
  static register(options: AppModuleOptions = {}): DynamicModule {
    return {
      module: AppModule,
      controllers: [WalletsController, WageringController, HealthController],
      providers: [
        {
          provide: LOGGER_PORT,
          useFactory: () => new StructuredLoggerService(),
        },
        {
          provide: METRICS_PORT,
          useFactory: () => new PrometheusMetricsService(),
        },
        {
          provide: PROVIDER_IDENTITY_PORT,
          useFactory: () => new ExternalIdpProviderIdentityAdapter(),
        },
        {
          provide: ProviderAuthGuard,
          useFactory: (identityPort: ProviderIdentityPort) =>
            new ProviderAuthGuard(identityPort),
          inject: [PROVIDER_IDENTITY_PORT],
        },
        {
          provide: MikroORM,
          useFactory: async () => {
            if (options.orm) {
              return options.orm;
            }
            const orm = await MikroORM.init(createMikroOrmOptions());
            if (process.env.AUTO_MIGRATE === 'true') {
              await orm.getMigrator().up();
            }
            return orm;
          },
        },
        {
          provide: UNIT_OF_WORK_PORT,
          useFactory: (orm: MikroORM, metrics: MetricsPort) => {
            if (options.uow) {
              return options.uow;
            }
            return new MikroOrmUnitOfWork(orm, metrics);
          },
          inject: [MikroORM, METRICS_PORT],
        },
        {
          provide: OpenWalletUseCase,
          useFactory: (
            uow: UnitOfWorkPort,
            logger: LoggerPort,
            metrics: MetricsPort,
          ) => new OpenWalletUseCase(uow, logger, metrics),
          inject: [UNIT_OF_WORK_PORT, LOGGER_PORT, METRICS_PORT],
        },
        {
          provide: ProcessWagerTransactionUseCase,
          useFactory: (
            uow: UnitOfWorkPort,
            logger: LoggerPort,
            metrics: MetricsPort,
          ) => new ProcessWagerTransactionUseCase(uow, logger, metrics),
          inject: [UNIT_OF_WORK_PORT, LOGGER_PORT, METRICS_PORT],
        },
        {
          provide: ReconcileWalletUseCase,
          useFactory: (
            uow: UnitOfWorkPort,
            logger: LoggerPort,
            metrics: MetricsPort,
          ) => new ReconcileWalletUseCase(uow, logger, metrics),
          inject: [UNIT_OF_WORK_PORT, LOGGER_PORT, METRICS_PORT],
        },
        {
          provide: RetryPendingReferencesUseCase,
          useFactory: (
            uow: UnitOfWorkPort,
            processWager: ProcessWagerTransactionUseCase,
            logger: LoggerPort,
            metrics: MetricsPort,
          ) =>
            new RetryPendingReferencesUseCase(
              uow,
              processWager,
              logger,
              metrics,
            ),
          inject: [
            UNIT_OF_WORK_PORT,
            ProcessWagerTransactionUseCase,
            LOGGER_PORT,
            METRICS_PORT,
          ],
        },
        {
          provide: SqsMessagingService,
          useFactory: (
            processWager: ProcessWagerTransactionUseCase,
            logger: LoggerPort,
            metrics: MetricsPort,
          ) => new SqsMessagingService(processWager, logger, metrics),
          inject: [ProcessWagerTransactionUseCase, LOGGER_PORT, METRICS_PORT],
        },
        {
          provide: EVENT_PUBLISHER_PORT,
          useFactory: (sqsService: SqsMessagingService) =>
            options.eventPublisher ?? sqsService,
          inject: [SqsMessagingService],
        },
        {
          provide: PublishOutboxMessagesUseCase,
          useFactory: (
            uow: UnitOfWorkPort,
            publisher: EventPublisherPort,
            logger: LoggerPort,
            metrics: MetricsPort,
          ) => new PublishOutboxMessagesUseCase(uow, publisher, logger, metrics),
          inject: [
            UNIT_OF_WORK_PORT,
            EVENT_PUBLISHER_PORT,
            LOGGER_PORT,
            METRICS_PORT,
          ],
        },
        {
          provide: BackgroundWorkersService,
          useFactory: (
            publishOutbox: PublishOutboxMessagesUseCase,
            retryPendingRef: RetryPendingReferencesUseCase,
            sqsService: SqsMessagingService,
            logger: LoggerPort,
          ) =>
            new BackgroundWorkersService(
              publishOutbox,
              retryPendingRef,
              sqsService,
              logger,
            ),
          inject: [
            PublishOutboxMessagesUseCase,
            RetryPendingReferencesUseCase,
            SqsMessagingService,
            LOGGER_PORT,
          ],
        },
      ],
      exports: [
        LOGGER_PORT,
        METRICS_PORT,
        UNIT_OF_WORK_PORT,
        OpenWalletUseCase,
        ProcessWagerTransactionUseCase,
        ReconcileWalletUseCase,
        RetryPendingReferencesUseCase,
        PublishOutboxMessagesUseCase,
        SqsMessagingService,
      ],
    };
  }
}
