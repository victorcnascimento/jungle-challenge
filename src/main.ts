import 'reflect-metadata';
import { MikroORM } from '@mikro-orm/postgresql';
import { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule, AppModuleOptions } from './app.module';
import { LOGGER_PORT, LoggerPort } from './application/ports/observability.port';
import { GlobalExceptionFilter } from './presentation/http/global-exception.filter';

/**
 * Fábrica que cria e configura uma instância da aplicação NestJS registrando o GlobalExceptionFilter
 * e ativando os hooks de encerramento gracioso (Shutdown Hooks).
 */
export async function createConfiguredApp(
  options: AppModuleOptions = {},
): Promise<INestApplication> {
  const app = await NestFactory.create(AppModule.register(options), {
    logger: false,
  });

  const logger = app.get<LoggerPort>(LOGGER_PORT);
  app.useGlobalFilters(new GlobalExceptionFilter(logger));
  app.enableShutdownHooks();

  return app;
}

/**
 * Função principal de inicialização (bootstrap) do servidor HTTP e registro dos handlers SIGTERM/SIGINT.
 */
async function bootstrap(): Promise<void> {
  const port = Number(process.env.PORT ?? 3000);
  const app = await createConfiguredApp();
  const logger = app.get<LoggerPort>(LOGGER_PORT);

  await app.listen(port, '0.0.0.0');

  logger.info('Distributed Wagering Processor listening', {
    port,
    instanceId: process.env.INSTANCE_ID ?? `pid-${process.pid}`,
  });

  const gracefulShutdown = async (signal: string) => {
    logger.info(`Received ${signal}; starting graceful shutdown...`);
    try {
      await app.close();
      const orm = app.get(MikroORM, { strict: false });
      if (orm) {
        await orm.close(true);
      }
      logger.info('Graceful shutdown completed.');
      process.exit(0);
    } catch (err) {
      logger.error('Error during graceful shutdown', {
        error: err instanceof Error ? err.message : String(err),
      });
      process.exit(1);
    }
  };

  process.once('SIGTERM', () => void gracefulShutdown('SIGTERM'));
  process.once('SIGINT', () => void gracefulShutdown('SIGINT'));
}

if (import.meta.main) {
  bootstrap().catch((err) => {
    console.error('Fatal bootstrap error:', err);
    process.exit(1);
  });
}
