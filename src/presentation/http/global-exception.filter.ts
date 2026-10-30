import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import type { LoggerPort } from '../../application/ports/observability.port';
import {
  CurrencyMismatchError,
  DomainError,
  DuplicateWalletError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InsufficientFundsError,
  InvalidMoneyError,
  InvalidTransactionContractError,
  InvalidTransactionStateError,
} from '../../domain/shared/domain-errors';

/**
 * Filtro global que garante distinção semântica clara nos códigos HTTP (Seção 9):
 * - 400 Bad Request: Payload inválido, escala de Money inválida, ausência de Idempotency-Key
 * - 404 Not Found: Recurso (Wallet / WagerTransaction) não encontrado
 * - 409 Conflict: Conflito de Idempotency-Key (mesma chave com payload diferente) ou Wallet duplicada
 * - 422 Unprocessable Entity: Rejeição por regra de domínio / estado inválido
 * - 503 Service Unavailable: Falha transitória de infraestrutura (PostgreSQL / SQS indisponível, lock timeout)
 */
@Catch()
export class GlobalExceptionFilter implements ExceptionFilter {
  constructor(private readonly logger: LoggerPort) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<{
      status: (code: number) => { json: (body: unknown) => void };
    }>();
    const request = ctx.getRequest<{
      url?: string;
      method?: string;
      headers?: Record<string, string | undefined>;
    }>();

    const correlationId =
      request.headers?.['x-correlation-id'] ??
      request.headers?.['idempotency-key'];

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      const resBody = exception.getResponse();
      response.status(status).json(
        typeof resBody === 'object' && resBody !== null
          ? { ...resBody, correlationId }
          : { code: 'HTTP_ERROR', message: String(resBody), correlationId },
      );
      return;
    }

    if (
      exception instanceof InvalidMoneyError ||
      exception instanceof InvalidTransactionContractError
    ) {
      response.status(HttpStatus.BAD_REQUEST).json({
        code: exception.code,
        message: exception.message,
        correlationId,
      });
      return;
    }

    if (exception instanceof EntityNotFoundError) {
      response.status(HttpStatus.NOT_FOUND).json({
        code: exception.code,
        message: exception.message,
        correlationId,
      });
      return;
    }

    if (
      exception instanceof IdempotencyConflictError ||
      exception instanceof DuplicateWalletError
    ) {
      response.status(HttpStatus.CONFLICT).json({
        code: exception.code,
        message: exception.message,
        correlationId,
      });
      return;
    }

    if (
      exception instanceof InsufficientFundsError ||
      exception instanceof CurrencyMismatchError ||
      exception instanceof InvalidTransactionStateError ||
      exception instanceof DomainError
    ) {
      response.status(HttpStatus.UNPROCESSABLE_ENTITY).json({
        code: exception.code,
        message: exception.message,
        correlationId,
      });
      return;
    }

    // Erro 22P02 do PostgreSQL (invalid_text_representation, ex.: UUID malformado) -> 400 Bad Request
    const pgCode = (exception as { code?: string })?.code;
    if (pgCode === '22P02') {
      response.status(HttpStatus.BAD_REQUEST).json({
        code: 'INVALID_IDENTIFIER_FORMAT',
        message: 'Invalid UUID identifier format provided in request.',
        correlationId,
      });
      return;
    }

    // Erros transitórios de banco/rede -> 503 Service Unavailable para sinalizar retry seguro ao provedor
    const isTransientDbError =
      pgCode === '40P01' ||
      pgCode === '55P03' ||
      pgCode === '40001' ||
      pgCode === '57P01' ||
      pgCode === '08006' ||
      pgCode === 'ECONNREFUSED';

    const status = isTransientDbError
      ? HttpStatus.SERVICE_UNAVAILABLE
      : HttpStatus.INTERNAL_SERVER_ERROR;

    this.logger.error('Unhandled exception in HTTP layer', {
      correlationId,
      path: request.url,
      method: request.method,
      errorCode: pgCode ?? 'INTERNAL_ERROR',
      error: exception instanceof Error ? exception.message : String(exception),
    });

    response.status(status).json({
      code: isTransientDbError
        ? 'TRANSIENT_INFRASTRUCTURE_ERROR'
        : 'INTERNAL_SERVER_ERROR',
      message: isTransientDbError
        ? 'Temporary infrastructure contention or unavailability; safe to retry with same Idempotency-Key.'
        : 'An unexpected internal error occurred.',
      correlationId,
    });
  }
}
