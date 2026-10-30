import {
  LoggerPort,
  StructuredLogContext,
} from '../../application/ports/observability.port';

/**
 * Conjunto de chaves sensíveis e financeiras que são mascaradas recursivamente nos logs (Seção 12).
 * Garante que credenciais, tokens, PII e payloads/valores financeiros nunca vazem em stdout/stderr.
 */
const REDACTED_KEYS = new Set([
  'authorization',
  'password',
  'secret',
  'token',
  'accesstoken',
  'refreshtoken',
  'apikey',
  'cookie',
  'rawpayload',
  'payload',
  'body',
  'headers',
  'creditcard',
  'cpf',
  'email',
  'money',
  'amount',
  'balance',
  'initialbalance',
  'storedbalance',
  'calculatedbalance',
]);

/**
 * Logger estruturado em formato JSON (Seção 12).
 * Inclui correlationId, messageId, transactionId, walletId, providerId e mascara recursivamente
 * qualquer dado sensível ou payload financeiro.
 */
export class StructuredLoggerService implements LoggerPort {
  private readonly instanceId = process.env.INSTANCE_ID ?? `pid-${process.pid}`;
  private readonly silent = process.env.LOG_SILENT === 'true';

  info(message: string, context?: StructuredLogContext): void {
    this.write('INFO', message, context);
  }

  warn(message: string, context?: StructuredLogContext): void {
    this.write('WARN', message, context);
  }

  error(message: string, context?: StructuredLogContext): void {
    this.write('ERROR', message, context);
  }

  debug(message: string, context?: StructuredLogContext): void {
    if (process.env.LOG_LEVEL === 'debug') {
      this.write('DEBUG', message, context);
    }
  }

  private write(
    level: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG',
    message: string,
    context?: StructuredLogContext,
  ): void {
    if (this.silent) {
      return;
    }

    const sanitizedContext = this.sanitizeObject(context ?? {});
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      instanceId: this.instanceId,
      message,
      ...sanitizedContext,
    };

    const serialized = JSON.stringify(logEntry);
    if (level === 'ERROR') {
      process.stderr.write(serialized + '\n');
    } else {
      process.stdout.write(serialized + '\n');
    }
  }

  private sanitizeValue(key: string, value: unknown): unknown {
    const normalizedKey = key.toLowerCase().replace(/[-_]/g, '');
    if (REDACTED_KEYS.has(normalizedKey)) {
      return '[REDACTED]';
    }
    if (Array.isArray(value)) {
      return value.map((item) =>
        typeof item === 'object' && item !== null
          ? this.sanitizeObject(item as Record<string, unknown>)
          : item,
      );
    }
    if (typeof value === 'object' && value !== null) {
      return this.sanitizeObject(value as Record<string, unknown>);
    }
    return value;
  }

  private sanitizeObject(
    context: Record<string, unknown>,
  ): Record<string, unknown> {
    const output: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(context)) {
      if (value === undefined) {
        continue;
      }
      output[key] = this.sanitizeValue(key, value);
    }
    return output;
  }
}
