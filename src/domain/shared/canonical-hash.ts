import { createHash } from 'crypto';
import { MoneyProps } from './money.vo';

export interface CanonicalWagerPayloadInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string | null;
}

/**
 * Ordena recursivamente as chaves de um objeto para produzir uma representação JSON canônica determinística.
 */
export function canonicalizeJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalizeJson(item)).join(',')}]`;
  }

  const record = value as Record<string, unknown>;
  const sortedKeys = Object.keys(record)
    .filter((key) => record[key] !== undefined)
    .sort();

  const entries = sortedKeys.map(
    (key) => `${JSON.stringify(key)}:${canonicalizeJson(record[key])}`,
  );

  return `{${entries.join(',')}}`;
}

/**
 * Calcula o hash SHA-256 hexadecimal sobre o JSON canônico de qualquer estrutura.
 */
export function sha256Canonical(value: unknown): string {
  const canonicalString = canonicalizeJson(value);
  return createHash('sha256').update(canonicalString, 'utf8').digest('hex');
}

/**
 * Calcula o payloadHash canônico exclusivamente sobre o subconjunto de campos de negócio da transação.
 * Headers HTTP, messageId da fila, timestamps de transporte e correlationId NÃO entram no hash (Seção 9).
 */
export function computeWagerPayloadHash(input: CanonicalWagerPayloadInput): string {
  const canonicalBusinessSubset = {
    externalTransactionId: input.externalTransactionId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: input.money.amount,
      currency: input.money.currency,
    },
    playerId: input.playerId,
    providerId: input.providerId,
    referenceExternalTransactionId: input.referenceExternalTransactionId ?? null,
    roundId: input.roundId,
    walletId: input.walletId,
  };

  return sha256Canonical(canonicalBusinessSubset);
}
