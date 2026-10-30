import { EntitySchema } from '@mikro-orm/core';

export class WalletOrmEntity {
  id!: string;
  playerId!: string;
  currency!: string;
  balanceAmount!: string;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;
}

export const WalletSchema = new EntitySchema<WalletOrmEntity>({
  class: WalletOrmEntity,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'string', fieldName: 'player_id', length: 128 },
    currency: { type: 'string', length: 3 },
    balanceAmount: {
      type: 'numeric',
      fieldName: 'balance_amount',
      precision: 18,
      scale: 2,
    },
    version: { type: 'integer', default: 1 },
    createdAt: { type: 'Date', fieldName: 'created_at' },
    updatedAt: { type: 'Date', fieldName: 'updated_at' },
  },
});

export class WagerTransactionOrmEntity {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  amount!: string;
  currency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  observedBalanceAmount?: string | null;
  retryAttempts!: number;
  nextRetryAt?: Date | null;
  createdAt!: Date;
  processedAt?: Date | null;
}

export const WagerTransactionSchema = new EntitySchema<WagerTransactionOrmEntity>({
  class: WagerTransactionOrmEntity,
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: 'string', fieldName: 'provider_id', length: 128 },
    externalTransactionId: {
      type: 'string',
      fieldName: 'external_transaction_id',
      length: 255,
    },
    idempotencyKey: {
      type: 'string',
      fieldName: 'idempotency_key',
      length: 255,
    },
    payloadHash: { type: 'string', fieldName: 'payload_hash', length: 64 },
    walletId: { type: 'uuid', fieldName: 'wallet_id' },
    playerId: { type: 'string', fieldName: 'player_id', length: 128 },
    roundId: { type: 'string', fieldName: 'round_id', length: 128 },
    gameId: { type: 'string', fieldName: 'game_id', length: 128 },
    kind: { type: 'string', length: 32 },
    amount: { type: 'numeric', precision: 18, scale: 2 },
    currency: { type: 'string', length: 3 },
    referenceExternalTransactionId: {
      type: 'string',
      fieldName: 'reference_external_transaction_id',
      length: 255,
      nullable: true,
    },
    referenceTransactionId: {
      type: 'uuid',
      fieldName: 'reference_transaction_id',
      nullable: true,
    },
    status: { type: 'string', length: 32 },
    failureCode: {
      type: 'string',
      fieldName: 'failure_code',
      length: 64,
      nullable: true,
    },
    observedBalanceAmount: {
      type: 'numeric',
      fieldName: 'observed_balance_amount',
      precision: 18,
      scale: 2,
      nullable: true,
    },
    retryAttempts: {
      type: 'integer',
      fieldName: 'retry_attempts',
      default: 0,
    },
    nextRetryAt: {
      type: 'Date',
      fieldName: 'next_retry_at',
      nullable: true,
    },
    createdAt: { type: 'Date', fieldName: 'created_at' },
    processedAt: { type: 'Date', fieldName: 'processed_at', nullable: true },
  },
});

export class WalletLedgerEntryOrmEntity {
  id!: string;
  sequenceNumber?: string;
  walletId!: string;
  transactionId!: string;
  direction!: string;
  amount!: string;
  currency!: string;
  balanceBefore!: string;
  balanceAfter!: string;
  createdAt!: Date;
}

export const WalletLedgerEntrySchema =
  new EntitySchema<WalletLedgerEntryOrmEntity>({
    class: WalletLedgerEntryOrmEntity,
    tableName: 'wallet_ledger_entries',
    properties: {
      id: { type: 'uuid', primary: true },
      sequenceNumber: {
        type: 'bigint',
        fieldName: 'sequence_number',
        autoincrement: true,
        nullable: true,
      },
      walletId: { type: 'uuid', fieldName: 'wallet_id' },
      transactionId: { type: 'uuid', fieldName: 'transaction_id' },
      direction: { type: 'string', length: 16 },
      amount: { type: 'numeric', precision: 18, scale: 2 },
      currency: { type: 'string', length: 3 },
      balanceBefore: {
        type: 'numeric',
        fieldName: 'balance_before',
        precision: 18,
        scale: 2,
      },
      balanceAfter: {
        type: 'numeric',
        fieldName: 'balance_after',
        precision: 18,
        scale: 2,
      },
      createdAt: { type: 'Date', fieldName: 'created_at' },
    },
  });

export class InboxMessageOrmEntity {
  consumerName!: string;
  messageId!: string;
  payloadHash!: string;
  receivedAt!: Date;
  processedAt?: Date | null;
}

export const InboxMessageSchema = new EntitySchema<InboxMessageOrmEntity>({
  class: InboxMessageOrmEntity,
  tableName: 'inbox_messages',
  properties: {
    consumerName: {
      type: 'string',
      fieldName: 'consumer_name',
      length: 128,
      primary: true,
    },
    messageId: {
      type: 'string',
      fieldName: 'message_id',
      length: 255,
      primary: true,
    },
    payloadHash: { type: 'string', fieldName: 'payload_hash', length: 64 },
    receivedAt: { type: 'Date', fieldName: 'received_at' },
    processedAt: { type: 'Date', fieldName: 'processed_at', nullable: true },
  },
});

export class OutboxMessageOrmEntity {
  id!: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt?: Date | null;
  publishedAt?: Date | null;
}

export const OutboxMessageSchema = new EntitySchema<OutboxMessageOrmEntity>({
  class: OutboxMessageOrmEntity,
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    aggregateId: { type: 'string', fieldName: 'aggregate_id', length: 128 },
    eventType: { type: 'string', fieldName: 'event_type', length: 128 },
    payload: { type: 'json' },
    occurredAt: { type: 'Date', fieldName: 'occurred_at' },
    attempts: { type: 'integer', default: 0 },
    nextAttemptAt: {
      type: 'Date',
      fieldName: 'next_attempt_at',
      nullable: true,
    },
    publishedAt: { type: 'Date', fieldName: 'published_at', nullable: true },
  },
});
