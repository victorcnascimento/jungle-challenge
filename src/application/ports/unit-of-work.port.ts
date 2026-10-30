import { InboxMessage } from '../../domain/messaging/inbox-message.entity';
import { OutboxMessage } from '../../domain/messaging/outbox-message.entity';
import {
  WagerTransaction,
  WagerTransactionKind,
} from '../../domain/wagering/wager-transaction.entity';
import { WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry.entity';
import { Wallet } from '../../domain/wallet/wallet.aggregate';

export interface LedgerPage {
  items: WalletLedgerEntry[];
  nextCursor: string | null;
}

export interface WalletRepositoryPort {
  findById(id: string): Promise<Wallet | null>;
  /** Adquire lock pessimista de linha (SELECT ... FOR UPDATE) sobre a walletId. */
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null>;
  insert(wallet: Wallet): Promise<void>;
  update(wallet: Wallet): Promise<void>;
}

export interface LedgerRepositoryPort {
  insert(entry: WalletLedgerEntry): Promise<void>;
  findByWalletCursor(
    walletId: string,
    cursor?: string,
    limit?: number,
  ): Promise<LedgerPage>;
  findAllByWalletOrdered(walletId: string): Promise<WalletLedgerEntry[]>;
}

export interface WagerTransactionRepositoryPort {
  findById(id: string): Promise<WagerTransaction | null>;
  findByIdForUpdate(id: string): Promise<WagerTransaction | null>;
  findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null>;
  findByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | null>;
  hasProcessedReversalFor(
    referenceTransactionId: string,
    kind: WagerTransactionKind,
  ): Promise<boolean>;
  claimDuePendingReferences(now: Date, limit: number): Promise<WagerTransaction[]>;
  insert(tx: WagerTransaction): Promise<void>;
  update(tx: WagerTransaction): Promise<void>;
}

export interface InboxRepositoryPort {
  findByKey(consumerName: string, messageId: string): Promise<InboxMessage | null>;
  insert(inbox: InboxMessage): Promise<void>;
  update(inbox: InboxMessage): Promise<void>;
}

export interface OutboxRepositoryPort {
  insertMany(messages: OutboxMessage[]): Promise<void>;
  /**
   * Seleciona mensagens vencidas na outbox usando FOR UPDATE SKIP LOCKED
   * para permitir múltiplos publishers concorrentes sem duplicação.
   */
  claimDueMessages(now: Date, limit: number): Promise<OutboxMessage[]>;
  update(message: OutboxMessage): Promise<void>;
  getLagStats(now: Date): Promise<{ pendingCount: number; maxLagSeconds: number }>;
}

export interface TransactionalRepositories {
  wallets: WalletRepositoryPort;
  ledger: LedgerRepositoryPort;
  transactions: WagerTransactionRepositoryPort;
  inbox: InboxRepositoryPort;
  outbox: OutboxRepositoryPort;
}

export const UNIT_OF_WORK_PORT = Symbol('UNIT_OF_WORK_PORT');

export interface UnitOfWorkPort {
  runInTransaction<T>(
    work: (repos: TransactionalRepositories) => Promise<T>,
  ): Promise<T>;
  getReadRepositories(): TransactionalRepositories;
}
