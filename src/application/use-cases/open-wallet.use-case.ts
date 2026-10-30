import { randomUUID } from 'crypto';
import {
  WalletBalanceChanged,
  WagerTransactionProcessed,
} from '../../domain/messaging/integration-events';
import { OutboxMessage } from '../../domain/messaging/outbox-message.entity';
import { computeWagerPayloadHash } from '../../domain/shared/canonical-hash';
import {
  DuplicateWalletError,
  EntityNotFoundError,
} from '../../domain/shared/domain-errors';
import { Money, MoneyProps } from '../../domain/shared/money.vo';
import { WagerTransaction } from '../../domain/wagering/wager-transaction.entity';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../../domain/wallet/wallet-ledger-entry.entity';
import { Wallet } from '../../domain/wallet/wallet.aggregate';
import { LoggerPort, MetricsPort } from '../ports/observability.port';
import { LedgerPage, UnitOfWorkPort } from '../ports/unit-of-work.port';

export interface OpenWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
  correlationId?: string;
}

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

/**
 * Caso de uso responsável pela abertura de novas carteiras (Wallets) e pelas consultas
 * de saldo e extrato paginado do Ledger (Seção 9).
 */
export class OpenWalletUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {}

  /**
   * Abre uma nova Wallet para um jogador e moeda.
   * Quando o saldo inicial é maior que zero, cria atomicamente na mesma transação SQL:
   * 1. O registro da Wallet (version = 1)
   * 2. Uma transação interna do tipo OPENING (status = PROCESSED)
   * 3. O lançamento contábil correspondente no Ledger (direction = CREDIT)
   * 4. Os eventos WagerTransactionProcessed e WalletBalanceChanged na Outbox
   */
  async execute(command: OpenWalletCommand): Promise<WalletView> {
    const initialMoney = Money.from(command.initialBalance);
    const correlationId = command.correlationId ?? randomUUID();
    const now = new Date();

    return this.uow.runInTransaction(async (repos) => {
      const existing = await repos.wallets.findByPlayerAndCurrency(
        command.playerId,
        initialMoney.currency,
      );

      if (existing) {
        throw new DuplicateWalletError(command.playerId, initialMoney.currency);
      }

      const walletId = randomUUID();
      const wallet = Wallet.open({
        id: walletId,
        playerId: command.playerId,
        initialBalance: initialMoney,
        createdAt: now,
      });

      await repos.wallets.insert(wallet);

      // Seção 9: O saldo inicial, quando maior que zero, gera uma transação interna OPENING
      // na mesma transação SQL, com lançamento CREDIT correspondente no ledger.
      if (initialMoney.isPositive()) {
        const openingTxId = randomUUID();
        const payloadHash = computeWagerPayloadHash({
          providerId: 'internal-system',
          externalTransactionId: `opening:${walletId}`,
          playerId: command.playerId,
          walletId,
          roundId: `opening-round:${walletId}`,
          gameId: 'wallet-opening',
          kind: 'OPENING',
          money: initialMoney.toJSON(),
        });

        const openingTx = WagerTransaction.createOpening({
          id: openingTxId,
          walletId,
          playerId: command.playerId,
          money: initialMoney,
          payloadHash,
          createdAt: now,
        });

        const ledgerEntry = WalletLedgerEntry.create({
          id: randomUUID(),
          walletId,
          transactionId: openingTx.id,
          direction: LedgerDirection.Credit,
          money: initialMoney,
          balanceBefore: Money.zero(initialMoney.currency),
          balanceAfter: initialMoney,
          createdAt: now,
        });

        const processedEvent = WagerTransactionProcessed.from(
          openingTx,
          wallet.balance.toJSON(),
          { correlationId, occurredAt: now },
        );
        const balanceEvent = WalletBalanceChanged.from(wallet, ledgerEntry, {
          correlationId,
          occurredAt: now,
        });

        await repos.transactions.insert(openingTx);
        await repos.ledger.insert(ledgerEntry);
        await repos.outbox.insertMany([
          OutboxMessage.enqueue(processedEvent),
          OutboxMessage.enqueue(balanceEvent),
        ]);

        this.metrics.incrementTransactionStatus('OPENING', 'PROCESSED');
      }

      this.logger.info('Wallet opened successfully', {
        correlationId,
        walletId: wallet.id,
        providerId: 'internal-system',
      });

      return {
        id: wallet.id,
        playerId: wallet.playerId,
        balance: wallet.balance.toJSON(),
        version: wallet.version,
      };
    });
  }

  /**
   * Busca os dados consolidados de uma carteira pelo seu ID ou lança EntityNotFoundError.
   */
  async getWalletById(walletId: string): Promise<WalletView> {
    const wallet = await this.uow.getReadRepositories().wallets.findById(walletId);
    if (!wallet) {
      throw new EntityNotFoundError('Wallet', walletId);
    }
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }

  /**
   * Lista os lançamentos do Ledger de uma carteira utilizando paginação baseada em cursor opaco e estável.
   */
  async getWalletLedger(
    walletId: string,
    cursor?: string,
    limit: number = 50,
  ): Promise<LedgerPage> {
    const readRepos = this.uow.getReadRepositories();
    const wallet = await readRepos.wallets.findById(walletId);
    if (!wallet) {
      throw new EntityNotFoundError('Wallet', walletId);
    }
    const sanitizedLimit = Math.min(Math.max(1, limit), 200);
    return readRepos.ledger.findByWalletCursor(walletId, cursor, sanitizedLimit);
  }
}
