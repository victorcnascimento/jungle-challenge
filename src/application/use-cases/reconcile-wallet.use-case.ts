import { EntityNotFoundError } from '../../domain/shared/domain-errors';
import { Money, MoneyProps } from '../../domain/shared/money.vo';
import { LedgerDirection } from '../../domain/wallet/wallet-ledger-entry.entity';
import { LoggerPort, MetricsPort } from '../ports/observability.port';
import { UnitOfWorkPort } from '../ports/unit-of-work.port';

export interface ReconciliationResult {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

/**
 * Reconcilia o saldo materializado da Wallet contra a soma algébrica de todos os lançamentos do Ledger (Seção 9).
 * Divergências NUNCA são corrigidas silenciosamente: são logadas, contabilizadas em métrica e sinalizadas na resposta.
 */
export class ReconcileWalletUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {}

  /**
   * Percorre todos os lançamentos do Ledger da carteira em ordem cronológica, reconstrói o saldo
   * esperado e valida se coincide exatamente com o saldo armazenado na tabela wallets.
   */
  async execute(
    walletId: string,
    correlationId?: string,
  ): Promise<ReconciliationResult> {
    return this.uow.runInTransaction(async (repos) => {
      const wallet = await repos.wallets.findById(walletId);
      if (!wallet) {
        throw new EntityNotFoundError('Wallet', walletId);
      }

      const entries = await repos.ledger.findAllByWalletOrdered(walletId);

      let calculated = Money.zero(wallet.currency);
      let chainConsistent = true;

      for (const entry of entries) {
        if (!entry.isBalanced() || !entry.balanceBefore.equals(calculated)) {
          chainConsistent = false;
        }

        calculated =
          entry.direction === LedgerDirection.Credit
            ? calculated.add(entry.money)
            : calculated.subtract(entry.money);
      }

      const difference = wallet.balance.subtract(calculated);
      const consistent = difference.isZero() && chainConsistent;

      if (!consistent) {
        this.metrics.incrementReconciliationDivergence(walletId);
        this.logger.error('Wallet reconciliation divergence detected', {
          correlationId,
          walletId,
          consistent: false,
          checkedEntries: entries.length,
        });
      } else {
        this.logger.info('Wallet reconciliation verified consistent', {
          correlationId,
          walletId,
          checkedEntries: entries.length,
        });
      }

      return {
        walletId: wallet.id,
        storedBalance: wallet.balance.toJSON(),
        calculatedBalance: calculated.toJSON(),
        difference: difference.toJSON(),
        consistent,
        checkedEntries: entries.length,
      };
    });
  }
}
