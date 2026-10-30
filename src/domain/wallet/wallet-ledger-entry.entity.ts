import { ImbalancedLedgerEntryError, InvalidMoneyError } from '../shared/domain-errors';
import { Money } from '../shared/money.vo';

/**
 * Direção contábil do lançamento no ledger da carteira.
 */
export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt?: Date;
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

/**
 * Entidade de domínio estritamente imutável (append-only).
 * Não possui campos mutáveis nem métodos de transição (Seção 6.4).
 */
export class WalletLedgerEntry {
  /**
   * Construtor privado que congela a instância (Object.freeze) para impedir mutações em runtime.
   */
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  /**
   * Cria um novo lançamento contábil validando que o valor movimentado é positivo,
   * que os saldos antes/depois não são negativos e que a aritmética fecha (isBalanced).
   */
  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.money.isPositive()) {
      throw new InvalidMoneyError(
        `Ledger entry money must be strictly positive (> 0.00). Got ${props.money.toString()}.`,
      );
    }

    if (props.balanceBefore.isNegative() || props.balanceAfter.isNegative()) {
      throw new ImbalancedLedgerEntryError(
        'Ledger entry balanceBefore and balanceAfter cannot be negative.',
      );
    }

    const entry = new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt ?? new Date(),
    );

    if (!entry.isBalanced()) {
      throw new ImbalancedLedgerEntryError(
        `Unbalanced ledger entry for transaction ${props.transactionId}: ` +
          `balanceBefore (${props.balanceBefore.toString()}) ${props.direction} ` +
          `money (${props.money.toString()}) !== balanceAfter (${props.balanceAfter.toString()}).`,
      );
    }

    return entry;
  }

  /**
   * Reidratação a partir do banco de dados — reconstrói o estado já persistido sem revalidar transições.
   */
  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  /**
   * Verifica a equação fundamental do ledger: balanceBefore ± money === balanceAfter.
   */
  isBalanced(): boolean {
    if (
      this.money.currency !== this.balanceBefore.currency ||
      this.money.currency !== this.balanceAfter.currency
    ) {
      return false;
    }

    const expectedAfter =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);

    return expectedAfter.equals(this.balanceAfter);
  }
}
