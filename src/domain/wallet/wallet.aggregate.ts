import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidMoneyError,
} from '../shared/domain-errors';
import { Money } from '../shared/money.vo';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry.entity';

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  createdAt?: Date;
}

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovementParams {
  entryId: string;
  transactionId: string;
  money: Money;
  occurredAt?: Date;
}

/**
 * Aggregate Root que encapsula o saldo, a versão e a geração obrigatória de entradas no ledger (Seção 6.2).
 * Invariantes garantidas:
 * - Saldo nunca fica negativo.
 * - Toda alteração de saldo produz um WalletLedgerEntry correspondente e balanceado.
 * - Moeda da movimentação deve ser idêntica à moeda da Wallet.
 * - Version inicia em 1 na abertura e incrementa exclusivamente quando o saldo muda.
 */
export class Wallet {
  /**
   * Construtor privado para garantir que a Wallet só seja instanciada via Wallet.open() ou Wallet.rehydrate().
   */
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /**
   * Fábrica de negócio para abertura de uma nova carteira com version inicial igual a 1.
   */
  static open(props: OpenWalletProps): Wallet {
    if (!props.id || !props.playerId) {
      throw new InvalidMoneyError('Wallet requires valid id and playerId.');
    }
    if (props.initialBalance.isNegative()) {
      throw new InvalidMoneyError('Wallet initialBalance cannot be negative.');
    }

    const now = props.createdAt ?? new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  /**
   * Reconstrução a partir da persistência — não revalida transições (Seção 6.0).
   */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  /** Retorna o saldo atual imutável da carteira. */
  get balance(): Money {
    return this._balance;
  }

  /** Retorna a versão atual da carteira (incrementada a cada mudança de saldo). */
  get version(): number {
    return this._version;
  }

  /** Retorna a data/hora da última atualização de saldo da carteira. */
  get updatedAt(): Date {
    return this._updatedAt;
  }

  /**
   * Consulta se a carteira possui saldo suficiente para cobrir o valor informado na mesma moeda.
   */
  canAfford(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  /**
   * Aplica um débito na carteira, atualizando saldo, incrementando version
   * e retornando o lançamento imutável correspondente no ledger.
   */
  debit(params: WalletMovementParams): WalletLedgerEntry {
    this.assertSameCurrency(params.money);

    if (!params.money.isPositive()) {
      throw new InvalidMoneyError('Debit amount must be strictly positive.');
    }

    if (this._balance.isLessThan(params.money)) {
      throw new InsufficientFundsError(
        this.id,
        this._balance.amount,
        params.money.amount,
        this.currency,
      );
    }

    const balanceBefore = this._balance;
    const balanceAfter = this._balance.subtract(params.money);
    const now = params.occurredAt ?? new Date();

    const entry = WalletLedgerEntry.create({
      id: params.entryId,
      walletId: this.id,
      transactionId: params.transactionId,
      direction: LedgerDirection.Debit,
      money: params.money,
      balanceBefore,
      balanceAfter,
      createdAt: now,
    });

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = now;

    return entry;
  }

  /**
   * Aplica um crédito na carteira, atualizando saldo, incrementando version
   * e retornando o lançamento imutável correspondente no ledger.
   */
  credit(params: WalletMovementParams): WalletLedgerEntry {
    this.assertSameCurrency(params.money);

    if (!params.money.isPositive()) {
      throw new InvalidMoneyError('Credit amount must be strictly positive.');
    }

    const balanceBefore = this._balance;
    const balanceAfter = this._balance.add(params.money);
    const now = params.occurredAt ?? new Date();

    const entry = WalletLedgerEntry.create({
      id: params.entryId,
      walletId: this.id,
      transactionId: params.transactionId,
      direction: LedgerDirection.Credit,
      money: params.money,
      balanceBefore,
      balanceAfter,
      createdAt: now,
    });

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = now;

    return entry;
  }

  /**
   * Valida se o valor monetário pertence à mesma moeda configurada na Wallet.
   */
  public assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
