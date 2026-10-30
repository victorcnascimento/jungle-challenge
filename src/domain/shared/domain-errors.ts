import { FailureCode } from './failure-code.enum';

/**
 * Classe base abstrata para todos os erros de regra de negócio e invariantes de domínio.
 * Permite que a camada de apresentação (GlobalExceptionFilter) e o consumidor SQS
 * identifiquem erros de domínio de forma tipada sem depender de strings de mensagem.
 */
export abstract class DomainError extends Error {
  public abstract readonly code: string;

  /**
   * Inicializa o erro de domínio ajustando o nome da classe e a cadeia de protótipos do TypeScript.
   */
  protected constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Erro lançado quando um valor monetário (Money) possui formato inválido, escala incorreta,
 * valor negativo em contrato de entrada ou código de moeda fora do padrão ISO-4217.
 */
export class InvalidMoneyError extends DomainError {
  public readonly code = 'INVALID_MONEY';

  constructor(message: string) {
    super(message);
  }
}

/**
 * Erro lançado quando há tentativa de somar, subtrair, comparar ou movimentar valores
 * entre moedas diferentes (ex.: tentar debitar USD de uma Wallet em BRL).
 */
export class CurrencyMismatchError extends DomainError {
  public readonly code = FailureCode.CurrencyMismatch;

  constructor(expectedCurrency: string, actualCurrency: string) {
    super(`Currency mismatch: expected ${expectedCurrency}, got ${actualCurrency}.`);
  }
}

/**
 * Erro lançado quando o código tenta realizar uma transição de estado proibida em uma
 * transação ou mensagem que já atingiu um estado terminal (PROCESSED, REJECTED ou FAILED).
 */
export class InvalidTransactionStateError extends DomainError {
  public readonly code = 'INVALID_TRANSACTION_STATE';

  constructor(currentStatus: string, targetStatus: string) {
    super(
      `Cannot transition WagerTransaction from status "${currentStatus}" to "${targetStatus}".`,
    );
  }
}

/**
 * Erro lançado quando o contrato de entrada de uma transação viola pré-condições estruturais
 * (ex.: submeter OPENING externamente, omitir referência em REFUND/ROLLBACK ou enviar valor zero em BET).
 */
export class InvalidTransactionContractError extends DomainError {
  public readonly code = 'INVALID_TRANSACTION_CONTRACT';

  constructor(message: string) {
    super(message);
  }
}

/**
 * Erro lançado caso uma entrada contábil (WalletLedgerEntry) viole a equação
 * fundamental do ledger: balanceBefore ± money === balanceAfter.
 */
export class ImbalancedLedgerEntryError extends DomainError {
  public readonly code = 'IMBALANCED_LEDGER_ENTRY';

  constructor(message: string) {
    super(message);
  }
}

/**
 * Erro lançado pelo Aggregate Root Wallet quando há tentativa de débito maior que o saldo disponível.
 */
export class InsufficientFundsError extends DomainError {
  public readonly code = FailureCode.InsufficientFunds;

  constructor(walletId: string, available: string, requested: string, currency: string) {
    super(
      `Insufficient funds in wallet ${walletId}: available ${available} ${currency}, requested ${requested} ${currency}.`,
    );
  }
}

/**
 * Erro lançado quando uma mesma Idempotency-Key (ou par providerId + externalTransactionId)
 * é reutilizada com um payload de negócio diferente do original (payloadHash divergente).
 */
export class IdempotencyConflictError extends DomainError {
  public readonly code = 'IDEMPOTENCY_KEY_CONFLICT';

  constructor(public readonly idempotencyKey: string) {
    super(
      `Idempotency key "${idempotencyKey}" was already used with a different request payload.`,
    );
  }
}

/**
 * Erro lançado quando já existe uma carteira para o mesmo par (playerId, currency).
 */
export class DuplicateWalletError extends DomainError {
  public readonly code = 'DUPLICATE_WALLET';

  constructor(playerId: string, currency: string) {
    super(`Wallet already exists for player "${playerId}" and currency "${currency}".`);
  }
}

/**
 * Erro lançado quando uma entidade buscada (Wallet ou WagerTransaction) não existe no banco.
 */
export class EntityNotFoundError extends DomainError {
  public readonly code = 'ENTITY_NOT_FOUND';

  constructor(entityName: string, identifier: string) {
    super(`${entityName} "${identifier}" was not found.`);
  }
}
