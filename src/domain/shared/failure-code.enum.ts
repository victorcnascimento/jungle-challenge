/**
 * Taxonomia estável e legível por máquina para rejeições de negócio e falhas permanentes.
 * Permite que o provedor de jogos decida deterministicamente se reenvia, corrige o payload ou desiste.
 */
export enum FailureCode {
  /** Aposta (BET) rejeitada porque o saldo disponível na Wallet é menor que o valor apostado. */
  InsufficientFunds = 'INSUFFICIENT_FUNDS',

  /**
   * Reversão (ROLLBACK de WIN ou REFUND) rejeitada porque debitar o valor deixaria o saldo da Wallet negativo.
   * Distinto de INSUFFICIENT_FUNDS por representar uma situação operacional diferente (Regra 7.9).
   */
  NegativeBalanceOnReversal = 'NEGATIVE_BALANCE_ON_REVERSAL',

  /** A transação de referência já foi revertida anteriormente pelo mesmo tipo de operação (Regra 7.4). */
  DuplicateReversal = 'DUPLICATE_REVERSAL',

  /**
   * Tipo da transação referenciada incompatível com a operação:
   * - REFUND só pode referenciar BET
   * - ROLLBACK só pode referenciar BET, WIN ou REFUND (Regra 7.3)
   */
  InvalidReferenceKind = 'INVALID_REFERENCE_KIND',

  /**
   * A transação referenciada diverge em playerId, walletId, currency, roundId ou valor (money)
   * em relação à transação de reversão/ganho (Regras 7.2 e 7.5).
   */
  ReferenceMismatch = 'REFERENCE_MISMATCH',

  /** A transação referenciada existe, porém não está em estado PROCESSED (ex.: foi REJECTED ou FAILED). */
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',

  /**
   * A transação permaneceu em PENDING_REFERENCE aguardando a transação referenciada fora de ordem,
   * mas esgotou o limite máximo de tentativas/TTL (Seção 7.1).
   */
  ReferenceNotFoundTimeout = 'REFERENCE_NOT_FOUND_TIMEOUT',

  /** A moeda da operação difere da moeda configurada na Wallet. */
  CurrencyMismatch = 'CURRENCY_MISMATCH',

  /** Falha permanente de infraestrutura ou inconsistência irrecuperável auditável. */
  PermanentInfrastructureFailure = 'PERMANENT_INFRASTRUCTURE_FAILURE',
}
