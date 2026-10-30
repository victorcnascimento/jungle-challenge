import { describe, expect, it } from 'bun:test';
import {
  InboxMessage,
} from '../../src/domain/messaging/inbox-message.entity';
import {
  WalletBalanceChanged,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WagerTransactionPendingReference,
} from '../../src/domain/messaging/integration-events';
import { OutboxMessage } from '../../src/domain/messaging/outbox-message.entity';
import {
  canonicalizeJson,
  computeWagerPayloadHash,
} from '../../src/domain/shared/canonical-hash';
import {
  CurrencyMismatchError,
  ImbalancedLedgerEntryError,
  InsufficientFundsError,
  InvalidMoneyError,
  InvalidTransactionContractError,
  InvalidTransactionStateError,
} from '../../src/domain/shared/domain-errors';
import { FailureCode } from '../../src/domain/shared/failure-code.enum';
import { Money } from '../../src/domain/shared/money.vo';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/wagering/wager-transaction.entity';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../../src/domain/wallet/wallet-ledger-entry.entity';
import { Wallet } from '../../src/domain/wallet/wallet.aggregate';
import { StructuredLoggerService } from '../../src/infrastructure/observability/structured-logger.service';

describe('Domain Layer — Unit Tests', () => {
  describe('6.1 Money (Value Object)', () => {
    it('creates valid Money instances with fixed scale of 2 decimal places', () => {
      const m = Money.from({ amount: '25.00', currency: 'BRL' });
      expect(m.amount).toBe('25.00');
      expect(m.currency).toBe('BRL');
      expect(m.isPositive()).toBe(true);
      expect(m.isZero()).toBe(false);
      expect(m.isNegative()).toBe(false);
      expect(m.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
      expect(m.toString()).toBe('25.00 BRL');
    });

    it('creates zero Money via Money.zero()', () => {
      const z = Money.zero('BRL');
      expect(z.amount).toBe('0.00');
      expect(z.isZero()).toBe(true);
      expect(z.isPositive()).toBe(false);
      expect(z.isNegative()).toBe(false);
      expect(z.negate().amount).toBe('0.00');
    });

    it('performs exact decimal arithmetic without IEEE-754 floating point drift', () => {
      const a = Money.from({ amount: '0.10', currency: 'BRL' });
      const b = Money.from({ amount: '0.20', currency: 'BRL' });
      const sum = a.add(b);
      expect(sum.amount).toBe('0.30');
      expect(sum.equals(Money.from({ amount: '0.30', currency: 'BRL' }))).toBe(true);

      const diff = sum.subtract(a);
      expect(diff.amount).toBe('0.20');

      const neg = a.negate();
      expect(neg.amount).toBe('-0.10');
      expect(neg.isNegative()).toBe(true);
    });

    it('rejects invalid monetary inputs (NaN, Infinity, scientific notation, empty, wrong scale, negative)', () => {
      const invalidAmounts = [
        '',
        'NaN',
        'Infinity',
        '-Infinity',
        '1e5',
        '1e-2',
        '25',
        '25.0',
        '25.000',
        '-10.00',
        '+10.00',
        '00.50',
        ' 25.00 ',
        'abc',
      ];

      for (const amount of invalidAmounts) {
        expect(() => Money.from({ amount, currency: 'BRL' })).toThrow(InvalidMoneyError);
      }

      const invalidCurrencies = ['', 'br', 'BRLX', '123', 'brl'];
      for (const currency of invalidCurrencies) {
        expect(() => Money.from({ amount: '10.00', currency })).toThrow(InvalidMoneyError);
      }
    });

    it('throws CurrencyMismatchError when operating across different currencies', () => {
      const brl = Money.from({ amount: '10.00', currency: 'BRL' });
      const usd = Money.from({ amount: '10.00', currency: 'USD' });

      expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
      expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
      expect(() => brl.equals(usd)).toThrow(CurrencyMismatchError);
      expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
    });
  });

  describe('6.2 Wallet (Aggregate Root) & 6.4 WalletLedgerEntry', () => {
    it('opens a wallet with version = 1 and non-negative balance', () => {
      const wallet = Wallet.open({
        id: 'wallet-1',
        playerId: 'player-1',
        initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
      });

      expect(wallet.id).toBe('wallet-1');
      expect(wallet.balance.amount).toBe('100.00');
      expect(wallet.version).toBe(1);
    });

    it('debits and credits wallet while maintaining strict ledger consistency and incrementing version', () => {
      const wallet = Wallet.open({
        id: 'wallet-1',
        playerId: 'player-1',
        initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
      });

      const debitEntry = wallet.debit({
        entryId: 'entry-1',
        transactionId: 'tx-1',
        money: Money.from({ amount: '80.00', currency: 'BRL' }),
      });

      expect(wallet.balance.amount).toBe('20.00');
      expect(wallet.version).toBe(2);
      expect(debitEntry.direction).toBe(LedgerDirection.Debit);
      expect(debitEntry.balanceBefore.amount).toBe('100.00');
      expect(debitEntry.balanceAfter.amount).toBe('20.00');
      expect(debitEntry.isBalanced()).toBe(true);

      const creditEntry = wallet.credit({
        entryId: 'entry-2',
        transactionId: 'tx-2',
        money: Money.from({ amount: '50.00', currency: 'BRL' }),
      });

      expect(wallet.balance.amount).toBe('70.00');
      expect(wallet.version).toBe(3);
      expect(creditEntry.direction).toBe(LedgerDirection.Credit);
      expect(creditEntry.balanceBefore.amount).toBe('20.00');
      expect(creditEntry.balanceAfter.amount).toBe('70.00');
      expect(creditEntry.isBalanced()).toBe(true);
    });

    it('rejects debit when funds are insufficient without mutating balance or version', () => {
      const wallet = Wallet.open({
        id: 'wallet-1',
        playerId: 'player-1',
        initialBalance: Money.from({ amount: '20.00', currency: 'BRL' }),
      });

      expect(() =>
        wallet.debit({
          entryId: 'entry-fail',
          transactionId: 'tx-fail',
          money: Money.from({ amount: '80.00', currency: 'BRL' }),
        }),
      ).toThrow(InsufficientFundsError);

      expect(wallet.balance.amount).toBe('20.00');
      expect(wallet.version).toBe(1);
    });

    it('rejects currency mismatch on wallet operations', () => {
      const wallet = Wallet.open({
        id: 'wallet-1',
        playerId: 'player-1',
        initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
      });

      expect(() =>
        wallet.debit({
          entryId: 'entry-usd',
          transactionId: 'tx-usd',
          money: Money.from({ amount: '10.00', currency: 'USD' }),
        }),
      ).toThrow(CurrencyMismatchError);
    });

    it('enforces arithmetic balance invariant on WalletLedgerEntry.create()', () => {
      expect(() =>
        WalletLedgerEntry.create({
          id: 'entry-bad',
          walletId: 'wallet-1',
          transactionId: 'tx-bad',
          direction: LedgerDirection.Debit,
          money: Money.from({ amount: '30.00', currency: 'BRL' }),
          balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
          balanceAfter: Money.from({ amount: '80.00', currency: 'BRL' }), // Should be 70.00
        }),
      ).toThrow(ImbalancedLedgerEntryError);
    });
  });

  describe('6.3 WagerTransaction & Section 7 Business Rules', () => {
    const baseProps = {
      id: 'tx-1',
      providerId: 'provider-a',
      externalTransactionId: 'ext-1',
      idempotencyKey: 'provider-a:ext-1',
      payloadHash: 'hash-1',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
    };

    it('forbids external creation of OPENING transaction kind', () => {
      expect(() =>
        WagerTransaction.create({
          ...baseProps,
          kind: WagerTransactionKind.Opening,
        }),
      ).toThrow(InvalidTransactionContractError);
    });

    it('requires referenceExternalTransactionId for REFUND and ROLLBACK', () => {
      expect(() =>
        WagerTransaction.create({
          ...baseProps,
          kind: WagerTransactionKind.Refund,
        }),
      ).toThrow(InvalidTransactionContractError);

      expect(() =>
        WagerTransaction.create({
          ...baseProps,
          kind: WagerTransactionKind.Rollback,
        }),
      ).toThrow(InvalidTransactionContractError);
    });

    it('prevents state transitions after reaching a terminal state (PROCESSED, REJECTED, FAILED)', () => {
      const tx = WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Bet,
      });

      tx.markProcessed(undefined, new Date());
      expect(tx.isTerminal()).toBe(true);

      expect(() => tx.reject(FailureCode.InsufficientFunds)).toThrow(
        InvalidTransactionStateError,
      );
      expect(() => tx.markPendingReference()).toThrow(InvalidTransactionStateError);
      expect(() => tx.fail(FailureCode.PermanentInfrastructureFailure)).toThrow(
        InvalidTransactionStateError,
      );
    });

    it('correctly identifies balance impact and ledger direction for BET, WIN, LOSS, REFUND, ROLLBACK', () => {
      const bet = WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Bet,
      });
      bet.markProcessed(undefined, new Date());
      expect(bet.affectsBalance()).toBe(true);
      expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);

      const win = WagerTransaction.create({
        ...baseProps,
        id: 'tx-win',
        externalTransactionId: 'ext-win',
        kind: WagerTransactionKind.Win,
      });
      win.markProcessed(bet.id, new Date());
      expect(win.affectsBalance()).toBe(true);
      expect(win.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);

      const loss = WagerTransaction.create({
        ...baseProps,
        id: 'tx-loss',
        externalTransactionId: 'ext-loss',
        kind: WagerTransactionKind.Loss,
        money: Money.zero('BRL'),
      });
      expect(loss.affectsBalance()).toBe(false);
      expect(() => loss.ledgerDirectionFor()).toThrow(InvalidTransactionContractError);

      const refund = WagerTransaction.create({
        ...baseProps,
        id: 'tx-refund',
        externalTransactionId: 'ext-refund',
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: bet.externalTransactionId,
      });
      expect(refund.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);

      const rollbackOfBet = WagerTransaction.create({
        ...baseProps,
        id: 'tx-rb-bet',
        externalTransactionId: 'ext-rb-bet',
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: bet.externalTransactionId,
      });
      expect(rollbackOfBet.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);

      const rollbackOfWin = WagerTransaction.create({
        ...baseProps,
        id: 'tx-rb-win',
        externalTransactionId: 'ext-rb-win',
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: win.externalTransactionId,
      });
      expect(rollbackOfWin.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    });

    it('validates reference rules (REFUND only references BET; ROLLBACK references BET, WIN, REFUND; matching amount & round)', () => {
      const bet = WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Bet,
      });
      bet.markProcessed(undefined, new Date());

      const win = WagerTransaction.create({
        ...baseProps,
        id: 'tx-win',
        externalTransactionId: 'ext-win',
        kind: WagerTransactionKind.Win,
      });
      win.markProcessed(bet.id, new Date());

      // REFUND referencing WIN must fail with INVALID_REFERENCE_KIND
      const refundOnWin = WagerTransaction.create({
        ...baseProps,
        id: 'tx-ref-win',
        externalTransactionId: 'ext-ref-win',
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: win.externalTransactionId,
      });
      expect(refundOnWin.validateReference(win)).toBe(FailureCode.InvalidReferenceKind);

      // REFUND with different amount must fail with REFERENCE_MISMATCH
      const partialRefund = WagerTransaction.create({
        ...baseProps,
        id: 'tx-ref-partial',
        externalTransactionId: 'ext-ref-partial',
        kind: WagerTransactionKind.Refund,
        money: Money.from({ amount: '10.00', currency: 'BRL' }),
        referenceExternalTransactionId: bet.externalTransactionId,
      });
      expect(partialRefund.validateReference(bet)).toBe(FailureCode.ReferenceMismatch);

      // Valid REFUND on BET returns null (no failure)
      const validRefund = WagerTransaction.create({
        ...baseProps,
        id: 'tx-ref-ok',
        externalTransactionId: 'ext-ref-ok',
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: bet.externalTransactionId,
      });
      expect(validRefund.validateReference(bet)).toBeNull();
    });
  });

  describe('Canonical Payload Hash & Idempotency Conflict Detection', () => {
    it('produces identical hash regardless of key order and detects payload divergence', () => {
      const hashA = canonicalizeJson({ b: 2, a: { y: 'yes', x: 'no' } });
      const hashB = canonicalizeJson({ a: { x: 'no', y: 'yes' }, b: 2 });
      expect(hashA).toBe(hashB);

      const basePayload = {
        providerId: 'provider-a',
        externalTransactionId: 'tx-123',
        playerId: 'player-1',
        walletId: 'wallet-1',
        roundId: 'round-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '25.00', currency: 'BRL' },
      };

      const h1 = computeWagerPayloadHash(basePayload);
      const h2 = computeWagerPayloadHash({ ...basePayload });
      const hConflict = computeWagerPayloadHash({
        ...basePayload,
        money: { amount: '50.00', currency: 'BRL' },
      });

      expect(h1).toBe(h2);
      expect(h1).not.toBe(hConflict);
    });
  });

  describe('6.5 Inbox, Outbox & 11 IntegrationEvents', () => {
    it('manages InboxMessage lifecycle', () => {
      const inbox = InboxMessage.receive({
        messageId: 'msg-1',
        consumerName: 'sqs-wager-consumer',
        payloadHash: 'hash-1',
      });
      expect(inbox.isProcessed()).toBe(false);
      inbox.markProcessed(new Date());
      expect(inbox.isProcessed()).toBe(true);
    });

    it('serializes IntegrationEvents into OutboxMessage and schedules exponential backoff retries', () => {
      const wallet = Wallet.open({
        id: 'wallet-1',
        playerId: 'player-1',
        initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
      });
      const entry = wallet.debit({
        entryId: 'entry-1',
        transactionId: 'tx-1',
        money: Money.from({ amount: '25.00', currency: 'BRL' }),
      });

      const event = WalletBalanceChanged.from(wallet, entry, {
        correlationId: 'corr-1',
      });
      const outbox = OutboxMessage.enqueue(event);

      expect(outbox.isPending()).toBe(true);
      expect(outbox.eventType).toBe('WalletBalanceChanged');
      expect((outbox.payload.data as Record<string, unknown>).walletVersion).toBe(2);

      const now = new Date();
      outbox.scheduleRetry(now);
      expect(outbox.attempts).toBe(1);
      expect(outbox.isDue(now)).toBe(false);

      outbox.markPublished(new Date());
      expect(outbox.isPending()).toBe(false);
    });

    it('redacts sensitive credentials and financial payloads recursively in StructuredLoggerService (Section 12)', () => {
      const logger = new StructuredLoggerService();
      const sanitized = (
        logger as unknown as {
          sanitizeObject: (ctx: Record<string, unknown>) => Record<string, unknown>;
        }
      ).sanitizeObject({
        correlationId: 'corr-123',
        walletId: 'wallet-123',
        providerId: 'provider-a',
        authorization: 'Bearer secret-jwt',
        money: { amount: '1000.00', currency: 'BRL' },
        nested: {
          password: '123',
          balance: '500.00',
          safeField: 'ok',
        },
      });

      expect(sanitized.correlationId).toBe('corr-123');
      expect(sanitized.walletId).toBe('wallet-123');
      expect(sanitized.authorization).toBe('[REDACTED]');
      expect(sanitized.money).toBe('[REDACTED]');
      expect((sanitized.nested as Record<string, unknown>).password).toBe('[REDACTED]');
      expect((sanitized.nested as Record<string, unknown>).balance).toBe('[REDACTED]');
      expect((sanitized.nested as Record<string, unknown>).safeField).toBe('ok');
    });
  });
});
