import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { randomUUID } from 'crypto';
import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { INestApplication } from '@nestjs/common';
import { PublishOutboxMessagesUseCase } from '../../src/application/use-cases/publish-outbox.use-case';
import { FailureCode } from '../../src/domain/shared/failure-code.enum';
import { WagerTransactionStatus } from '../../src/domain/wagering/wager-transaction.entity';
import { createMikroOrmOptions } from '../../src/infrastructure/database/mikro-orm.config';
import { SqsMessagingService } from '../../src/infrastructure/messaging/sqs.service';
import { createConfiguredApp } from '../../src/main';
import { TestInfrastructure } from '../helpers/test-infra';

describe('Integration Tests — PostgreSQL Schema, Atomicity, Inbox/Outbox, DLQ & Recovery', () => {
  const infra = new TestInfrastructure();
  let orm: MikroORM;
  let app: INestApplication;
  let baseUrl: string;

  beforeAll(async () => {
    await infra.start();
    orm = await MikroORM.init(createMikroOrmOptions(infra.databaseUrl));

    // Testa reversibilidade das migrations (up -> down -> up)
    const migrator = orm.getMigrator();
    await migrator.up();
    await migrator.down();
    await migrator.up();

    app = await createConfiguredApp({ orm });
    await app.listen(0, '127.0.0.1');
    const serverAddr = app.getHttpServer().address();
    baseUrl = `http://127.0.0.1:${serverAddr.port}`;
  });

  afterAll(async () => {
    if (app) {
      await app.close();
    }
    if (orm) {
      await orm.close(true);
    }
    await infra.stop();
  });

  it('1. Enforces non-negativity, uniqueness, balanced arithmetic and ledger immutability in PostgreSQL schema (Section 5.9)', async () => {
    const conn = orm.em.getConnection();
    const walletId = randomUUID();
    const playerId = `player-schema-${randomUUID()}`;

    // 1.1 CHECK (balance_amount >= 0) na tabela wallets
    await expect(
      conn.execute(
        `INSERT INTO wallets (id, player_id, currency, balance_amount, version)
         VALUES (?, ?, 'BRL', -10.00, 1)`,
        [walletId, playerId],
      ),
    ).rejects.toThrow();

    // Inserção válida
    await conn.execute(
      `INSERT INTO wallets (id, player_id, currency, balance_amount, version)
       VALUES (?, ?, 'BRL', 100.00, 1)`,
      [walletId, playerId],
    );

    // 1.2 UNIQUE (player_id, currency) na tabela wallets
    await expect(
      conn.execute(
        `INSERT INTO wallets (id, player_id, currency, balance_amount, version)
         VALUES (?, ?, 'BRL', 50.00, 1)`,
        [randomUUID(), playerId],
      ),
    ).rejects.toThrow();

    // Criar transação válida para testar constraints do ledger
    const txId = randomUUID();
    await conn.execute(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency, status
      ) VALUES (?, 'prov-1', ?, ?, ?, ?, ?, 'r-1', 'g-1', 'BET', 30.00, 'BRL', 'PROCESSED')`,
      [
        txId,
        `ext-${txId}`,
        `idem-${txId}`,
        'a'.repeat(64),
        walletId,
        playerId,
      ],
    );

    // 1.3 CHECK ((direction = 'DEBIT' AND balance_after = balance_before - amount)) no ledger
    await expect(
      conn.execute(
        `INSERT INTO wallet_ledger_entries (
          id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after
        ) VALUES (?, ?, ?, 'DEBIT', 30.00, 'BRL', 100.00, 90.00)`,
        [randomUUID(), walletId, txId],
      ),
    ).rejects.toThrow();

    // Lançamento balanceado válido (100.00 - 30.00 = 70.00)
    const entryId = randomUUID();
    await conn.execute(
      `INSERT INTO wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after
      ) VALUES (?, ?, ?, 'DEBIT', 30.00, 'BRL', 100.00, 70.00)`,
      [entryId, walletId, txId],
    );

    // 1.4 Trigger trg_prevent_wallet_ledger_mutation impede UPDATE e DELETE no ledger
    await expect(
      conn.execute(
        `UPDATE wallet_ledger_entries SET amount = 10.00 WHERE id = ?`,
        [entryId],
      ),
    ).rejects.toThrow();

    await expect(
      conn.execute(`DELETE FROM wallet_ledger_entries WHERE id = ?`, [entryId]),
    ).rejects.toThrow();
  });

  it('2. Executes full HTTP lifecycle: Wallet Opening, BET, WIN, LOSS, REFUND, ROLLBACK, Negative Reversal Rejection & Cursor Ledger', async () => {
    const playerId = randomUUID();

    // 2.1 Criar Wallet com saldo inicial 1000.00 BRL -> gera OPENING + CREDIT no ledger + Outbox
    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '1000.00', currency: 'BRL' },
      }),
    });
    expect(createRes.status).toBe(201);
    const wallet = (await createRes.json()) as {
      id: string;
      playerId: string;
      balance: { amount: string; currency: string };
      version: number;
    };
    expect(wallet.balance).toEqual({ amount: '1000.00', currency: 'BRL' });
    expect(wallet.version).toBe(1);

    // Criar wallet duplicada para o mesmo playerId + currency -> 409 Conflict
    const dupRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '500.00', currency: 'BRL' },
      }),
    });
    expect(dupRes.status).toBe(409);

    // 2.2 Submeter BET de 200.00 BRL -> saldo passa para 800.00 BRL
    const betExtId = `bet-${randomUUID()}`;
    const betRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '200.00', currency: 'BRL' },
      }),
    });
    expect(betRes.status).toBe(200);
    const betData = (await betRes.json()) as {
      transactionId: string;
      status: string;
      balance: { amount: string; currency: string };
      idempotentReplay: boolean;
    };
    expect(betData.status).toBe(WagerTransactionStatus.Processed);
    expect(betData.balance.amount).toBe('800.00');
    expect(betData.idempotentReplay).toBe(false);

    // 2.3 Submeter LOSS -> não altera saldo nem gera entrada no ledger
    const lossExtId = `loss-${randomUUID()}`;
    const lossRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${lossExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: lossExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: 'LOSS',
        money: { amount: '0.00', currency: 'BRL' },
      }),
    });
    expect(lossRes.status).toBe(200);
    const lossData = (await lossRes.json()) as {
      status: string;
      balance: { amount: string };
    };
    expect(lossData.status).toBe(WagerTransactionStatus.Processed);
    expect(lossData.balance.amount).toBe('800.00');

    // 2.4 Submeter REFUND da BET de 200.00 -> saldo volta para 1000.00 BRL
    const refundExtId = `refund-${randomUUID()}`;
    const refundRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${refundExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: refundExtId,
        referenceExternalTransactionId: betExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '200.00', currency: 'BRL' },
      }),
    });
    expect(refundRes.status).toBe(200);
    const refundData = (await refundRes.json()) as {
      status: string;
      balance: { amount: string };
    };
    expect(refundData.status).toBe(WagerTransactionStatus.Processed);
    expect(refundData.balance.amount).toBe('1000.00');

    // 2.5 Tentar segundo REFUND na mesma BET -> rejeitado com DUPLICATE_REVERSAL (422)
    const dupRefundExtId = `refund-dup-${randomUUID()}`;
    const dupRefundRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${dupRefundExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: dupRefundExtId,
        referenceExternalTransactionId: betExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '200.00', currency: 'BRL' },
      }),
    });
    expect(dupRefundRes.status).toBe(422);
    const dupRefundBody = (await dupRefundRes.json()) as {
      status: string;
      failureCode: string;
    };
    expect(dupRefundBody.status).toBe(WagerTransactionStatus.Rejected);
    expect(dupRefundBody.failureCode).toBe(FailureCode.DuplicateReversal);

    // 2.6 Testar Regra 7.9: Reversão (ROLLBACK de WIN) que deixaria saldo negativo
    // WIN de 500.00 (saldo = 1500.00) -> BET de 1200.00 (saldo = 300.00) -> ROLLBACK do WIN de 500.00 -> REJECTED com NEGATIVE_BALANCE_ON_REVERSAL
    const winExtId = `win-${randomUUID()}`;
    await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${winExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: winExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-200',
        gameId: 'fortune-chimp',
        kind: 'WIN',
        money: { amount: '500.00', currency: 'BRL' },
      }),
    });

    const bigBetExtId = `bigbet-${randomUUID()}`;
    await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${bigBetExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: bigBetExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-201',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '1200.00', currency: 'BRL' },
      }),
    });

    const rbWinExtId = `rb-win-${randomUUID()}`;
    const rbWinRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${rbWinExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: rbWinExtId,
        referenceExternalTransactionId: winExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-200',
        gameId: 'fortune-chimp',
        kind: 'ROLLBACK',
        money: { amount: '500.00', currency: 'BRL' },
      }),
    });
    expect(rbWinRes.status).toBe(422);
    const rbWinBody = (await rbWinRes.json()) as {
      status: string;
      failureCode: string;
      balance: { amount: string };
    };
    expect(rbWinBody.status).toBe(WagerTransactionStatus.Rejected);
    expect(rbWinBody.failureCode).toBe(FailureCode.NegativeBalanceOnReversal);
    expect(rbWinBody.balance.amount).toBe('300.00');

    // 2.7 Paginação por cursor estável e opaco no Ledger
    const page1Res = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?limit=2`,
    );
    expect(page1Res.status).toBe(200);
    const page1 = (await page1Res.json()) as {
      items: Array<{ id: string }>;
      nextCursor: string | null;
    };
    expect(page1.items.length).toBe(2);
    expect(page1.nextCursor).toBeTruthy();

    const page2Res = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?cursor=${page1.nextCursor}&limit=10`,
    );
    const page2 = (await page2Res.json()) as {
      items: Array<{ id: string }>;
      nextCursor: string | null;
    };
    // Total de lançamentos no ledger: OPENING (+1000), BET (-200), REFUND (+200), WIN (+500), BET (-1200) = 5 entradas (LOSS e REJECTED não entram!)
    expect(page1.items.length + page2.items.length).toBe(5);
    expect(page2.nextCursor).toBeNull();

    // 2.8 Reconciliação final da Wallet
    const reconRes = await fetch(
      `${baseUrl}/wallets/${wallet.id}/reconciliation`,
      { method: 'POST' },
    );
    expect(reconRes.status).toBe(200);
    const recon = (await reconRes.json()) as {
      consistent: boolean;
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
      difference: { amount: string };
      checkedEntries: number;
    };
    expect(recon.consistent).toBe(true);
    expect(recon.storedBalance.amount).toBe('300.00');
    expect(recon.calculatedBalance.amount).toBe('300.00');
    expect(recon.difference.amount).toBe('0.00');
    expect(recon.checkedEntries).toBe(5);

    // 2.9 Validar conflito de Idempotency-Key quando reutilizada com outra walletId -> 409 Conflict
    const conflictRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betExtId,
        playerId,
        walletId: randomUUID(), // Outra walletId com a mesma Idempotency-Key
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '200.00', currency: 'BRL' },
      }),
    });
    expect(conflictRes.status).toBe(409);

    // 2.10 Validar que UUID malformado retorna 400 Bad Request em vez de 500
    const badUuidRes = await fetch(`${baseUrl}/wallets/not-a-valid-uuid`);
    expect(badUuidRes.status).toBe(400);
  });

  it('3. Deduplicates SQS messages via persistent Inbox, routes poison messages to DLQ, and publishes Outbox events', async () => {
    const sqsService = app.get(SqsMessagingService);
    const outboxPublisher = app.get(PublishOutboxMessagesUseCase);

    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: '100.00', currency: 'BRL' },
      }),
    });
    const wallet = (await createRes.json()) as { id: string; playerId: string };

    const msgId = `msg-${randomUUID()}`;
    const extId = `sqs-bet-${randomUUID()}`;
    const validEnvelope = {
      messageId: msgId,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'provider-sqs',
        externalTransactionId: extId,
        idempotencyKey: `provider-sqs:${extId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-sqs-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '40.00', currency: 'BRL' },
      },
    };

    // Enviar a mesma mensagem duas vezes (simulando redelivery at-least-once do SQS)
    await sqsService.client.send(
      new SendMessageCommand({
        QueueUrl: sqsService.wagerQueueUrl,
        MessageBody: JSON.stringify(validEnvelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: randomUUID(),
      }),
    );
    await sqsService.client.send(
      new SendMessageCommand({
        QueueUrl: sqsService.wagerQueueUrl,
        MessageBody: JSON.stringify(validEnvelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: randomUUID(),
      }),
    );

    // Enviar também uma mensagem malformada que deve ir para a DLQ
    await sqsService.client.send(
      new SendMessageCommand({
        QueueUrl: sqsService.wagerQueueUrl,
        MessageBody: '{"invalid_json_envelope": true}',
        MessageGroupId: wallet.id,
        MessageDeduplicationId: randomUUID(),
      }),
    );

    await sqsService.pollAndProcessBatch(10, 0);

    // Verificar que o saldo foi debitado apenas 1 vez (100 - 40 = 60.00)
    const walletRes = await fetch(`${baseUrl}/wallets/${wallet.id}`);
    const updatedWallet = (await walletRes.json()) as {
      balance: { amount: string };
      version: number;
    };
    expect(updatedWallet.balance.amount).toBe('60.00');
    expect(updatedWallet.version).toBe(2);

    // Verificar que a mensagem inválida foi movida para a DLQ
    const dlqCount = await sqsService.getApproximateQueueCount(
      sqsService.wagerDlqUrl,
    );
    expect(dlqCount).toBeGreaterThanOrEqual(1);

    // Publicar eventos da Outbox e verificar que saem de pendente
    let totalPublished = 0;
    while (true) {
      const batch = await outboxPublisher.executeBatch(50);
      totalPublished += batch;
      if (batch === 0) break;
    }
    expect(totalPublished).toBeGreaterThan(0);

    // Health checks e métricas Prometheus
    const liveRes = await fetch(`${baseUrl}/health/live`);
    expect(liveRes.status).toBe(200);

    const readyRes = await fetch(`${baseUrl}/health/ready`);
    expect(readyRes.status).toBe(200);

    const metricsRes = await fetch(`${baseUrl}/metrics`);
    expect(metricsRes.status).toBe(200);
    const metricsText = await metricsRes.text();
    expect(metricsText).toContain('wagering_transactions_total');
    expect(metricsText).toContain('wagering_duplicates_detected_total');
    expect(metricsText).toContain('wagering_dlq_messages_total');
  });
});
