import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { randomUUID } from 'crypto';
import { ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { INestApplication } from '@nestjs/common';
import { PublishOutboxMessagesUseCase } from '../../src/application/use-cases/publish-outbox.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
import { RetryPendingReferencesUseCase } from '../../src/application/use-cases/retry-pending-references.use-case';
import { FailureCode } from '../../src/domain/shared/failure-code.enum';
import {
  WagerTransaction,
  WagerTransactionStatus,
} from '../../src/domain/wagering/wager-transaction.entity';
import { createMikroOrmOptions } from '../../src/infrastructure/database/mikro-orm.config';
import { SqsMessagingService } from '../../src/infrastructure/messaging/sqs.service';
import { createConfiguredApp } from '../../src/main';
import { TestInfrastructure } from '../helpers/test-infra';

describe('Concurrency & Fault-Tolerance Suite — All 8 Mandatory Scenarios (Section 13)', () => {
  const infra = new TestInfrastructure();
  let orm1: MikroORM;
  let orm2: MikroORM;
  let orm3: MikroORM;
  let app1: INestApplication;
  let app2: INestApplication;
  let app3: INestApplication;
  let urls: [string, string, string];
  const createdWalletIds: string[] = [];

  beforeAll(async () => {
    await infra.start();

    orm1 = await MikroORM.init(createMikroOrmOptions(infra.databaseUrl));
    await orm1.getMigrator().up();

    orm2 = await MikroORM.init(createMikroOrmOptions(infra.databaseUrl));
    orm3 = await MikroORM.init(createMikroOrmOptions(infra.databaseUrl));

    // Inicia 3 instâncias independentes da aplicação apontando para o mesmo PostgreSQL e SQS
    app1 = await createConfiguredApp({ orm: orm1 });
    app2 = await createConfiguredApp({ orm: orm2 });
    app3 = await createConfiguredApp({ orm: orm3 });

    await Promise.all([
      app1.listen(0, '127.0.0.1'),
      app2.listen(0, '127.0.0.1'),
      app3.listen(0, '127.0.0.1'),
    ]);

    urls = [
      `http://127.0.0.1:${app1.getHttpServer().address().port}`,
      `http://127.0.0.1:${app2.getHttpServer().address().port}`,
      `http://127.0.0.1:${app3.getHttpServer().address().port}`,
    ];
  });

  afterAll(async () => {
    await Promise.allSettled([app1?.close(), app2?.close(), app3?.close()]);
    await Promise.allSettled([
      orm1?.close(true),
      orm2?.close(true),
      orm3?.close(true),
    ]);
    await infra.stop();
  });

  async function createWallet(initialAmount: string): Promise<{
    id: string;
    playerId: string;
  }> {
    const playerId = randomUUID();
    const res = await fetch(`${urls[0]}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: initialAmount, currency: 'BRL' },
      }),
    });
    expect(res.status).toBe(201);
    const data = (await res.json()) as { id: string; playerId: string };
    createdWalletIds.push(data.id);
    return data;
  }

  it('Scenario 1: The exact same bet sent 50 times in parallel across 3 instances -> single debit and 49 idempotent replays', async () => {
    const wallet = await createWallet('100.00');
    const extId = `bet-50x-${randomUUID()}`;
    const idempotencyKey = `provider-a:${extId}`;
    const payload = {
      providerId: 'provider-a',
      externalTransactionId: extId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-50x',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };

    const parallelRequests = Array.from({ length: 50 }, (_, idx) => {
      const targetUrl = urls[idx % urls.length]!;
      return fetch(`${targetUrl}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': idempotencyKey,
        },
        body: JSON.stringify(payload),
      }).then(async (r) => ({
        status: r.status,
        body: (await r.json()) as {
          transactionId: string;
          status: string;
          balance: { amount: string };
          idempotentReplay: boolean;
        },
      }));
    });

    const responses = await Promise.all(parallelRequests);

    // Todas as 50 respostas devem ter HTTP 200, status PROCESSED, mesmo transactionId e mesmo saldo observado (75.00)
    for (const res of responses) {
      expect(res.status).toBe(200);
      expect(res.body.status).toBe(WagerTransactionStatus.Processed);
      expect(res.body.balance.amount).toBe('75.00');
    }

    const originalCount = responses.filter(
      (r) => r.body.idempotentReplay === false,
    ).length;
    const replayCount = responses.filter(
      (r) => r.body.idempotentReplay === true,
    ).length;

    expect(originalCount).toBe(1);
    expect(replayCount).toBe(49);

    // Verificar ledger: apenas 2 lançamentos (1 OPENING de 100.00 + 1 DEBIT de 25.00)
    const ledgerRes = await fetch(`${urls[0]}/wallets/${wallet.id}/ledger`);
    const ledger = (await ledgerRes.json()) as {
      items: Array<{ direction: string; money: { amount: string } }>;
    };
    expect(ledger.items.length).toBe(2);
    expect(ledger.items.filter((i) => i.direction === 'DEBIT').length).toBe(1);
  });

  it('Scenario 2: Mandatory Section 8 Race — Initial balance 100.00 BRL, two simultaneous 80.00 BRL bets -> 1 PROCESSED, 1 REJECTED, final balance 20.00 BRL', async () => {
    const wallet = await createWallet('100.00');
    const betAExt = `race-80-a-${randomUUID()}`;
    const betBExt = `race-80-b-${randomUUID()}`;

    const reqA = fetch(`${urls[0]}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betAExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betAExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-80',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '80.00', currency: 'BRL' },
      }),
    }).then(async (r) => ({
      httpStatus: r.status,
      body: (await r.json()) as {
        status: string;
        failureCode?: string;
        balance: { amount: string };
      },
    }));

    const reqB = fetch(`${urls[1]}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betBExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betBExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-80',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '80.00', currency: 'BRL' },
      }),
    }).then(async (r) => ({
      httpStatus: r.status,
      body: (await r.json()) as {
        status: string;
        failureCode?: string;
        balance: { amount: string };
      },
    }));

    const [resA, resB] = await Promise.all([reqA, reqB]);
    const both = [resA, resB];

    const processed = both.filter(
      (r) => r.body.status === WagerTransactionStatus.Processed,
    );
    const rejected = both.filter(
      (r) => r.body.status === WagerTransactionStatus.Rejected,
    );

    expect(processed.length).toBe(1);
    expect(rejected.length).toBe(1);
    expect(rejected[0]!.httpStatus).toBe(422);
    expect(rejected[0]!.body.failureCode).toBe(FailureCode.InsufficientFunds);

    // Verificar saldo final == 20.00 BRL e exatamente 1 lançamento de débito no ledger
    const walletRes = await fetch(`${urls[2]}/wallets/${wallet.id}`);
    const walletState = (await walletRes.json()) as {
      balance: { amount: string };
      version: number;
    };
    expect(walletState.balance.amount).toBe('20.00');
    expect(walletState.version).toBe(2);

    const ledgerRes = await fetch(`${urls[2]}/wallets/${wallet.id}/ledger`);
    const ledger = (await ledgerRes.json()) as {
      items: Array<{ direction: string }>;
    };
    const debitEntries = ledger.items.filter((i) => i.direction === 'DEBIT');
    expect(debitEntries.length).toBe(1);
  });

  it('Scenario 3 & 4: Distinct wallets processed in parallel AND >= 3 instances under hot-wallet contention', async () => {
    // 3.1: 8 wallets distintas processadas simultaneamente
    const wallets = await Promise.all(
      Array.from({ length: 8 }, () => createWallet('500.00')),
    );

    await Promise.all(
      wallets.map(async (w, idx) => {
        const targetUrl = urls[idx % urls.length]!;
        const betId = `multi-w-bet-${idx}-${randomUUID()}`;
        const winId = `multi-w-win-${idx}-${randomUUID()}`;

        const r1 = await fetch(`${targetUrl}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `provider-a:${betId}`,
          },
          body: JSON.stringify({
            providerId: 'provider-a',
            externalTransactionId: betId,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `r-${idx}`,
            gameId: 'fortune-chimp',
            kind: 'BET',
            money: { amount: '100.00', currency: 'BRL' },
          }),
        });
        expect(r1.status).toBe(200);

        const r2 = await fetch(`${targetUrl}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `provider-a:${winId}`,
          },
          body: JSON.stringify({
            providerId: 'provider-a',
            externalTransactionId: winId,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `r-${idx}`,
            gameId: 'fortune-chimp',
            kind: 'WIN',
            money: { amount: '250.00', currency: 'BRL' },
          }),
        });
        expect(r2.status).toBe(200);
      }),
    );

    for (const w of wallets) {
      const res = await fetch(`${urls[0]}/wallets/${w.id}`);
      const state = (await res.json()) as { balance: { amount: string } };
      expect(state.balance.amount).toBe('650.00');
    }

    // 4.1: Hot wallet disputada simultaneamente pelas 3 instâncias (app1, app2, app3)
    // Saldo 150.00 BRL, 10 apostas simultâneas de 30.00 BRL distribuídas nas 3 instâncias -> exatamente 5 PROCESSED e 5 REJECTED, saldo final 0.00 BRL
    const hotWallet = await createWallet('150.00');
    const hotPromises = Array.from({ length: 10 }, (_, idx) => {
      const extId = `hot-bet-${idx}-${randomUUID()}`;
      const instanceUrl = urls[idx % 3]!;
      return fetch(`${instanceUrl}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-hot:${extId}`,
        },
        body: JSON.stringify({
          providerId: 'provider-hot',
          externalTransactionId: extId,
          playerId: hotWallet.playerId,
          walletId: hotWallet.id,
          roundId: `round-hot-${idx}`,
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '30.00', currency: 'BRL' },
        }),
      }).then(async (r) => (await r.json()) as { status: string });
    });

    const hotResults = await Promise.all(hotPromises);
    expect(
      hotResults.filter((r) => r.status === WagerTransactionStatus.Processed)
        .length,
    ).toBe(5);
    expect(
      hotResults.filter((r) => r.status === WagerTransactionStatus.Rejected)
        .length,
    ).toBe(5);

    const hotStateRes = await fetch(`${urls[0]}/wallets/${hotWallet.id}`);
    const hotState = (await hotStateRes.json()) as {
      balance: { amount: string };
    };
    expect(hotState.balance.amount).toBe('0.00');
  });

  it('Scenario 5: Worker crashes AFTER SQL commit and BEFORE SQS ACK -> redelivery to another instance does not duplicate debit', async () => {
    const wallet = await createWallet('100.00');
    const sqs1 = app1.get(SqsMessagingService);
    const sqs2 = app2.get(SqsMessagingService);

    await infra.resetState();

    const msgId = `crash-msg-${randomUUID()}`;
    const extId = `crash-tx-${randomUUID()}`;
    const envelope = {
      messageId: msgId,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'provider-crash',
        externalTransactionId: extId,
        idempotencyKey: `provider-crash:${extId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-crash',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '45.00', currency: 'BRL' },
      },
    };

    await sqs1.client.send(
      new SendMessageCommand({
        QueueUrl: sqs1.wagerQueueUrl,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: msgId,
      }),
    );

    // Instância 1 recebe a mensagem, confirma o COMMIT no PostgreSQL, mas morre antes do ACK
    const received = await sqs1.client.send(
      new ReceiveMessageCommand({
        QueueUrl: sqs1.wagerQueueUrl,
        MaxNumberOfMessages: 1,
        AttributeNames: ['All'],
      }),
    );
    expect(received.Messages?.length).toBe(1);

    await expect(
      sqs1.handleSingleSqsMessage(received.Messages![0]!, {
        simulateCrashBeforeAck: true,
      }),
    ).rejects.toThrow('SIMULATED_CRASH_AFTER_COMMIT_BEFORE_ACK');

    // O commit já ocorreu: saldo é 55.00 BRL, mas a mensagem continua na fila para redelivery
    const midRes = await fetch(`${urls[0]}/wallets/${wallet.id}`);
    const midWallet = (await midRes.json()) as { balance: { amount: string } };
    expect(midWallet.balance.amount).toBe('55.00');

    // Instância 2 consome a mensagem reentregue pela fila, detecta via InboxMessage e faz o ACK sem duplicar o débito!
    const processedByApp2 = await sqs2.pollAndProcessBatch(5, 0);
    expect(processedByApp2).toBe(1);

    const finalRes = await fetch(`${urls[1]}/wallets/${wallet.id}`);
    const finalWallet = (await finalRes.json()) as {
      balance: { amount: string };
      version: number;
    };
    expect(finalWallet.balance.amount).toBe('55.00');
    expect(finalWallet.version).toBe(2);

    const remainingInQueue = await sqs2.getApproximateQueueCount(
      sqs2.wagerQueueUrl,
    );
    expect(remainingInQueue).toBe(0);
  });

  it('Scenario 6: Two concurrent Outbox publishers across instances publish pending events without loss', async () => {
    const publisher1 = app1.get(PublishOutboxMessagesUseCase);
    const publisher2 = app2.get(PublishOutboxMessagesUseCase);
    const publisher3 = app3.get(PublishOutboxMessagesUseCase);

    // Executa os 3 publishers simultaneamente sobre a mesma tabela outbox_messages (usando FOR UPDATE SKIP LOCKED)
    const [c1, c2, c3] = await Promise.all([
      publisher1.executeBatch(30),
      publisher2.executeBatch(30),
      publisher3.executeBatch(30),
    ]);

    expect(c1 + c2 + c3).toBeGreaterThan(0);

    // Drena o restante se houver e verifica que não restam mensagens pendentes na outbox
    while ((await publisher1.executeBatch(100)) > 0) {
      // drain
    }

    const pendingRows = await orm1.em.getConnection().execute<
      Array<{ cnt: string }>
    >(
      `SELECT COUNT(*)::text AS cnt FROM outbox_messages WHERE published_at IS NULL`,
    );
    expect(Number(pendingRows[0]?.cnt ?? 0)).toBe(0);
  });

  it('Scenario 7: Out-of-order REFUND / ROLLBACK delivered BEFORE the referenced BET -> PENDING_REFERENCE -> resolved by scheduled worker (and timeout rejection when missing)', async () => {
    const wallet = await createWallet('200.00');
    const missingBetExtId = `late-bet-${randomUUID()}`;
    const earlyRefundExtId = `early-refund-${randomUUID()}`;

    // 7.1 Enviar REFUND antes da BET existir -> deve retornar 202 Accepted com status PENDING_REFERENCE
    const refundRes = await fetch(`${urls[0]}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-ooo:${earlyRefundExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-ooo',
        externalTransactionId: earlyRefundExtId,
        referenceExternalTransactionId: missingBetExtId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-ooo-1',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '60.00', currency: 'BRL' },
      }),
    });

    expect(refundRes.status).toBe(202);
    const refundPending = (await refundRes.json()) as {
      transactionId: string;
      status: string;
      balance: { amount: string };
    };
    expect(refundPending.status).toBe(WagerTransactionStatus.PendingReference);
    expect(refundPending.balance.amount).toBe('200.00');

    // 7.2 Agora a BET referenciada chega (200.00 - 60.00 = 140.00 BRL)
    const betRes = await fetch(`${urls[1]}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-ooo:${missingBetExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-ooo',
        externalTransactionId: missingBetExtId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-ooo-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '60.00', currency: 'BRL' },
      }),
    });
    expect(betRes.status).toBe(200);
    const betBody = (await betRes.json()) as { balance: { amount: string } };
    expect(betBody.balance.amount).toBe('140.00');

    // Tornar next_retry_at imediatamente vencido para disparar o worker sem esperar sleep
    await orm1.em
      .getConnection()
      .execute(
        `UPDATE wager_transactions SET next_retry_at = NOW() - INTERVAL '1 second' WHERE id = ?`,
        [refundPending.transactionId],
      );

    const retryWorker = app2.get(RetryPendingReferencesUseCase);
    const resolvedCount = await retryWorker.executeBatch(10);
    expect(resolvedCount).toBeGreaterThanOrEqual(1);

    // Consultar a transação de REFUND: deve estar PROCESSED e o saldo da Wallet deve ter voltado para 200.00 BRL!
    const txCheckRes = await fetch(
      `${urls[2]}/wagering/transactions/${refundPending.transactionId}`,
    );
    const txCheck = (await txCheckRes.json()) as {
      status: string;
      referenceTransactionId?: string;
    };
    expect(txCheck.status).toBe(WagerTransactionStatus.Processed);
    expect(txCheck.referenceTransactionId).toBeTruthy();

    const walletAfterRes = await fetch(`${urls[2]}/wallets/${wallet.id}`);
    const walletAfter = (await walletAfterRes.json()) as {
      balance: { amount: string };
    };
    expect(walletAfter.balance.amount).toBe('200.00');

    // 7.3 Testar esgotamento de tentativas (TTL / Max retries) quando a referência NUNCA chega -> REJECTED com REFERENCE_NOT_FOUND_TIMEOUT
    const orphanRollbackExt = `orphan-rb-${randomUUID()}`;
    const orphanRes = await fetch(`${urls[0]}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-ooo:${orphanRollbackExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-ooo',
        externalTransactionId: orphanRollbackExt,
        referenceExternalTransactionId: `non-existent-${randomUUID()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-ooo-orphan',
        gameId: 'fortune-chimp',
        kind: 'ROLLBACK',
        money: { amount: '20.00', currency: 'BRL' },
      }),
    });
    expect(orphanRes.status).toBe(202);
    const orphanTx = (await orphanRes.json()) as { transactionId: string };

    // Simular todas as tentativas até esgotar MAX_PENDING_REFERENCE_RETRIES
    for (
      let attempt = 0;
      attempt < WagerTransaction.MAX_PENDING_REFERENCE_RETRIES;
      attempt++
    ) {
      await orm1.em
        .getConnection()
        .execute(
          `UPDATE wager_transactions SET next_retry_at = NOW() - INTERVAL '1 second' WHERE id = ?`,
          [orphanTx.transactionId],
        );
      await retryWorker.executeBatch(10);
    }

    const orphanFinalRes = await fetch(
      `${urls[0]}/wagering/transactions/${orphanTx.transactionId}`,
    );
    const orphanFinal = (await orphanFinalRes.json()) as {
      status: string;
      failureCode: string;
    };
    expect(orphanFinal.status).toBe(WagerTransactionStatus.Rejected);
    expect(orphanFinal.failureCode).toBe(FailureCode.ReferenceNotFoundTimeout);
  });

  it('Scenario 8: Service restart & Global Final Invariant (wallet.balance == balance reconstructed from ledger across ALL wallets)', async () => {
    // Encerra as instâncias app2 e app3 e inicializa uma nova instância do zero para provar recuperação limpa
    await app2.close();
    await app3.close();

    const ormRecovered = await MikroORM.init(
      createMikroOrmOptions(infra.databaseUrl),
    );
    const appRecovered = await createConfiguredApp({ orm: ormRecovered });
    await appRecovered.listen(0, '127.0.0.1');

    try {
      const reconcileUseCase = appRecovered.get(ReconcileWalletUseCase);

      expect(createdWalletIds.length).toBeGreaterThanOrEqual(12);

      for (const walletId of createdWalletIds) {
        const report = await reconcileUseCase.execute(walletId);
        expect(report.consistent).toBe(true);
        expect(report.difference.amount).toBe('0.00');
        expect(report.storedBalance).toEqual(report.calculatedBalance);
      }
    } finally {
      await appRecovered.close();
      await ormRecovered.close(true);
    }
  });
});
